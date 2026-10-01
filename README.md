# Проект 9-го спринта: облачные технологии

В проекте я сделал три сервиса, которые читают заказы из Kafka и по очереди заполняют слои хранилища в PostgreSQL: STG → DDS → CDM.
Первый сервис (stg) делали в уроках, в проекте я дописал второй (dds) и третий (cdm). Все сервисы запущены в Kubernetes в Yandex Cloud.

## Как всё устроено

```
order-service_orders  ->  stg-service  ->  stg-service-orders  ->  dds-service  ->  dds-service-orders  ->  cdm-service
                              |                                        |                                       |
                       stg.order_events                         dds (хабы, линки,                        cdm (витрины)
                       + данные из Redis                           сателлиты)
```

Что использовал в Yandex Cloud:
- Managed Kafka — очереди сообщений
- Managed Valkey (Redis) — справочники пользователей и ресторанов
- Managed PostgreSQL — хранилище (БД `sprint9dwh`)
- Container Registry — образы сервисов
- Managed Kubernetes — запуск сервисов через Helm

## Ссылка на Container Registry

`cr.yandex/crpj2frsk4tdepb9t651`

Образы:
- `cr.yandex/crpj2frsk4tdepb9t651/stg_service`
- `cr.yandex/crpj2frsk4tdepb9t651/dds_service`
- `cr.yandex/crpj2frsk4tdepb9t651/cdm_service`

Тег делал по дате и номеру релиза, например `v2026-10-01-r1`. Какой тег сейчас задеплоен — видно в `app/values.yaml` у каждого сервиса.

## Структура

```
solution/
  docker-compose.yaml   - чтобы запускать сервисы локально
  .env.example          - пример переменных (без паролей)
  service_stg/          - сервис из уроков
  service_dds/          - второй сервис
  service_cdm/          - третий сервис
```

В каждом сервисе есть `app/` (helm-чарт), `dockerfile`, `requirements.txt` и `src/` с кодом.
Логика лежит в `src/dds_loader/` и `src/cdm_loader/`: отдельно обработчик сообщений (`*_message_processor_job.py`) и репозиторий для работы с базой (`repository/`).

## Что делает каждый сервис

Все сервисы работают одинаково: раз в 25 секунд берут из Kafka пачку до 100 сообщений и обрабатывают их.

**stg-service**
- читает топик `order-service_orders`
- кладёт сообщение как есть в `stg.order_events` (если такой `object_id` уже есть — обновляет)
- достаёт из Redis имя и логин пользователя, название ресторана, названия и категории блюд
- отправляет обогащённый заказ в топик `stg-service-orders`

**dds-service**
- читает `stg-service-orders`
- раскладывает заказ по таблицам DDS (Data Vault):
  - хабы: `h_order`, `h_user`, `h_restaurant`, `h_product`, `h_category`
  - линки: `l_order_user`, `l_order_product`, `l_product_restaurant`, `l_product_category`
  - сателлиты: `s_user_names`, `s_restaurant_names`, `s_product_names`, `s_order_cost`, `s_order_status`
- в `load_src` пишу `orders-system-kafka`, в `load_dt` — текущее время UTC
- отправляет в топик `dds-service-orders` сообщение для витрин: id заказа, статус, id пользователя и список блюд с категориями

Пример выходного сообщения:
```json
{
  "object_id": 322519,
  "object_type": "order_report",
  "payload": {
    "id": 322519,
    "order_dt": "2022-11-19 16:06:36",
    "status": "CLOSED",
    "user_id": "<h_user_pk>",
    "products": [
      {"id": "<h_product_pk>", "name": "РОЛЛ С ТОФУ...", "category_id": "<h_category_pk>", "category_name": "Выпечка"}
    ]
  }
}
```

**cdm-service**
- читает `dds-service-orders`
- обновляет витрины `cdm.user_product_counters` и `cdm.user_category_counters`
- считаю только заказы со статусом `CLOSED`, отменённые не учитываю

## Повторные сообщения

Старался сделать так, чтобы одно и то же сообщение можно было обработать несколько раз и данные не задвоились:
- в STG — `ON CONFLICT (object_id) DO UPDATE`
- в DDS ключи хабов и линков считаю через `uuid5` от бизнес-ключа (id заказа, id пользователя и т.д.), поэтому у одного объекта всегда один и тот же ключ, и вставка идёт с `ON CONFLICT DO NOTHING`. Для сателлитов так же считаю `hashdiff` от значений — новая строка появится, только если что-то поменялось
- в CDM не прибавляю +1 на каждое сообщение, а заново считаю счётчики для пользователя по данным из DDS и делаю upsert. Так при повторе получается то же самое число

## Как запустить локально

```bash
cd solution
cp .env.example .env    # вписать пароли
docker compose up -d --build
```

Проверка, что поднялись: `localhost:5011/health` (stg), `localhost:5012/health` (dds), `localhost:5013/health` (cdm).

## Как деплоил в Kubernetes

На примере dds:

```bash
cd solution/service_dds
docker build . -t cr.yandex/crpj2frsk4tdepb9t651/dds_service:v2026-10-01-r1
docker push cr.yandex/crpj2frsk4tdepb9t651/dds_service:v2026-10-01-r1
# поменять tag в app/values.yaml
helm upgrade --install --atomic dds-service app -f secrets.yaml
kubectl get pods
```

Пароли в `values.yaml` не пишу, они передаются отдельным файлом `secrets.yaml`, который в `.gitignore`.

Пара моментов, на которые наткнулся:
- в неймспейсе квота на 3 пода, а при обычном обновлении (RollingUpdate) нужен 4-й, поэтому в чартах поставил `strategy: Recreate`
- Yandex Container Registry не принимал образ, собранный новым Docker, помогло собирать с `--provenance=false`


Дашборд в DataLens не делал, он в проекте необязательный.
