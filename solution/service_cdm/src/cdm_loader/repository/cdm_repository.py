import uuid

from lib.pg import PgConnect


class CdmRepository:
    """Обновление витрин-счётчиков.

    Счётчики не инкрементируются «вслепую», а пересчитываются для пользователя
    по детальному слою (DDS) и записываются через upsert. Поэтому повторная
    обработка одного и того же сообщения не искажает витрины (идемпотентность).
    Учитываются только заказы в финальном статусе CLOSED (последняя версия статуса).
    """

    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def refresh_user_counters(self, user_id: uuid.UUID) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH last_status AS (
                        SELECT DISTINCT ON (h_order_pk) h_order_pk, status
                        FROM dds.s_order_status
                        ORDER BY h_order_pk, load_dt DESC
                    ),
                    last_name AS (
                        SELECT DISTINCT ON (h_product_pk) h_product_pk, name
                        FROM dds.s_product_names
                        ORDER BY h_product_pk, load_dt DESC
                    )
                    INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                    SELECT lou.h_user_pk, lop.h_product_pk, ln.name, COUNT(DISTINCT lou.h_order_pk)
                    FROM dds.l_order_user lou
                    JOIN last_status ls ON ls.h_order_pk = lou.h_order_pk
                    JOIN dds.l_order_product lop ON lop.h_order_pk = lou.h_order_pk
                    JOIN last_name ln ON ln.h_product_pk = lop.h_product_pk
                    WHERE lou.h_user_pk = %(user_id)s
                      AND ls.status = 'CLOSED'
                    GROUP BY lou.h_user_pk, lop.h_product_pk, ln.name
                    ON CONFLICT (user_id, product_id) DO UPDATE
                    SET order_cnt = EXCLUDED.order_cnt,
                        product_name = EXCLUDED.product_name;
                    """,
                    {"user_id": user_id})

                cur.execute(
                    """
                    WITH last_status AS (
                        SELECT DISTINCT ON (h_order_pk) h_order_pk, status
                        FROM dds.s_order_status
                        ORDER BY h_order_pk, load_dt DESC
                    )
                    INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                    SELECT lou.h_user_pk, hc.h_category_pk, hc.category_name, COUNT(DISTINCT lou.h_order_pk)
                    FROM dds.l_order_user lou
                    JOIN last_status ls ON ls.h_order_pk = lou.h_order_pk
                    JOIN dds.l_order_product lop ON lop.h_order_pk = lou.h_order_pk
                    JOIN dds.l_product_category lpc ON lpc.h_product_pk = lop.h_product_pk
                    JOIN dds.h_category hc ON hc.h_category_pk = lpc.h_category_pk
                    WHERE lou.h_user_pk = %(user_id)s
                      AND ls.status = 'CLOSED'
                    GROUP BY lou.h_user_pk, hc.h_category_pk, hc.category_name
                    ON CONFLICT (user_id, category_id) DO UPDATE
                    SET order_cnt = EXCLUDED.order_cnt,
                        category_name = EXCLUDED.category_name;
                    """,
                    {"user_id": user_id})
