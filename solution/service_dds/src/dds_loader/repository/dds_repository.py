import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect


class DdsRepository:
    """Запись заказа в детальный слой (Data Vault: хабы, линки, сателлиты).

    Все вставки идемпотентны:
    - хабы и линки — ON CONFLICT (pk) DO NOTHING (ключи детерминированы, uuid5);
    - сателлиты — новая версия пишется, только если изменился hashdiff
      (ON CONFLICT (hashdiff) DO NOTHING).
    """

    NS = uuid.NAMESPACE_DNS

    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # ---------- генерация ключей ----------
    @classmethod
    def key(cls, *parts: Any) -> uuid.UUID:
        return uuid.uuid5(cls.NS, "|".join(str(p) for p in parts))

    # ---------- запись заказа целиком в одной транзакции ----------
    def save_order(self, order: Dict, load_dt: datetime, load_src: str) -> Dict:
        order_id = int(order["id"])
        order_dt = datetime.strptime(order["date"], "%Y-%m-%d %H:%M:%S")
        user = order["user"]
        restaurant = order["restaurant"]
        products: List[Dict] = order["products"]

        h_order_pk = self.key("order", order_id)
        h_user_pk = self.key("user", user["id"])
        h_restaurant_pk = self.key("restaurant", restaurant["id"])

        result = {"h_order_pk": h_order_pk, "h_user_pk": h_user_pk, "products": []}

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # --- хабы ---
                cur.execute(
                    """INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (h_order_pk) DO NOTHING;""",
                    (h_order_pk, order_id, order_dt, load_dt, load_src))
                cur.execute(
                    """INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                       VALUES (%s, %s, %s, %s) ON CONFLICT (h_user_pk) DO NOTHING;""",
                    (h_user_pk, user["id"], load_dt, load_src))
                cur.execute(
                    """INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                       VALUES (%s, %s, %s, %s) ON CONFLICT (h_restaurant_pk) DO NOTHING;""",
                    (h_restaurant_pk, restaurant["id"], load_dt, load_src))

                # --- линк заказ-пользователь ---
                cur.execute(
                    """INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_order_user_pk) DO NOTHING;""",
                    (self.key(h_order_pk, h_user_pk), h_order_pk, h_user_pk, load_dt, load_src))

                # --- продукты, категории и их связи ---
                for p in products:
                    h_product_pk = self.key("product", p["id"])
                    h_category_pk = self.key("category", p["category"])
                    cur.execute(
                        """INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                           VALUES (%s, %s, %s, %s) ON CONFLICT (h_product_pk) DO NOTHING;""",
                        (h_product_pk, p["id"], load_dt, load_src))
                    cur.execute(
                        """INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                           VALUES (%s, %s, %s, %s) ON CONFLICT (h_category_pk) DO NOTHING;""",
                        (h_category_pk, p["category"], load_dt, load_src))
                    cur.execute(
                        """INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_order_product_pk) DO NOTHING;""",
                        (self.key(h_order_pk, h_product_pk), h_order_pk, h_product_pk, load_dt, load_src))
                    cur.execute(
                        """INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;""",
                        (self.key(h_product_pk, h_restaurant_pk), h_product_pk, h_restaurant_pk, load_dt, load_src))
                    cur.execute(
                        """INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_product_category_pk) DO NOTHING;""",
                        (self.key(h_product_pk, h_category_pk), h_product_pk, h_category_pk, load_dt, load_src))
                    # сателлит названия продукта
                    cur.execute(
                        """INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                           VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;""",
                        (h_product_pk, p["name"], load_dt, load_src, self.key(h_product_pk, p["name"])))
                    result["products"].append({
                        "id": str(h_product_pk),
                        "name": p["name"],
                        "category_id": str(h_category_pk),
                        "category_name": p["category"],
                    })

                # --- сателлиты ---
                cur.execute(
                    """INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;""",
                    (h_user_pk, user["name"], user.get("login") or user["name"], load_dt, load_src,
                     self.key(h_user_pk, user["name"], user.get("login"))))
                cur.execute(
                    """INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;""",
                    (h_restaurant_pk, restaurant["name"], load_dt, load_src,
                     self.key(h_restaurant_pk, restaurant["name"])))
                cur.execute(
                    """INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;""",
                    (h_order_pk, order["cost"], order["payment"], load_dt, load_src,
                     self.key(h_order_pk, order["cost"], order["payment"])))
                cur.execute(
                    """INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;""",
                    (h_order_pk, order["status"], load_dt, load_src, self.key(h_order_pk, order["status"])))

        return result
