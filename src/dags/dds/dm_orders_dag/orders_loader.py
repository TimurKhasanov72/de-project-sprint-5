import datetime
from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    user_id: int
    timestamp_id: int


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, rank_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    select 
                        oo.id,
                        (object_value::jsonb ->> '_id') as order_key,
                        (object_value::jsonb ->> 'final_status') as order_status,
                        dr.id as restaurant_id,
                        du.id as user_id,
                        dt.id as timestamp_id    
                    from stg.ordersystem_orders oo	
                    inner join dds.dm_restaurants dr on ((oo.object_value::jsonb ->> 'restaurant')::jsonb ->> 'id') = dr.restaurant_id
                    inner join dds.dm_users du on ((oo.object_value::jsonb ->> 'user')::jsonb ->> 'id') = du.user_id
                    inner join dds.dm_timestamps dt on (oo.object_value::jsonb ->> 'update_ts')::timestamp = dt.ts
                    WHERE oo.id > %(threshold)s -- Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oo.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, user_id, timestamp_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(user_id)s, %(timestamp_id)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET                        
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        user_id = EXCLUDED.user_id,
                        timestamp_id = EXCLUDED.timestamp_id                  
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "user_id": order.user_id,
                    "timestamp_id": order.timestamp_id
                },
            )


class OrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.stg = OrderDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
