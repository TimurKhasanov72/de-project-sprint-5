from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel): 
    id: int
    rate: int
    order_id: str
    tip_sum: float
    courier_id: str
    delivery_key: str


class DeliveryOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, rank_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        dsd.id,
                        dsd.payload->>'rate' as rate,
                        dsd.payload->>'tip_sum' as tip_sum,
                        do2.id as order_id,    
                        dsd.payload->>'delivery_id' as delivery_key,
                        co.id as courier_id
                    FROM stg.delivery_system_deliveries dsd
                    join dds.dm_orders do2 on do2.order_key = dsd.payload->>'order_id'
                    join dds.dm_couriers co on co.courier_id = dsd.payload->>'courier_id'
                    WHERE dsd.id > %(threshold)s -- Пропускаем те объекты, которые уже загрузили.
                    ORDER BY dsd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(
                        rate,
                        tip_sum,
                        order_id,
                        delivery_key,
                        courier_id                    
                    )
                    VALUES (%(rate)s, %(tip_sum)s, %(order_id)s, %(delivery_key)s, %(courier_id)s)
                    ON CONFLICT (delivery_key) DO UPDATE
                    SET
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id;
                """,
                {
                    "delivery_key": delivery.delivery_key,
                    "tip_sum": delivery.tip_sum,
                    "order_id": delivery.order_id,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate
                },
            )


class DeliveryLoader:
    WF_KEY = "deliverys_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliveryOriginRepository(pg_origin)
        self.dds = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
