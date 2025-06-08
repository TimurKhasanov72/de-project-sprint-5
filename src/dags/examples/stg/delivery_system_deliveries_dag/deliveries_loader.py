import json
from logging import Logger

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection

from examples.stg.DeliveryServiceAPI import DeliveryServiceAPI as Api
from examples.stg.models.Delivery import DeliveryObj


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_deliveries(delivery_id, delivery_ts, payload)
                    VALUES (%(delivery_id)s, %(delivery_ts)s, %(payload)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        delivery_ts = EXCLUDED.delivery_ts,
                        payload = EXCLUDED.payload
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "delivery_ts": delivery.delivery_ts,
                    "payload": json.dumps(delivery.payload)
                },
            )


class DeliveryLoader:
    WF_KEY = "delivery_system_delivery_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "current_offset"
    BATCH_LIMIT = 10  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, api: Api, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.api = api
        self.stg = DeliveryDestRepository()
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
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_offset = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.api.list_deliveries(offset=last_offset, limit=self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliverys to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)
    
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_offset + self.BATCH_LIMIT
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
