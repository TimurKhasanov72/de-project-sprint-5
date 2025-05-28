import datetime
from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SalesObj(BaseModel):
    id: int
    order_id: int
    product_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, rank_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    with t1 as (
                        select 
                            id,
                            event_value::jsonb ->> 'order_id' as order_id,
                            event_value::jsonb ->> 'product_payments' as product_paymets
                        from stg.bonussystem_events be 
                        where event_type = 'bonus_transaction'
                    ),
                    t2 as (
                        select 
                            t1.id,
                            do2.id as order_id,
                            jsonb_array_elements_text(t1.product_paymets::jsonb) as obj
                        from t1
                        join dds.dm_orders do2 on do2.order_key = t1.order_id
                    )
                    select 
                        t2.id,
                        t2.order_id,
                        --	t2.obj::json ->> 'product_id' as product_id
                        dp.id as product_id,
                        (t2.obj::json ->> 'quantity')::int as count,
                        (t2.obj::json ->> 'price')::numeric(19, 5) as price,
                        (t2.obj::json ->> 'quantity')::int * (t2.obj::json ->> 'price')::numeric(19, 5) as total_sum,
                        (t2.obj::json ->> 'bonus_payment')::numeric(19, 5) as bonus_payment,
                        (t2.obj::json ->> 'bonus_grant')::numeric(19, 5) as bonus_grant
                    from t2
                    join dds.dm_products dp on dp.product_id = t2.obj::json ->> 'product_id'
                    WHERE t2.id > %(threshold)s -- Пропускаем те объекты, которые уже загрузили.
                    ORDER BY t2.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDestRepository:

    def insert_sale(self, conn: Connection, sale: SalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(order_id, product_id, price, count, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(order_id)s, %(product_id)s, %(price)s, %(count)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (order_id, product_id) DO UPDATE
                    SET                        
                        price = EXCLUDED.price,
                        count = EXCLUDED.count, 
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant                   
                """,
                {
                    "order_id": sale.order_id,
                    "product_id": sale.product_id,
                    "price": sale.price,
                    "count": sale.count,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant
                },
            )


class SalesLoader:
    WF_KEY = "fct_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SalesOriginRepository(pg_origin)
        self.stg = SalesDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_sales(self):
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
            load_queue = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sale in load_queue:
                self.stg.insert_sale(conn, sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
