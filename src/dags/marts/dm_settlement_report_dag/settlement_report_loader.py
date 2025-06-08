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
    restaurant_id: int
    restaurant_name: str
    settlement_date: datetime.date
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float
    orders_count: int


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, rank_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    with closed_orders as (
                        select distinct
                            do2.restaurant_id,
                            dr.restaurant_name,
                            dt.date as settlement_date,
                            do2.id as order_id
                        from dds.dm_orders do2 
                        join dds.dm_timestamps dt on do2.timestamp_id = dt.id
                        join dds.dm_restaurants dr on do2.restaurant_id = dr.id	
                        where do2.order_status = 'CLOSED'
                    ),
                    orders_cnt_tbl as (
                        select 	
                            co.restaurant_id,
                            co.restaurant_name,
                            co.settlement_date,
                            count(co.order_id) as orders_count
                        from closed_orders co                        
                        group by co.restaurant_id, co.restaurant_name, co.settlement_date
                    ),
                    payments_table as (
                        select 
                            co.restaurant_id,
                            co.restaurant_name,
                            co.settlement_date,		
                            sum(total_sum) as orders_total_sum,
                            sum(bonus_payment) as orders_bonus_payment_sum,
                            sum(bonus_grant) as orders_bonus_granted_sum,
                            sum(total_sum) * 0.25 as order_processing_fee,
                            sum(total_sum) - sum(total_sum) * 0.25 - sum(bonus_payment) as restaurant_reward_sum
                        from closed_orders co	
                        left join dds.fct_product_sales fps using (order_id)	
                        group by co.restaurant_id, co.restaurant_name, co.settlement_date
                    )
                    select 
                        pt.*,
                        oct.orders_count
                    from payments_table pt
                    join orders_cnt_tbl oct using (restaurant_id, settlement_date)
                    WHERE oct.settlement_date > %(threshold)s and oct.orders_count > 0 -- Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oct.settlement_date ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
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
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum,
                        orders_count
                    )
                    VALUES (
                        %(restaurant_id)s,
                        %(restaurant_name)s,
                        %(settlement_date)s,
                        %(orders_total_sum)s,
                        %(orders_bonus_payment_sum)s,
                        %(orders_bonus_granted_sum)s,
                        %(order_processing_fee)s,
                        %(restaurant_reward_sum)s,
                        %(orders_count)s
                    )
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET                        
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum,
                        orders_count = EXCLUDED.orders_count
                """,
                {
                    "restaurant_id": sale.restaurant_id,
                    "restaurant_name": sale.restaurant_name,
                    "settlement_date": sale.settlement_date,
                    "orders_total_sum": sale.orders_total_sum,
                    "orders_bonus_payment_sum": sale.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": sale.orders_bonus_granted_sum,
                    "order_processing_fee": sale.order_processing_fee,
                    "restaurant_reward_sum": sale.restaurant_reward_sum,
                    "orders_count": sale.orders_count
                },
            )


class SalesLoader:
    WF_KEY = "marts_dm_settlement_report"
    LAST_LOADED_ID_KEY = "last_settlement_date"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

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
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: '1979-01-01 00:00:00'}
                )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last loaded info: {last_loaded}')
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
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.settlement_date for t in load_queue])
            if wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]:
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY].strftime('%Y-%m-%d')
            self.log.info('workflow settings:')
            self.log.info(wf_setting.workflow_settings)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
