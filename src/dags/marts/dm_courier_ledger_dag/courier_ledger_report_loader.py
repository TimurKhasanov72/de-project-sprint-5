import datetime
from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLedgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers_ledger(self, last_settlement_date: str) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
                    with couriers_orders as ( -- данные по курьерам
                        select distinct
                            de.courier_id,
                            co.courier_name,
                            dt.year as settlement_year,
                            dt.month as settlement_month,
                            dt.ts,
                            do2.id as order_id,
                            fps.total_sum,
                            de.tip_sum,
                            de.rate        
                        from dds.dm_orders do2 
                        join dds.dm_timestamps dt on do2.timestamp_id = dt.id
                        join dds.dm_deliveries de on do2.id = de.order_id
                        join dds.dm_couriers co on de.courier_id = co.id
                        join dds.fct_product_sales fps on fps.order_id = do2.id
                        where do2.order_status = 'CLOSED' 
                              and dt.ts > %(threshold)s  -- Пропускаем те объекты, которые уже загрузили.
                        ORDER BY de.courier_id, dt.year, dt.month ASC --Обязательна сортировка по ts, т.к. id используем в качестве курсора.                        
                    ),
                    couriers_stat as ( -- статистика за месяца по каждому курьеру
                        select 	
                            co.courier_id,
                            co.courier_name,
                            co.settlement_year,
                            co.settlement_month,
                            count(order_id) as orders_count,
                            sum(total_sum) as orders_total_sum,
                            avg(rate) as rate_avg,
                            sum(tip_sum) as courier_tips_sum,
                            sum(total_sum) * 0.25 as order_processing_fee
                        from couriers_orders co
                        group by courier_id, courier_name, settlement_year, settlement_month
                        order by courier_name, settlement_year, settlement_month 
                    ),
                    couriers_order_sum as (  -- сумма за доставку с учетом рейтинга
                        select 
                            co.courier_id,
                            co.settlement_year,
                            co.settlement_month,
                            case
                                when cs.rate_avg < 4 then 
                                    case when co.total_sum * 0.05 < 100 then 100
                                        else co.total_sum * 0.05
                                    end		
                                when 4 <= cs.rate_avg and cs.rate_avg < 4.5 then 
                                    case when co.total_sum * 0.07 < 150 then 150
                                        else co.total_sum * 0.07
                                    end
                                when 4.5 <= cs.rate_avg and cs.rate_avg < 4.9 then 
                                    case when co.total_sum * 0.08 < 175 then 175
                                        else co.total_sum * 0.08
                                    end
                                when 4.9 <= cs.rate_avg then 
                                    case when co.total_sum * 0.1 < 200 then 200
                                        else co.total_sum * 0.1
                                    end
                            end courier_order_sum
                        from couriers_stat cs
                        join couriers_orders co 
                            on cs.courier_id = co.courier_id 
                            and cs.settlement_year = co.settlement_year 
                            and cs.settlement_month = co.settlement_month
                    ),
                    couriers_order_sum_by_month as ( -- сумма за доставку с учетом рейтинга за месяц
                        select 
                            courier_id,
                            settlement_year,
                            settlement_month,	
                            sum(courier_order_sum) as courier_order_sum
                        from couriers_order_sum 
                        group by courier_id, settlement_year, settlement_month
                    )
                    select 
                        cs.courier_id,
                        cs.courier_name,
                        cs.settlement_year,
                        cs.settlement_month,
                        cs.orders_count,
                        cs.orders_total_sum,
                        cs.rate_avg,
                        cs.order_processing_fee,
                        cos.courier_order_sum,
                        cs.courier_tips_sum,
                        cos.courier_order_sum + cs.courier_tips_sum * 0.95 as courier_reward_sum
                    from couriers_stat cs
                    join couriers_order_sum_by_month cos 
                        on cs.courier_id = cos.courier_id 
                        and cs.settlement_year = cos.settlement_year 
                        and cs.settlement_month = cos.settlement_month
                """, {
                    "threshold": last_settlement_date,
                }
            )
            objs = cur.fetchall()
        return objs


class CourierLedgerDestRepository:

    def insert_courier_ledger(self, conn: Connection, courier_ledger_obj: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into cdm.dm_courier_ledger (	
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum
                    )
                    VALUES (
                        %(courier_id)s,
                        %(courier_name)s,
                        %(settlement_year)s,
                        %(settlement_month)s,
                        %(orders_count)s,
                        %(orders_total_sum)s,
                        %(rate_avg)s,
                        %(order_processing_fee)s,
                        %(courier_order_sum)s,
                        %(courier_tips_sum)s,
                        %(courier_reward_sum)s
                    )
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET                        
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum
                """,
                {
                    "courier_id": courier_ledger_obj.courier_id,
                    "courier_name": courier_ledger_obj.courier_name,
                    "settlement_year": courier_ledger_obj.settlement_year,
                    "settlement_month": courier_ledger_obj.settlement_month,
                    "orders_count": courier_ledger_obj.orders_count,
                    "orders_total_sum": courier_ledger_obj.orders_total_sum,
                    "rate_avg": courier_ledger_obj.rate_avg,
                    "order_processing_fee": courier_ledger_obj.order_processing_fee,
                    "courier_order_sum": courier_ledger_obj.courier_order_sum,
                    "courier_tips_sum": courier_ledger_obj.courier_tips_sum,
                    "courier_reward_sum": courier_ledger_obj.courier_reward_sum
                },
            )


class CourierLedgerLoader:
    WF_KEY = "marts_dm_courier_ledger_report"
    LAST_LOADED_ID_KEY = "last_settlement_date"    

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgerOriginRepository(pg_origin)
        self.stg = CourierLedgerDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers_ledger(self):
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
                    workflow_settings={self.LAST_LOADED_ID_KEY: '2025-01-01 00:00:00'}
                )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last loaded info: {last_loaded}')
            load_queue = self.origin.list_couriers_ledger(last_loaded)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for sale in load_queue:
                self.stg.insert_courier_ledger(conn, sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                [
                   datetime.datetime(t.settlement_year, t.settlement_month, 1) for t in load_queue
                ]
            )
            if wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]:
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY].strftime('%Y-%m-%d')
            self.log.info('workflow settings:')
            self.log.info(wf_setting.workflow_settings)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
