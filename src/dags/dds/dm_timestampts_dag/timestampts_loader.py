import datetime
from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestamptObj(BaseModel):
    id: int
    ts: datetime.datetime
    year: int
    month: int
    day: int
    date: datetime.date
    time: datetime.time


class TimestamptsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestampts(self, rank_threshold: int, limit: int) -> List[TimestamptObj]:
        with self._db.client().cursor(row_factory=class_row(TimestamptObj)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        object_value::json ->> 'date' as ts,
                        date_part('year', (object_value::json ->> 'date')::timestamp) as year,
                        date_part('month', (object_value::json ->> 'date')::timestamp) as month,
                        date_part('day', (object_value::json ->> 'date')::timestamp) as day,
                        (object_value::json ->> 'date')::date as date,
                        (object_value::json ->> 'date')::time as time
                    from stg.ordersystem_orders oo
                    WHERE id > %(threshold)s -- Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestamptDestRepository:

    def insert_timestampt(self, conn: Connection, timestampt: TimestamptObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                    ON CONFLICT (ts) DO UPDATE
                    SET
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time
                """,
                {
                    "ts": timestampt.ts,
                    "year": timestampt.year,
                    "month": timestampt.month,
                    "day": timestampt.day,
                    "date": timestampt.date,
                    "time": timestampt.time
                },
            )


class TimestamptLoader:
    WF_KEY = "timestampts_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestamptsOriginRepository(pg_origin)
        self.stg = TimestamptDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_timestampts(self):
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
            load_queue = self.origin.list_timestampts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestampts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestampt in load_queue:
                self.stg.insert_timestampt(conn, timestampt)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
