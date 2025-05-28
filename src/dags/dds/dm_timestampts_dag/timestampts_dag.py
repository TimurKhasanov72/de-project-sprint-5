import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_timestampts_dag.timestampts_loader import TimestamptLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_dm_timestampts_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_timestampts_load")
    def load_timestampts():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimestamptLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestampts()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    timestampts_dict = load_timestampts()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    timestampts_dict  # type: ignore


timestampts_dag = dds_dm_timestampts_dag()
