import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_orders_dag.orders_loader import OrderLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_dm_orders_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = OrderLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    orders_dict = load_orders()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    orders_dict  # type: ignore


orders_dag = dds_dm_orders_dag()
