import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_restaurants_dag.restaurants_loader import RestaurantLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_dm_restaurants_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    restaurants_dict = load_restaurants()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    restaurants_dict  # type: ignore


restaurants_dag = dds_dm_restaurants_dag()
