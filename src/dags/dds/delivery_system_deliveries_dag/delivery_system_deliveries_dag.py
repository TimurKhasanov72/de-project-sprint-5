import logging

import pendulum
from airflow.decorators import dag, task
from dds.delivery_system_deliveries_dag.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_delivery_system_deliveries_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="events_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DeliveryLoader(
            pg_origin=dwh_pg_connect, 
            pg_dest=dwh_pg_connect, 
            log=log
        )
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    events_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    events_dict  # type: ignore


load_deliveries_dag = dds_delivery_system_deliveries_dag()
