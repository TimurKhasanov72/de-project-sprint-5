import logging

import pendulum
from airflow.decorators import dag, task
from marts.dm_courier_ledger_dag.courier_ledger_report_loader import CourierLedgerLoader
from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='@daily',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 5, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def marts_dm_courier_ledger_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="couriers_ledger_load")
    def load_couriers_ledger():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CourierLedgerLoader(
            pg_origin=dwh_pg_connect, 
            pg_dest=dwh_pg_connect, 
            log=log
        )
        rest_loader.load_couriers_ledger()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    courier_ledger_dict = load_couriers_ledger()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    courier_ledger_dict  # type: ignore


report_dag = marts_dm_courier_ledger_report_dag()
