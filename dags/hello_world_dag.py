from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_world():
    print("Hello, world!")


# Визначаємо DAG
with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2023, 1, 1),  # Вкажи дату початку
    schedule_interval="@daily",  # Виконання DAG щодня
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    hello_task = PythonOperator(task_id="hello_task", python_callable=hello_world)

    end = DummyOperator(task_id="end")

    # Послідовність завдань
    start >> hello_task >> end
