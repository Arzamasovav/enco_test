from datetime import datetime

from airflow.sdk import dag

from task_2.task import upload_test_dataset


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
)
def task_2_dag():
    upload_test_dataset()


task_2_dag()
