import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="sample_dynamic_task", 
    schedule=None,
    catchup=False, 
    start_date=datetime.datetime(2025, 10, 1)
):

    @task
    def get_data():
        return [
            {"data" : 1, "name": "a"}, 
            {"data" : 2, "name": "b"}, 
            {"data" : 3, "name": "c"}
        ]

    @task
    def print_data(data):
        print(f"data: {data}")


    print_data.expand(data=get_data())