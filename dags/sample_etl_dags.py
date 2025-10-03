import datetime
import pandas as pd
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

with DAG(
    dag_id="sample_etl_dags",
    start_date=datetime.datetime(2025, 10, 1),
    schedule=None,
):

    @task
    def get_customer_data():
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        customers_df = pg_hook.get_df("SELECT * FROM customers;")
        file_path = "/tmp/customers.csv"
        customers_df.to_csv(file_path, index=False)
        return file_path
    
    @task
    def transform_customer_data(ti):
        source_file_path = ti.xcom_pull(task_ids="get_customer_data")
        customers_df = pd.read_csv(source_file_path)

        # Format the datetime from "12 May 1990" to "1990-05-12"
        customers_df["birthdate"] = pd.to_datetime(customers_df["birthdate"], format="%d %B %Y").dt.strftime("%Y-%m-%d")
        transform_file_path = "/tmp/customers.parquet"
        customers_df.to_parquet(transform_file_path)
        print(customers_df.head())
        return transform_file_path

    @task
    def load_data_to_landing(ti):
        transform_file_path = ti.xcom_pull(task_ids="transform_customer_data")
        s3_hook = S3Hook(aws_conn_id="my_aws_connection")
        s3_hook.load_file(
            filename=transform_file_path,
            key="test/customers.parquet",
            bucket_name="pea-watt",
            replace=True
        )
        

    get_customer_data() >> transform_customer_data() >> load_data_to_landing()