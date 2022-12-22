import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
#from dags.etl.config import BUCKET_BASE, LANDING_ZONE

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


list_tables = ["state","city","zone","district","branch",
               "marital_status","customer","department",
               "employee","product_group","supplier",
               "product","sale","sale_item"]

default_args = {
    'owner': 'coder2 - Albertoj',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def export_from_postgres_to_minio_s3():

    hook = PostgresHook(postgres_conn_id="postgres_shopping_db")
    conn = hook.get_conn()
    s3_hook = S3Hook(aws_conn_id="minio_conn")

    for item_table in list_tables:
    
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {item_table}")

        with NamedTemporaryFile(mode='w', suffix=f"{item_table}") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([i[0] for i in cursor.description])
            csv_writer.writerows(cursor)
            f.flush()
            cursor.close()

            key = f"datalake/{item_table}.csv"

            s3_hook.load_file(
                filename=f.name,
                key=key,
                bucket_name="datalake",
                replace=True
            )
            logging.info(f"Saved {item_table} table in csv file: s3a://datalake/{key}")
    conn.close()    

# Exporta do bancode dados em csv as tabelas relacionadas
# as vendas para o bucket Minio S3
with DAG(
    dag_id="dag_with_postgres_hooks_v04",
    default_args=default_args,
    start_date=datetime(2022, 4, 30),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=export_from_postgres_to_minio_s3
    )
    task1

    