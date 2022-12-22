from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

spark_master = "spark://spark:7077"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "store",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-etl", 
        description="DAG - ETL Normalization",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_landing_to_consolidation = SparkSubmitOperator(
    task_id="spark_job_load_landing_to_consolidation",
    application="/usr/local/airflow/dags/etl/normalization.py", 
    name="load-landing-to-consolidation",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)    

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_landing_to_consolidation >> end
