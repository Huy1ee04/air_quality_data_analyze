from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ETL import extract
from ETL import load

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "air_quality_to_gcs",
    default_args=default_args,
    schedule_interval= None,   # "5 12 * * *",  # Runs daily at 12:00 UTC (~19:00 VN)
    catchup=False
)

task_fetch_data = PythonOperator(
    task_id="fetch_air_quality_data",
    python_callable=extract.fetch_air_quality_data,
    dag=dag
)

task_upload_gcs = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=load.upload_to_gcs,
    dag=dag
)

task_fetch_data >> task_upload_gcs
