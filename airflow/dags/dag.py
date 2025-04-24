from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import extract
import load

# DAG Configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "air_quality_to_gcs",
    default_args=default_args,
    schedule_interval= "5 0 * * 1",  # Runs weekly at Monday, 7:05 UTC+7
    catchup=False
)

task_fetch_data = PythonOperator(
    task_id="fetch_air_quality_data",
    python_callable=extract.fetch_air_quality_data,
    dag=dag,
    on_failure_callback=lambda context: print("Fetch data failed!")
)

task_upload_gcs = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=load.upload_to_gcs,
    dag=dag,
    on_failure_callback=lambda context: print("Upload to GCS failed!")
)

task_fetch_data >> task_upload_gcs