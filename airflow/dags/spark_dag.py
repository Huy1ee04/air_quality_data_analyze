from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'airflow_exec_spark_submit_in_container',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run spark-submit inside spark container from Airflow',
) as dag:

    spark_submit = BashOperator(
        task_id='run_spark_job',
        bash_command="""
        docker exec spark-worker-driver spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          /opt/spark/work-dir/write_to_bigquery.py
        """
    )