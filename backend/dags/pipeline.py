import json, requests, os, sys
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
dag_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_path)
from pendulum import datetime, duration
from airflow.operators.python import PythonOperator

import tasks

load_dotenv()
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")

logger = LoggingMixin().log

default_args = {
    "owner":"crime-data",
    "start_date":days_ago(1),
    "retries": 3,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=2),
}

with DAG(
    dag_id="crime-data-id",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_1 = PythonOperator(
        task_id="extract_data",
        python_callable=tasks.extract_data,
        op_args=[
            "crime-files-bucket",
            "crime_incidents_by_category.csv",
            "/usr/local/airflow/data"
        ]
    )

    task_2 = PythonOperator(
        task_id="load_data_to_redis",
        python_callable=tasks.load_to_redis,
        op_args=[
            os.path.join("/usr/local/airflow/data",
                         "crime_incidents_by_category.csv"
                        )
        ]
    )

    task_3 = PythonOperator(
        task_id="redis_to_postgres",
        python_callable=tasks.redis_to_postgres
    )

    task_4 = PythonOperator(
        task_id="transform_data",
        python_callable=tasks.transform,
        op_args=["/usr/local/airflow/data", "crime_incidents_by_category.csv"],
        provide_context=True  # Allows access to `kwargs`
    )

    task_5 = PythonOperator(
        task_id="load_data",
        python_callable=tasks.load_with_xcom,
        provide_context=True
    )


    task_1 >> task_2 >> task_3 >> task_4 >> task_5