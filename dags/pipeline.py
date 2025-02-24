import json, requests, os, boto3
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")

logger = LoggingMixin().log

default_args = {
    "owner":"crime-data",
    "start_date":days_ago(1)
}

with DAG(
    dag_id="crime-data-id",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    @task
    def connect_to_aws():
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            logger.info("AWS connection successful")
            return s3
        except Exception as e:
            logger.error("An error has occurred with connecting to AWS", e)

