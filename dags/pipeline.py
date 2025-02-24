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

    @task
    def load_files(bucket_name, file_name, aws_client):
        try:
            resources = os.path.dirname(os.path.abspath(__name__))
            file_path = os.path.join(resources, file_name)

            if os.path.exists(file_path):
                aws_client.download_file(
                    bucket_name,
                    file_name,
                    file_path
                )
                logger.info("Files downloaded")
            else:
                logger.warning("File downloading skipped, file already exists.")
        except Exception as e:
            logger.error("An error has occurred with downloading files", e)