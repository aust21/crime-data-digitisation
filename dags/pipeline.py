import json, requests, os, boto3
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
    def extract_data(bucket_name, file_name):
        try:
            aws_client = S3Hook(aws_conn_id="awsconn")
            resources = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(resources, file_name)

            if not os.path.exists(file_path):
                # aws_client.download_file(
                #     bucket_name,
                #     file_name,
                #     file_path
                # )
                logger.info("Files downloaded")
            else:
                logger.warning("File downloading skipped, file already exists.")
        except Exception as e:
            logger.error(f"An error has occurred with downloading files: {str(e)}", exc_info=True)


    @task
    def transform():
        pass


    extract_data(
        "crime-files-bucket",
        "crime_incidents_by_category.csv")