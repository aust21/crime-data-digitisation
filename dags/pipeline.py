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
    def extract_data(bucket_name, file_name, data_dir):
        try:
            aws_client = S3Hook(aws_conn_id="conn", region_name="af-south-1")
            s3_client = aws_client.get_conn()
            os.makedirs(data_dir, exist_ok=True)
            file_path = os.path.join(data_dir, file_name)

            logger.info(f"Attempting to download {file_name} to {file_path}")

            if not os.path.exists(file_path):
                s3_client.download_file(bucket_name, file_name, file_path)
                logger.info("File downloaded successfully")
            else:
                logger.warning("File downloading skipped, file already exists.")

        except Exception as e:
            logger.error(f"An error has occurred: {str(e)}", exc_info=True)


    @task
    def transform(data_dir, file_name):
        true_path = os.path.join(data_dir, file_name)
        logger.info(f"Path to downloaded file: {true_path}")
        df = pd.read_csv(true_path)
        new_df = df.groupby("Crime Category").nunique()
        print(new_df)
        return new_df



    data_dir = "/usr/local/airflow/data"
    file_name = "crime_incidents_by_category.csv"
    extract_data(
        "crime-files-bucket",
        file_name, data_dir)

    transform(data_dir, file_name)