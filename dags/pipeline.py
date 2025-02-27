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
            aws_client = S3Hook(aws_conn_id="conn", region_name="af-south-1")

            # Get detailed connection info for debugging
            conn = aws_client.get_connection(aws_client.aws_conn_id)
            logger.info(f"Connection region: {aws_client.region_name}")

            # Get the S3 client
            s3_client = aws_client.get_conn()

            # List buckets to verify credentials
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            logger.info(f"Available buckets: {buckets}")

            # Check if our bucket exists in the list
            if bucket_name in buckets:
                logger.info(f"Bucket {bucket_name} found")
            else:
                logger.warning(
                    f"Bucket {bucket_name} NOT found in available buckets"
                    )

            # List objects with full response for debugging
            response = s3_client.list_objects_v2(Bucket=bucket_name)

            if 'Contents' in response:
                for obj in response['Contents']:
                    logger.info(
                        f"Object key: {obj['Key']}, Size: {obj['Size']}"
                        )

            data_dir = "/usr/local/airflow/data"

            # Make sure directory exists
            os.makedirs(data_dir, exist_ok=True)

            # Use this path for downloads
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
    def transform():
        pass


    extract_data(
        "crime-files-bucket",
        "crime_incidents_by_category.csv")