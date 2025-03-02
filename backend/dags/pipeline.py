import json, requests, os, sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
dag_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_path)
# from backend.dags.aggregated_data_model import CrimeByCategory
from aggregated_data_model import CrimeByCategory


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
        df = pd.read_csv(true_path)
        new_df = df.groupby("Crime Category").size().reset_index(
            name="total_count")
        return new_df

    @task
    def load(data):
        conf = {
            'host': os.getenv("MASTER_ENDPOINT"),
            'port': os.getenv("MASTER_PORT"),
            'database': os.getenv("MASTER_DBNAME"),
            'user': os.getenv("MASTER_USERNAME"),
            'password': os.getenv("MASTER_PASSWORD")
        }

        try:
            engine = create_engine(
                "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                    **conf
                    )
                )
            logger.info("SQLachemy connection successful")

            session = sessionmaker(bind=engine)
            session_obj = session()
            for _, row in data.iterrows():
                crime_entry = CrimeByCategory(
                    category = row["Crime Category"],
                    total_count = int(row["total_count"])
                )

                session_obj.add(crime_entry)
            session_obj.commit()
            session_obj.close()
            logger.info("Data written to postgres")
        except Exception as e:
            logger.error(f"Failed to create sqlachemy instance {e}",
                         exc_info=True)

    data_dir = "/usr/local/airflow/data"
    file_name = "crime_incidents_by_category.csv"
    extract_data(
        "crime-files-bucket",
        file_name, data_dir)

    data = transform(data_dir, file_name)
    load(data)