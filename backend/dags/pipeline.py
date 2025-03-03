import json, requests, os, sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
dag_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_path)
from pendulum import datetime, duration
from aggregated_data_model import CrimeByCategory
import redis
from airflow.hooks.base import BaseHook


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
    def load_to_redis(file):
        df = pd.read_csv(file)
        conn = BaseHook.get_connection("redis")
        try:
            r = redis.Redis(host=conn.host, port=6379, decode_responses=True)

            for indx, row in df.iterrows():
                key = f"row:{indx}"
                r.set(key, json.dumps(row.to_dict()))
            logger.info("Data written to redis successfully")
        except Exception as e:
            logger.error(f"Something went wrong with Redis: {str(e)}",
                         exc_info=True)


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
            'host': Variable.get("MASTER_ENDPOINT", default_var=None),
            'port': Variable.get("MASTER_PORT", default_var=None),
            'database': Variable.get("MASTER_DBNAME", default_var=None),
            'user': Variable.get("MASTER_USERNAME", default_var=None),
            'password': Variable.get("MASTER_PASSWORD", default_var=None)
        }
        logger.info(f"port: {conf['port']}, password: {conf['password']}, "
                    f"database: {conf['database']}, user: {conf['user']}, "
                    f"host: {conf['host']}")
        logger.info(f"Direct retrieve {os.getenv('MASTER_PORT')}")
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

    load_to_redis(os.path.join(data_dir, file_name))

    data = transform(data_dir, file_name)
    load(data)