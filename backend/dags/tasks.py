import json, requests, os, sys
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
dag_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_path)
from aggregated_data_model import CrimeByCategory, CrimeData
import redis
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable

logger = LoggingMixin().log

conf = {
    'host': Variable.get("MASTER_ENDPOINT", default_var=None),
}

def generate_cache_key(row):
    """Generate a unique key for each row based on important fields"""
    return (f"crime:"
            f"{row.get('Geography')}:{row.get('Financial Year')}"
            f":{row.get('Crime Category')}")


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


def load_to_redis(file):
    df = pd.read_csv(file)
    conn = BaseHook.get_connection("redis")

    try:
        # for batch inserts
        pool = redis.ConnectionPool(
            host=conn.host, port=6379,
            decode_responses=True
        )
        r = redis.Redis(connection_pool=pool)

        with r.pipeline() as pipe:
            for indx, row in df.iterrows():
                key = f"row:{indx}"
                # logger.info(f"processing row: {key}")
                pipe.set(key, json.dumps(row.to_dict()))

            # insert as a batch
            pipe.execute()
    except Exception as e:
        logger.error(f"Something went wrong with Redis: {str(e)}",
                     exc_info=True)
    finally:
        # Clean up connection pool
        pool.disconnect()


def redis_to_postgres():
    conn = BaseHook.get_connection("redis")
    try:
        pool = redis.ConnectionPool(
            host=conn.host, port=6379, decode_responses=True
        )
        r = redis.Redis(connection_pool=pool)
        engine = create_engine(
            "postgresql://avnadmin:{host}:13557/defaultdb?sslmode"
            "=require".format(
                **conf
            )
        )
        session = sessionmaker(bind=engine)
        session_obj = session()
        # batch inserts
        batch_size = 50
        rows_to_insert = []

        #prefetch data from postgres
        stmt = select(CrimeData)
        results = session_obj.execute(stmt).fetchall()

        for row_key in results:
            row = row_key[0]
            logger.info(f"row: {row}")
            cache_key = f"crime:{row.geography}:{row.financial_year}:{row.crime_category}"
            r.setex(cache_key, 3600, "1")

        # scan for efficient look
        for key in r.scan_iter(match="row:*"):
            row = json.loads(r.get(key))
            cache_key = generate_cache_key(row)
            if r.exists(cache_key):
                logger.warning(f"Duplicate entry: {cache_key}")
                continue
            geo = row.get("Geography")
            if not geo:
                continue
            data = CrimeData(
                geography=geo,
                crime_category = row["Crime Category"],
                financial_year = row["Financial Year"],
                crime_count = row["Count"]
            )

            rows_to_insert.append(data)

            if len(rows_to_insert) >= batch_size:
                session_obj.bulk_save_objects(rows_to_insert)
                session_obj.commit()
                rows_to_insert = []
        if rows_to_insert:
            session_obj.bulk_save_objects(rows_to_insert)
            session_obj.commit()
        logger.info("Data written to postgres from redis successfully")
    except Exception as e:
        logger.error(
            f"Something went wrong with Redis to Postgres: {str(e)}",
            exc_info=True
            )
    finally:
        # Cleanup
        session_obj.close()
        engine.dispose()
        pool.disconnect()


def transform(data_dir, file_name):
    true_path = os.path.join(data_dir, file_name)
    df = pd.read_csv(true_path)
    new_df = df.groupby("Crime Category").size().reset_index(
        name="total_count")
    return new_df

def load(data):

    try:
        engine = create_engine(
            "postgresql://avnadmin:{host}:13557/defaultdb?sslmode"
            "=require".format(
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

def load_with_xcom(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids="transform_data")

    if transformed_data is None:
        raise ValueError("No transformed data found in XCom.")

    load(transformed_data)