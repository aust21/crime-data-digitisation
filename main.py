import boto3, os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")

current_dir = os.path.dirname(os.path.abspath(__name__))
path_to_files = os.path.join(current_dir, "resources")

s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
bucket_name = "crime-files-bucket"

df = pd.read_csv("crime_incidents_by_category.csv")
print(df.groupby("Crime Category").nunique())

# MASTER_PASSWORD=os.getenv("MASTER_PASSWORD")MASTER_PASSWORD=os.getenv("MASTER_PASSWORD")
# MASTER_USERNAME=os.getenv("MASTER_USERNAME")
# MASTER_ENDPOINT=os.getenv("MASTER_ENDPOINT")
# MASTER_PORT=os.getenv("MASTER_PORT")
# MASTER_DBNAME=os.getenv("MASTER_DBNAME")
#
# conn = psycopg2.connect(
#     host=MASTER_ENDPOINT,
#     database=MASTER_DBNAME,
#     user=MASTER_USERNAME,
#     password=MASTER_PASSWORD
#     )
#
# cur = conn.cursor()
# cur.execute("SELECT version()")
# db_version = cur.fetchone()
# print(db_version)
# MASTER_USERNAME=os.getenv("MASTER_USERNAME")
# MASTER_ENDPOINT=os.getenv("MASTER_ENDPOINT")
# MASTER_PORT=os.getenv("MASTER_PORT")
# MASTER_DBNAME=os.getenv("MASTER_DBNAME")
#
# conn = psycopg2.connect(
#     host=MASTER_ENDPOINT,
#     database=MASTER_DBNAME,
#     user=MASTER_USERNAME,
#     password=MASTER_PASSWORD
#     )
#
# cur = conn.cursor()
# cur.execute("SELECT version()")
# db_version = cur.fetchone()
# print(db_version)