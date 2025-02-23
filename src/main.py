import boto3, os
from dotenv import load_dotenv

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

def read_files(bucket_name, file_name, target_name):
    s3.download_file(bucket_name, file_name, file_name)
    print("file downloaded")

read_files(
    "crime-files-bucket",
    "crime_incidents_by_category.csv",
    "crime_incidents_by_category.csv")