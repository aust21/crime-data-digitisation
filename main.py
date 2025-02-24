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
bucket_name = "crime-files-bucket"

response = s3.list_objects_v2(Bucket=bucket_name)
for obj in response.get("Contents", []):
    print(obj["Key"])  # List all filenames
