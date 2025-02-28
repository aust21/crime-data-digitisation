import boto3
import os

def download_files_aws(file_name, bucket):
    try:
        s3_bucket = boto3.client(
            "s3",
            region_name="af-south-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        resources_path = os.path.dirname(os.path.abspath(__file__))
        destination_path = os.path.join(resources_path, "../resources")
        os.makedirs(destination_path, exist_ok=True)
        file_dest = os.path.join(destination_path, file_name)

        if not os.path.exists(file_dest):
            s3_bucket.download_file(bucket, file_name, file_dest)
            print("file downloaded")
        else:
            print("File exists")
    except Exception as e:
        print("An error has occurred", e)
