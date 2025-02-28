import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import boto3, os
from sqlalchemy import  create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

original_file = pd.read_csv("resources/crime_incidents_by_category.csv")

conf = {
    'host': os.getenv("MASTER_ENDPOINT"),
    'port': os.getenv("MASTER_PORT"),
    'database': os.getenv("MASTER_DBNAME"),
    'user': os.getenv("MASTER_USERNAME"),
    'password': os.getenv("MASTER_PASSWORD")
}

engine = create_engine(
    "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        **conf
        )
    )

session = sessionmaker(bind=engine)


def download_files_aws(file_name, bucket):
    try:
        s3_bucket = boto3.client(
            "s3",
            region_name="af-south-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        resources_path = os.path.dirname(os.path.abspath(__file__))
        destination_path = os.path.join(resources_path, "resources")
        os.makedirs(destination_path, exist_ok=True)
        file_dest = os.path.join(destination_path, file_name)

        if not os.path.exists(file_dest):
            s3_bucket.download_file(bucket, file_name, file_dest)
            print("file downloaded")
        else:
            print("File exists")
    except Exception as e:
        print("An error has occurred", e)


def fetch_aggrigated_by_cat():
    session_obj = session()

    query = """
    SELECT * FROM crime_by_category;
    """
    result = session_obj.execute(query)

    data = result.fetchall()
    # st.write(data)
    return data

# Centered title
st.markdown(
    "<h1 style='text-align: center; color: white;'>Crime Stats</h1>",
    unsafe_allow_html=True
)

# Dropdown to select aggregation method
option = st.selectbox(
    "Select Aggregation",
    ["Original Stats", "Aggregated by category"]
)

# Process Data Based on Selection
if option == "Original Stats":
    df_result = original_file
elif option == "Aggregated by category":
    df_result = fetch_aggrigated_by_cat()

# Full-width DataFrame
st.dataframe(df_result, use_container_width=True)