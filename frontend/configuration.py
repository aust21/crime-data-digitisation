import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Read CSV
file_path = os.path.dirname(os.path.abspath(__file__))
filename = os.path.join(file_path, "resources",
                        "crime_incidents_by_category.csv")
original_file = pd.read_csv(filename)

# Database Config
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
