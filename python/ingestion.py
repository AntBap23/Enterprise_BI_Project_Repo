# Data ingestion script
import pandas as pd
import sqlalchemy as sa
import os
from dotenv import load_dotenv

load_dotenv()

engine = sa.create_engine(os.getenv("DATABASE_URL"))

def ingest_data(file_path, table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists="replace", index=False)