#Optional Script using Prefect to orchestrate the ETL Pipeline

from prefect import flow,task
import pandas as pd 
from scripts.with_duckdb import load_to_duckdb
from scripts.extract import extract_data
from scripts.transform import transform_data

@flow
def etl_flow():
    df = extract_data("./etl_project/data/companies.csv")
    clean_df = transform_data(df)
    load_to_duckdb(clean_df)
    
if __name__ == "__main__":
    etl_flow()
