import os 
from dotenv import load_dotenv
from scripts.with_duckdb import load_to_duckdb
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

load_dotenv()

def run_pipeline():
    file_path = "./etl_project/data/companies.csv"
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_name = "companies_cleaned"

    print("Starting ETL Pipeline....")

    df = extract_data(file_path)
    if df is None:
        print("Extraction failed. Exiting pipeline.")
        return
    df_transformed = transform_data(df)

    #Load Data to DuckDB (Optional)
    load_to_duckdb(df_transformed)
    #load Data to BigQuery
    #load_data(df_transformed, project_id, dataset_id, table_name)
    print("ETL Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
#the above if statement ensures that the run_pipeline functuin is called only when this script is executed directly, not when imported as a module.


