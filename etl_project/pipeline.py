from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

def run_pipeline():
    file_path = "data/companies.csv"
    project_id = "etl-csv-to-sql-bigquery"
    dataset_id = "etl_demo_project"
    table_name = "companies_cleaned"

    print("Starting ETL Pipeline....")

    df = extract_data(file_path)
    if df is None:
        print("Extraction failed. Exiting pipeline.")
        return
    df_transformed = transform_data(df)

    load_data(df_transformed, project_id, dataset_id, table_name)
    print("ETL Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()


