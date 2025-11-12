from pandas_gbq import to_gbq

def load_data(df, project_id, dataset_id, table_name):
    try:
        table_id = f"{dataset_id}.{table_name}"
        to_gbq(df, table_id, project_id=project_id, if_exists='replace')
        print(f"Loaded data to BigQuery table: {table_id}")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")

