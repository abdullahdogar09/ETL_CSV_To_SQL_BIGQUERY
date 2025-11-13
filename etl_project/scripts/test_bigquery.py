import pandas as pd 
from pandas_gbq import to_gbq

data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}

df = pd.DataFrame(data)

project_id = "etl-csv-to-sql-bigquery"
dataset_id = "etl_demo_project"
table_name = "test_table"

to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')

print("Data loaded successfully to BigQuery.")
