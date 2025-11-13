import os 
from dotenv import load_dotenv
import pandas as pd 
from pandas_gbq import to_gbq

load_dotenv()

data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35]
}

df = pd.DataFrame(data)

project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_name = "test_table"

to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, if_exists='replace')

print("Data loaded successfully to BigQuery.")
