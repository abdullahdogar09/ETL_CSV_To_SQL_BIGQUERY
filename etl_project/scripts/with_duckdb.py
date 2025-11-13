import pandas as pd
import duckdb

def load_to_duckdb(df, db_name="etl_project.duckdb", table_name="companies_cleaned"):
    try:
        conn = duckdb.connect(db_name)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
        print(f"Loaded data to Duckdb table: {table_name}")
        conn.close()
    except Exception as e:
        print(f"Error loading data to Duckdb: {e}")