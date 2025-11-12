import pandas as pd
import numpy as np

def transform_data(df):
    try:
        # Change column names to uppercase
        df.columns = df.columns.str.upper(inplace=True)
        #-----------------------------------------------------------------------------------
        # Create an index column
        df.reset_index(inplace=True)
        #----------------------------------------------------------------------------------- 
        #create a labelling column 
        conditions = [
            df["COMPANY_SIZE"].between(1,3),
            df["COMPANY_SIZE"].between(4,6),
            df["COMPANY_SIZE"] >= 7
        ]
        labels = ["micro-company", "mini-company", "small-company"]
        df["COMPANY_SIZE_LABELS"] = np.select(conditions, labels, default="unknown")
        #-----------------------------------------------------------------------------------
        # Drop the last row in the dataframe
        df.drop(df.tail(1).index, inplace=True)
        #-----------------------------------------------------------------------------------
        # filter out rows where state = 0
        df =df[df["STATE"] != 0]
        #-----------------------------------------------------------------------------------
        # sort the df by ascending order of company_id
        df = df.sort_values(by="COMPANY_ID", ascending=True)
        #-----------------------------------------------------------------------------------
        # drop duplicate rows based on company_id, keeping the first occurance
        df.drop_duplicates(subset="COMPANY_ID", keep="first", inplace=True)
        #-----------------------------------------------------------------------------------
        # drop entire blank rows 
        df.dropna(how='all', inplace=True)
        #-----------------------------------------------------------------------------------
        # replace a value with null in a column 
        df["ADDRESS"] = df["ADDRESS"].replace("-", np.nan)
        df["ADDRESS"] = df["ADDRESS"].replace("--", np.nan)
        #-----------------------------------------------------------------------------------
        
        print("Transformation complete.")
        return df
    except Exception as e:
        print(f"Error transforming file: {e}")
        return df