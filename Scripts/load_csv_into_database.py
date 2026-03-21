import os                                                       
import pandas as pd 
from sqlalchemy import create_engine

#Load csv file into sql 
def load_csv_to_sql(
    csv_path: str,
    table_name: str,
    connection_string: str,
    schema: str = None,
    if_exists: str = "replace" #replace used for testing
):

    #read csv
    df=pd.read_csv(csv_path)

    engine = create_engine(connection_string)

    df.to_sql(
        name = table_name,
        con=engine,
        schema=schema,
        if_exists= if_exists,
        index=False
    )

    full_table_name = f"(schema).{table_name}" if schema else table_name
    print(f"Loaded {len(df)} rows from {os.path.basename(csv_path)}'into' {full_table_name}'")

#function to load csv into sqldb
def load_folder_to_sql(folder_path: str,
                       connection_string = str,
                       schema: str = None,
                       if_exists: str = "replace"):
        #check if folder exists
        if not os.path.exists(folder_path):
             raise FileNotFoundError(f"File not found:{folder_path}")
        #find csv files 
        csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]
        
        #check if any csv files exists 
        if not csv_files:
             raise ValueError(f"No CSV files found in folder: {folder_path}")
        
        #load dimensions first and then facts
        dim_files = sorted([f for f in csv_files if f.lower().startswith("dim_")])
        fact_files = sorted([f for f in csv_files if f.lower().startswith("fact_")])
        other_files = sorted([f for f in csv_files if f not in dim_files + fact_files])
        
        #build final load order
        ordered_files = dim_files + fact_files + other_files
        
        #print the final load order
        print(" Files to loaded in this order:")
        for file_name in ordered_files:
             print(f"-{file_name}")

        #loop through files in folder and load each one   
        for file_name in ordered_files:
             csv_files = os.path.join(folder_path, file_name)
             table_name = os.path.splitext(file_name)[0]
             load_csv_to_sql(csv_path=csv_files, table_name=table_name,
                      connection_string=connection_string, schema=schema, if_exists= if_exists)

if __name__ == "__main__":
    folder_path = "C:\\Users\\chowd\\VS_Projects\\Fact_and_Dimensions"
    schema = 'dbo'
    
    connection_string = (
    "mssql+pyodbc://@NIKI\\SQLEXPRESS/AgriAnalyticsDB"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes")

    load_folder_to_sql(folder_path, connection_string, schema, if_exists="replace") #replace used for testing 