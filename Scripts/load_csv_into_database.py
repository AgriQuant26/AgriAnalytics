from sqlalchemy import create_engine
from notebookutils import mssparkutils

#Load csv file into sql 
def load_csv_to_sql(
    spark,
    csv_path: str,
    table_name: str,
    connection_string: str,
    schema: str = None,
    if_exists: str = "replace" #replace used for testing
    ):
        #read csv 
        df=spark.read.option("header",True).option("inferSchema",True).csv(csv_path).toPandas()
        engine = create_engine(connection_string)
        
        df.to_sql(
            name = table_name,
            con=engine,
            schema=schema,
            if_exists= if_exists,
            index=False
        )

        full_table_name = f"{schema}.{table_name}" if schema else table_name
        print(f"Loaded {len(df)} rows into {full_table_name}")

#function to load csv into sqldb
def load_folder_to_sql(spark, folder_path: str,
    connection_string: str,
    schema: str = None,
    if_exists: str = "replace"):
    #check if folder exists
    try:
        files = mssparkutils.fs.ls(folder_path)
    except Exception as e:
        raise FileNotFoundError(f"Folder not found: {folder_path}") from e
        
    existing_items= [f.name.rstrip("/") for f in files]

    if not existing_items:
        raise ValueError(f"No files or folders found in: {folder_path}")
        
        
    #load dimensions first and then facts
    dim_files = sorted([f for f in existing_items if f.lower().startswith("dim_")])
    fact_files = sorted([f for f in existing_items if f.lower().startswith("fact_")])
    other_files = sorted([f for f in existing_items if f not in dim_files + fact_files])
        
    #build final load order
    ordered_files = dim_files + fact_files + other_files
        
    #print the final load order
    print(" Files to loaded in this order:")
    for file_name in ordered_files:
        print(f"-{file_name}")

    #loop through files in folder and load each one   
    for file_name in ordered_files:
        csv_path = folder_path.rstrip("/")+"/"+file_name+"/"
        table_name = file_name
        load_csv_to_sql(spark, csv_path=csv_path, table_name=table_name,
            connection_string=connection_string, schema=schema, if_exists= if_exists)

def run_pipeline(spark):
    folder_path = "abfss://datalake-agrisynapse@agrisynapse.dfs.core.windows.net/gold/fact_and_dimensions/"
    schema = 'dbo'
    
    connection_string = ("mssql+pyodbc://sqladmin:Agriquant26@agri-sql-server.database.windows.net/AgriAnalyiticsDB"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
    "&Connection+Timeout=30")

    load_folder_to_sql(spark, folder_path, connection_string, schema, if_exists="replace") #replace used for testing 