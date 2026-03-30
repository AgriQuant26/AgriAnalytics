from notebookutils import mssparkutils

def run_validation(spark):
    base_path = "abfss://datalake-agrisynapse@agrisynapse.dfs.core.windows.net/gold/fact_and_dimensions/"

    print("start validation...\n")

    #check if required output folders exists
    required_outputs = ["dim_location", "dim_year", "dim_crop", "fact_production"]
    existing_folders = [f.name.rstrip("/") for f in mssparkutils.fs.ls(base_path)]

    print("Available output folders:", existing_folders)

    #check missing outputs
    missing_outputs = [name for name in required_outputs if name not in existing_folders]
    if missing_outputs:
        raise ValueError(f"Missing expected output folders:{missing_outputs}")
    
    #print success if all expceted outputs exist
    print("All expected outputs exist.\n")

    #read each output into spark 
    dim_location = spark.read.option("header",True).option("inferSchema", True).csv(base_path+"dim_location/")
    dim_crop = spark.read.option("header", True).option("infraSChema",True).csv(base_path+"dim_crop/")
    dim_year = spark.read.option("header",True).option("infraSchema",True).csv(base_path+"dim_year/")
    fact_production = spark.read.option("header",True).option("infraSchema", True).csv(base_path+"fact_production/")

    #sanity checks
    loc_count = dim_location.count()
    crop_count = dim_crop.count()
    year_count = dim_year.count()
    fact_count = fact_production.count()

    #print row counts
    print("Row counts: \n dim_location:{loc_count} \n dim_crop:{crop_count} \n dim_year:{year_count} \n fact_production:{fact_count}\n")
    
    #check primary key uniqueness for dimensions
    loc_unique = dim_location.select("location_id").distinct().count()
    crop_unique = dim_crop.select("crop_id").distinct().count()
    year_unique = dim_year.select("year").distinct().count()

    #check for duplicate primary keys in dimensions
    if loc_unique != loc_count:
        raise ValueError("dim_location.location_id is not unique")
    if crop_unique != crop_count:
        raise ValueError("dim_crop.crop_id is not unique")
    if year_unique != year_count:
        raise ValueError("dim_year.year is not unique")
    
    print("primary key uniqueness checks passed.\n")

    #check for nulls in fact
    null_location = fact_production.filter("location_id IS NULL").count()
    null_crop = fact_production.filter("crop_id IS NULL").count()
    null_year = fact_production.filter("year IS NULL").count()

    #print null counts 
    print("Null foreign key counts in fact: \n location_id nulls: {null_location} \n crop_id nulls: {null_crop} \n year nulls: {null_year}\n")

    #stop validation id foreign keys are missing 
    if null_location > 0:
        raise ValueError("fact_production has {null_location} null location_id values.")
    if null_crop>0:
        raise ValueError("fact_production has {null_crop} null crop_id values.")
    if null_year>0:
        raise ValueError("fact_production has {null_year} null year values.")
    
    #print success if fact has all required keys populated
    print("Fact foreign key null checks passed.\n")

    #referential integrity checks
    fact_loc_joint_count = fact_production.join(dim_location, on="location_id", how="inner").count()
    fact_crop_joint_count = fact_production.join(dim_crop, on="crop_id", how="inner").count()
    fact_year_joint_count = fact_production.join(dim_year, on="year", how="inner").count()

    #compare join_count and fact_counts to check if fact rows successfully maps to dimensions
    if fact_loc_joint_count != fact_count:
        raise ValueError("some fact_production rows do not match dim_location")
    if fact_crop_joint_count != fact_count:
        raise ValueError ("some fact_production rows do not match dim_crop")
    if fact_year_joint_count != fact_count:
        raise ValueError("some fact_production rows do not match dim_year")

    #print success if all fact keys correctly map to dimensions 
    print("Referential integrity checks passed.\n") 

    #Duplicate grain checks (location + crop + year combo should be unique)
    duplicate_grain = (fact_production.groupBy("location_id", "crop_id", "year").count()
    .filter("count > 1").count())

    #print dupliate count
    print("Duplicate fact grain rows:{duplicate_grain}\n")

    #fail if duplicate exist
    if duplicate_grain > 0:
        raise ValueError("Duplicate rows found at fact grain: location_id + crop_id + year")
    
    #print success if fact has the expected granuality
    print("Fact grain uniqueness checks passed.\n")

    #Negative measure checks
    measure_cols = ["area", "production", "fertilizer_qty", "pesticide_qty", "annual_rainfall"]
    
    #loop through all measures
    for col in measure_cols:
        if col in fact_production.columns:
            negative_count = fact_production.filter(f"{col} < 0").count()
            print(f"Negative Values in {col}: {negative_count}")
            if negative_count > 0:
                raise ValueError(f"Negative values found in {col}.")
            
    #print success if measures are not negative
    print("\nNegative measure checks passed.\n")

    #Schema 
    print("Schemas: \n")
    print("dim_location_schema:")
    dim_location.printSchema()
    print("dim_crop_schema:")
    dim_crop.printSchema()
    print("dim_year_schema:")
    dim_year.printSchema()
    print("fact_production_schema:")
    fact_production.printSchema()
    
    #sample rows
    print("\nsample rows from dim_location:")
    dim_location.show(5, truncate=False)

    print("\nsample rows from dim_crop:")
    dim_crop.show(5, truncate=False)

    print("\nsample rows from dim_year:")
    dim_year.show(5, truncate=False)

    print("\nsample rows from fact_production:")
    fact_production.show(5, truncate=False)

    #print final success message if all validations checks are passed
    print("validation completed successfully.")

