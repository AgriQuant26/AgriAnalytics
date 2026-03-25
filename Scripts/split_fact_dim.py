import os 
import pandas as pd

input_file = "AgriAnalytics\\cleaned_crop_data_final.csv"
#create output folder
output_folder = "AgriAnalytics\\Fact_and_Dimensions"
os.makedirs(output_folder,exist_ok=True)

df = pd.read_csv(input_file)
#standardise column names
df.columns = (df.columns.str.strip()
                .str.lower()
                .str.replace(" ","_"))
    
#fetch text and numeric columns
text_cols = list(df.select_dtypes(include="object").columns)
numeric_cols = list(df.select_dtypes(include=["int64","float64"]).columns)

#check if any required columns are missing                                                                                                                                                  
time_cols = [col for col in df.columns if "year" in col.lower()]
geo_cols =[col for col in df.columns if "state" in col.lower()]
crop_cols = [col for col in df.columns if "crop" in col.lower()]

if not time_cols:
    raise ValueError("Missing time column, Expected a column containing 'year'.")
if not geo_cols:
    raise ValueError("Missing geography columns, Expected a column containing 'State'.")
if not crop_cols:
    raise ValueError("Missing crop-related columns, Expected a column containing 'crop'.")
if not numeric_cols:
    raise ValueError("Missing numeric columns")
    
#drop duplicates
df = df.drop_duplicates().copy()

#Dimension Location
#standardise text column 
df["state"] = df["state"].where(
    df["state"].isna(),
    df["state"].astype(str).str.strip().str.title()
)

#fetch unqiue state names
dim_location = (df[["state"]].dropna().drop_duplicates()
                .sort_values("state").reset_index(drop=True))
    
Zone_map = { "Andhra Pradesh": "South",
    "Arunachal Pradesh": "North-East",
    "Assam": "North-East",
    "Bihar": "East",
    "Chhattisgarh": "Central",
    "Goa": "West",
    "Gujarat": "West",
    "Haryana": "North",
    "Himachal Pradesh": "North",
    "Jharkhand": "East",
    "Karnataka": "South",
    "Kerala": "South",
    "Madhya Pradesh": "Central",
    "Maharashtra": "West",
    "Manipur": "North-East",
    "Meghalaya": "North-East",
    "Mizoram": "North-East",
    "Nagaland": "North-East",
    "Odisha": "East",
    "Punjab": "North",
    "Rajasthan": "West",
    "Sikkim": "North-East",
    "Tamil Nadu": "South",
    "Telangana": "South",
    "Tripura": "North-East",
    "Uttar Pradesh": "North",
    "Uttarakhand": "North",
    "West Bengal": "East",
    "Delhi": "North",
    "Jammu And Kashmir": "North",
    "Ladakh": "North",
    "Puducherry": "South",
    "Andaman And Nicobar Islands": "South",
    "Chandigarh": "North",
    "Dadra And Nagar Haveli And Daman And Diu": "West",
    "Lakshadweep": "South" }

#create zone annd location_id columns
dim_location["zone"] = dim_location["state"].map(Zone_map)
dim_location["location_id"] = range(1, len(dim_location)+1)
#create location dimension 
dim_location = dim_location[["location_id", "state", "zone"]]
    
#check for unmapped zones
unmaped_states = dim_location[dim_location["zone"].isna()]["state"].tolist()
if unmaped_states:
    print("Warning: These states do not have zone mapping")
    print(unmaped_states)
    
#save location dimension to output folder
dim_location.to_csv(os.path.join(output_folder, "dim_location.csv"), index=False)
print("Saved dim_location.csv")
#Dimension Crop

#fetch unique crop and season combinations
dim_crop = (df[["crop","season"]].dropna(subset=["crop"]).drop_duplicates()
            .sort_values(["crop","season"]).reset_index(drop=True))
    
#renaming columns
dim_crop = dim_crop.rename(columns={
        "crop": "crop_name",
        "season": "crop_type"
    })
#create columns for crop dimension
dim_crop["crop_id"] = range(1, len(dim_crop)+1)
dim_crop = dim_crop[["crop_id","crop_name", "crop_type"]]
    
#save dimension crop to output folder
dim_crop.to_csv(os.path.join(output_folder, "dim_crop.csv"), index=False)
print("Saved dim_crop.csv")
#Dimension Year
    
#function to assign policy era
def assign_policy_era(year):
    if pd.isna(year):
        return None
    elif year < 2000:
        return "Pre-2000 Era"
    elif year < 2010:
        return "2000's Policy Era"
    elif year < 2020:
        return "2010's Policy Era"
    else:
        return "2020+ Policy Era"
    
# fetch unique years 
dim_year = (df[["crop_year"]].dropna().drop_duplicates()
            .sort_values("crop_year").reset_index(drop=True))
    
#create columns for dimension year
dim_year["crop_year"] = dim_year["crop_year"].astype(int)
dim_year["decade"] = ((dim_year["crop_year"]//10)*10).astype(str)+"s"
dim_year["policy_era"] = dim_year["crop_year"].apply(assign_policy_era)

#rename columns
dim_year= dim_year.rename(columns={"crop_year":"year"})
#creating dimension Year
dim_year = dim_year[["year", "decade", "policy_era"]]
#save Dimension year to output folder
dim_year.to_csv(os.path.join(output_folder, "dim_year.csv"),index=False)
print("Saved dim_year.csv")
#Fact_Production

#merge source and dimensions
fact_production = df.merge(dim_location[["location_id","state"]], on="state",how="left")
fact_production = fact_production.merge(dim_crop, left_on=["crop","season"], right_on = ["crop_name", "crop_type"], how="left")
#calculate yield 
fact_production["yield"] = (fact_production["production"]/fact_production["area"].replace(0, pd.NA))

#rename columns to match fact design
fact_production = fact_production.rename(columns = {
        "crop_year":"year","fertilizer":"fertilizer_qty",
        "pesticide": "pesticide_qty"})
    
#select final columsn for fact 
fact_production = fact_production[["location_id", "crop_id",
                                    "year", "area", "production",
                                    "fertilizer_qty", "pesticide_qty",
                                    "annual_rainfall", "yield"]].copy()
    
# Quality checks
missing_location_keys = fact_production["location_id"].isna().sum()
missing_crop_keys = fact_production["crop_id"].isna().sum()
missing_years = fact_production["year"].isna().sum()

print(f"Missing location_id values: {missing_location_keys}")
print(f"Missing crop_id values: {missing_crop_keys}")
print(f"Missing year values: {missing_years}")

# Save fact
fact_production.to_csv(os.path.join(output_folder, "fact_production.csv"), index=False)
print("Saved fact_production.csv")

    

