import os
import numpy as np
import pandas as pd

#load original data
df= pd.read_csv("AgriAnalytics\\Data\\raw\\crop_yield_data.csv")

#standarise column names
df.columns = (df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ","_") )

#base data for synthetic years
base_df = df[df["crop_year"] == 2020 ].copy() 

#function to add random noise
def add_noise(value, percent=0.05):
    if pd.isna(value):
        return np.nan
    std = abs(value)*percent
    new_value = np.random.normal(value,std)

    return max(0, new_value)

#generate synthetic data for 2021 - 2025
synthetic_rows = []

for year in range (2021, 2026):
    trend = 1+(0.01*(year-2020))
    for _, row in base_df.iterrows():
        new_row = row.copy()
        new_row["crop_year"]= year
        new_row["area"] = add_noise(row["area"],0.04)

        new_row["annual_rainfall"] = add_noise(row["annual_rainfall"]*trend, 0.06)
        new_row["fertilizer"] = add_noise(row["fertilizer"]*trend, 0.05)
        new_row["pesticide"] = add_noise(row["pesticide"]*trend, 0.05)
        new_row["yield"] = add_noise(row["yield"], 0.05)
        new_row["production"] = new_row["area"]*new_row["yield"]

        synthetic_rows.append(new_row)

synthetic_df = pd.DataFrame(synthetic_rows)

#combine original and synthetic data
crop_yield_1997_2025 = pd.concat([df, synthetic_df], ignore_index=True)

#create output folders
output_folder = "C:\\Users\\chowd\\VS_Projects\\AgriAnalytics\\Data"
yearly_folder = os.path.join(output_folder, "yearly_csv")

os.makedirs(output_folder, exist_ok=True)
os.makedirs(yearly_folder, exist_ok=True) 

#save full dataset
crop_yield_1997_2025.to_csv(os.path.join(output_folder, "crop_yield_1997_2025.csv"), index=False)

#save each year as sepearte csv
years = sorted(crop_yield_1997_2025["crop_year"].unique())

for year in years:
    year_df = crop_yield_1997_2025[crop_yield_1997_2025["crop_year"]== year]
    year_csv_path= os.path.join(yearly_folder, f"crop_yield_{year}.csv")
    year_df.to_csv(year_csv_path, index=False)
    

