import numpy as np
import pandas as pd

df= pd.read_csv("C:\\Users\\chowd\\Downloads\\crop_yield.csv")

df.columns = (df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ","_") )

base_df = df[df["crop_year"] == 2020 ].copy() 

def add_noise(value, percent=0.05):
    if pd.isna(value):
        return np.nan
    std = abs(value)*percent
    new_value = np.random.normal(value,std)

    return max(0, new_value)

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

crop_yield_2025 = pd.concat([df, synthetic_df], ignore_index=True)

crop_yield_2025.to_csv("crop_yield_2025.csv",index=False)
