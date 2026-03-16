import pandas as pd
import numpy as np
file_path=r'C:\Users\bolli\Documents\sow\Projects\Repo\AgriAnalytics\crop_yield_2025.csv'
df=pd.read_csv(file_path)
#print(df.head())
#Fill missing Production where Area and Yield exist
mask_prod = df['production'].isna() & df['area'].notna() & df['yield'].notna()
df.loc[mask_prod, 'production'] = df.loc[mask_prod, 'area'] * df.loc[mask_prod, 'yield']
print("taskdone")
# 2. Fill missing Yield where Production and Area exist
# We add a check for Area > 0 to avoid DivisionByZero errors
mask_yield = df['yield'].isna() & df['production'].notna() & (df['area'] > 0)
df.loc[mask_yield, 'yield'] = df.loc[mask_yield, 'production'] / df.loc[mask_yield, 'area']
print("task2done")
# 3. Handling the 'Zero vs NaN' dilemma
# Flag or replace suspicious zeros in Area where Production is high
# For example, if Area is 0 but Production > 0, set Area to NaN to mark it for review
df.loc[(df['area'] == 0) & (df['production'] > 0), 'area'] = np.nan
print("Data cleaning complete.")
print("--- Missing Values Count ---")
print(df.isnull().sum())
# Define the new file path
output_file = r'C:\Users\bolli\Documents\sow\Projects\Repo\AgriAnalytics\crop_yield_2025_cleaned.csv'

# Save to CSV (index=False prevents an extra "Unnamed: 0" column)
df.to_csv(output_file, index=False)

print(f"File successfully saved to: {output_file}")