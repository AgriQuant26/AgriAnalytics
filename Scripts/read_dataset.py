import kagglehub
import os
import pandas as pd

# Download latest version
path = kagglehub.dataset_download("akshatgupta7/crop-yield-in-indian-states-dataset")

print("Path to dataset files:", path)

# List all files in the downloaded path
files = os.listdir(path)
print(files)

filename = "crop_yield.csv" 

# 2. Create the full path to the file
full_path = os.path.join(path, filename)

# 3. Read it into a DataFrame
df = pd.read_csv(full_path)

# 4. Success! Check the data
print(df.head())

# 5. Save the data in csv format
output_folder = "AgriAnalytics/Data/raw"
df.to_csv(os.path.join(output_folder, "crop_yield_data.csv"), index=False)