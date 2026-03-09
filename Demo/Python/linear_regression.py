import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime
df = pd.read_csv('Sales spreadsheet.csv')
df['Order Date']=pd.to_datetime(df['Order Date'])
# we need to convert dates to ordinal(numbers) to perform the calculation
df['Date_Ordinal']=df['Order Date'].map(datetime.toordinal)
X = df[['Date_Ordinal']] # Independent variable (Time)
Y = df['Sales']          # Dependent variable (Sales)
#Training the model
model=LinearRegression()
model.fit(X,Y)
#we need to generate the required years data
date_range_2021 = pd.date_range(start='2021-01-01', end='2021-12-31', freq='D')
df_2021 = pd.DataFrame({
    'Order Date': date_range_2021,
    'Date_Ordinal': date_range_2021.map(datetime.toordinal)
    })
predictions = model.predict(df_2021[['Date_Ordinal']]) #predicting the 2021 data usng linear regression
# Add noise so it looks like real retail data 
noise = np.random.normal(0, df['Sales'].std() * 0.1, size=len(predictions))
#The above part is optional to make data look like real data

df_2021['Sales'] = np.round(predictions + noise, 2)
last_id = df['ID'].max()
df_2021['ID'] = range(last_id + 1, last_id + 1 + len(df_2021))
df_2021['Product Name'] =np.random.choice(['Furnishings-Saepe','Chairs-Recusandae','Chairs-Nam','Envelopes-Necessitatibus','Art-Sunt'],size=len(df_2021))
df_2021['Quantity'] = np.random.randint(1, 10, size=len(df_2021))
df_2021['Year'] = 2021
#cLeaning and appending
df_2021 = df_2021[['ID', 'Order Date', 'Product Name', 'Sales', 'Quantity', 'Year']]
df_2021['Order Date'] = df_2021['Order Date'].dt.strftime('%m/%d/%Y')

final_df = pd.concat([df, df_2021.drop(columns=['Date_Ordinal'], errors='ignore')], ignore_index=True)
final_df.to_csv('sales_with_regression_2021.csv', index=False)
print("2021 data generated successfully using Linear Regressionin csv!")