import pandas as pd
import numpy as np
df=pd.read_csv('Sales spreadsheet.csv')
date_range= pd.date_range(start='2021-01-01',end='2021-12-31',freq='D')
num_rows=len(date_range)
last_id=df['ID'].max()
new_data={
    'ID': range(last_id+1,last_id + 1+num_rows),
    'Order Date':date_range.strftime('%m/%d/%y'),
    'Product Name':np.random.choice(['Furnishings-Saepe','Chairs-Recusandae','Chairs-Nam','Envelopes-Necessitatibus','Art-Sunt'],num_rows),
    'Sales': np.round(np.random.uniform(50.0, 1000.0, size=num_rows), 2),
    'Quantity': np.random.randint(1, 10, size=num_rows),
    'Year': 2021
}
df_2021=pd.DataFrame(new_data)
Final_df=pd.concat([df,df_2021], ignore_index=True)
Final_df.to_csv('sales_data_updated.csv', index=False)
print("Rows added successfully")
print(Final_df.tail())