import pandas as pd

# Read in the csv file it is encoded in latin1
data = pd.read_csv('Online_Retail.csv', sep = ',',encoding='latin1')

# Drop all rows with missing values
data = data.dropna()

# Drop all rows where the InvoiceNo is not a number
data = data[data['InvoiceNo'].apply(lambda x: str(x).isdigit())]

# Create a new DataFrame with limited amount of rows
data_limited = data.head(1000)

# Save the cleaned data to a new csv file
data.to_csv('Online_Retail_Cleaned.csv', index = False, encoding='utf-8', sep=',')
data_limited.to_csv('Online_Retail_Cleaned_1000rows.csv', index = False, encoding='utf-8', sep=',')