import pandas as pd


df = pd.read_parquet('/home/tur24958/bigdata-lab/data/nyc_yellow_2023-01.parquet')

print("Shape:", df.shape)          # Rows, columns
print("\nColumn Names:\n", df.columns.tolist())
print("\nData Types:\n", df.dtypes)

# Preview first few rows
print("\nFirst 5 Rows:\n", df.head())