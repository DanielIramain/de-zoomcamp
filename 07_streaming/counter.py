import pandas as pd

url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'

df = pd.read_parquet(url)

print(df.info())