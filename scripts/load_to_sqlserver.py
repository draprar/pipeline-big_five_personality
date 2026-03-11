import pandas as pd
from sqlalchemy import create_engine
import time

print('Loading CSV...')
df = pd.read_csv('data/raw/data-final.csv', sep='\t', low_memory=False)
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

engine = create_engine(
    "mssql+pyodbc://WALERY\\SQLEXPRESS/BigFiveDB"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

print('Loading into SQL Server...')
start = time.time()
df.to_sql(
    'big_five_raw',
    engine,
    if_exists='replace',
    index=False,
    chunksize=5000
)
print(f'Took {time.time()-start} seconds')