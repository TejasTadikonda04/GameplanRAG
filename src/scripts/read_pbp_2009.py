import pandas as pd
from pathlib import Path

parquet_path = Path("data/lake/pbp/season=2009/pbp_2009.parquet")

df = pd.read_parquet(parquet_path)

print(df.shape)
print(df.head(3))
