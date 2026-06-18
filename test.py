import pandas as pd
import glob

files = glob.glob('data/silver/record_type=wearable_event/**/*.parquet', recursive=True)
print("files found:", len(files))

if not files:
    print("No files matched — check the path / run from project root")
else:
    df = pd.concat([pd.read_parquet(f) for f in files])
    print("total rows:", len(df))
    print(df['failure_reason'].value_counts(dropna=False))
    print("columns:", df.columns.tolist())