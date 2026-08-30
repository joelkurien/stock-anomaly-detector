import duckdb

conn = duckdb.connect("processing/local.duckdb")

conn.execute("""
    CREATE OR REPLACE TABLE min_data AS 
    SELECT * FROM read_parquet('stock_data/minute_streaming/**/*.parquet', union_by_name=True)
""")

df = conn.execute("SELECT * FROM min_data").fetchdf()

print(df.iloc[10])
