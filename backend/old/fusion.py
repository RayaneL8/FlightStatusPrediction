import duckdb

duckdb.execute("""
COPY (SELECT * FROM './../data/*.parquet') TO './../data/db.parquet' (FORMAT 'parquet');
""")