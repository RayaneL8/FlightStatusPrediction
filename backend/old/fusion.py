import duckdb

duckdb.execute("""
COPY (SELECT * FROM './data/*.parquet') TO 'db.parquet' (FORMAT 'parquet');
""")