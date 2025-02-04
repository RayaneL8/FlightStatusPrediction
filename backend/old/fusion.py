import duckdb

duckdb.execute("""
COPY (SELECT * FROM './backend/data/*.parquet') TO './backend/data/db.parquet' (FORMAT 'parquet');
""")