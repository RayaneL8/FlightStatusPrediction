import duckdb

duckdb.execute("""
COPY (SELECT * FROM 'C:/Users/danso/Documents/depots/FlightStatusPrediction/backend/data/*.parquet') TO 'C:/Users/danso/Documents/depots/FlightStatusPrediction/backend/data/db.parquet' (FORMAT 'parquet');
""")