from pyspark.sql import SparkSession
import os

# Créer une session Spark
def get_spark_session(app_name="FastAPI_Spark_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.master", "local") \
        .getOrCreate()

# Générer des données de test et les sauvegarder en Parquet
def generate_parquet_data(output_path="app/data/data.parquet"):
    spark = get_spark_session()
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35},
    ]
    df = spark.createDataFrame(data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

# Charger un fichier Parquet et exécuter une requête
def query_parquet_data(query, parquet_path="app/data/data.parquet"):
    spark = get_spark_session()
    df = spark.read.parquet(parquet_path)
    df.createOrReplaceTempView("data")
    return spark.sql(query).toPandas()
