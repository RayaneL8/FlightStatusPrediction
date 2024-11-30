from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession

app = FastAPI()

# Chemin du fichier Parquet
PARQUET_PATH = "./data/data.parquet"

# Fonction pour initialiser une session Spark
def get_spark_session(app_name="FastAPI_Spark_App"):
    if SparkSession._instantiatedSession is not None:
        return SparkSession._instantiatedSession
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.master", "local") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.hadoop.home.dir", "C:\\hadoop") \
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:\\hadoop\\bin") \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:\\hadoop\\bin") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()



# Fonction utilitaire pour charger les données
def load_parquet():
    spark = get_spark_session()
    return spark.read.parquet(PARQUET_PATH)

# Endpoint 1 : Récupérer toutes les données
@app.get("/all/")
async def get_all_data():
    try:
        df = load_parquet()
        result = df.toPandas()
        return {"data": result.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint 2 : Filtrer les personnes de plus de 30 ans
@app.get("/filter/age-over-30/")
async def filter_age_over_30():
    try:
        df = load_parquet()
        filtered_df = df.filter(df.age > 30)
        result = filtered_df.toPandas()
        return {"data": result.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint 3 : Trier les personnes par âge décroissant
@app.get("/sort/by-age-desc/")
async def sort_by_age_desc():
    try:
        df = load_parquet()
        sorted_df = df.orderBy(df.age.desc())
        result = sorted_df.toPandas()
        return {"data": result.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint 4 : Calculer l'âge moyen
@app.get("/aggregate/average-age/")
async def calculate_average_age():
    try:
        df = load_parquet()
        avg_age = df.groupBy().avg("age").collect()[0][0]
        return {"average_age": avg_age}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint 5 : Récupérer les noms des personnes
@app.get("/names/")
async def get_names():
    try:
        df = load_parquet()
        names = df.select("name").toPandas()
        return {"names": names["name"].tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
