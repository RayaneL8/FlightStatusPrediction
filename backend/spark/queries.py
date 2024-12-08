from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt


# Fonction pour obtenir une session Spark
def get_spark_session(app_name="FastAPI_Spark_App"):
    # Vérifie si une session Spark existe déjà, sinon en crée une nouvelle
    if SparkSession._instantiatedSession is not None:
        return SparkSession._instantiatedSession
    
    # Crée une nouvelle session Spark avec une configuration spécifique pour l'exécution locale et les chemins Hadoop sur Windows
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.master", "local") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.hadoop.home.dir", "C:\\Users\\danso\\winutils\\hadoop-3.0.1") \
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:\\Users\\danso\\winutils\\hadoop-3.0.1") \
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:\\Users\\danso\\winutils\\hadoop-3.0.1\\bin") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

# Spécification du chemin du fichier Parquet contenant les données
PARQUET_PATH = "C:/Users/danso/Documents/Bigdata/FlightStatusPrediction/backend/data/db.parquet"        

# Initialisation de Spark
spark = get_spark_session()

# Chargement du fichier Parquet dans un DataFrame Spark
df = spark.read.parquet(PARQUET_PATH)

# Enregistrement du DataFrame comme une table temporaire pour pouvoir effectuer des requêtes SQL
df.createOrReplaceTempView("flights")


# Liste des compagnies aériennes présentes dans les données
def list_airlines():
    query = "SELECT DISTINCT Airline FROM flights"
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols annulés pour une année donnée
def cancelled_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Cancelled_Percentage
        FROM flights 
        WHERE Year = {year} AND Cancelled = 1
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols annulés depuis une certaine date
def cancelled_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Cancelled_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND Cancelled = 1
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols retardés pour une année donnée
def delayed_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Delayed_Percentage
        FROM flights 
        WHERE Year = {year} AND DepDelay > 0
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols retardés depuis une certaine date
def delayed_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Delayed_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND DepDelay > 0
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Top 10 des compagnies aériennes les plus utilisées
def most_used_airlines():
    query = """
        SELECT Airline, COUNT(*) AS TotalFlights
        FROM flights
        GROUP BY Airline
        ORDER BY TotalFlights DESC
        LIMIT 10
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Meilleures compagnies aériennes en termes de performance (moins d'annulations et moins de retard)
def best_performing_airlines():
    query = """
        SELECT Airline, 
               AVG(Cancelled) * 100 AS CancelledRate, 
               AVG(DepDelay) AS AvgDelay 
        FROM flights
        GROUP BY Airline
        ORDER BY CancelledRate ASC, AvgDelay ASC
        LIMIT 10
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Classement des états pour un état spécifique, basé sur le nombre de vols, annulations et retards
def ranking_states(state: str):
    query = f"""
        SELECT OriginState, 
               COUNT(*) AS TotalFlights, 
               SUM(Cancelled) AS CancelledFlights, 
               AVG(DepDelay) AS AvgDelay 
        FROM flights
        WHERE OriginState = '{state}'
        GROUP BY OriginState
        ORDER BY CancelledFlights ASC, AvgDelay ASC
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols déroutés pour une année donnée
def diverted_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Diverted_Percentage
        FROM flights 
        WHERE Year = {year} AND Diverted = 1
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Pourcentage de vols déroutés depuis une certaine date
def diverted_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Diverted_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND Diverted = 1
    """
    return spark.sql(query).toPandas().to_dict(orient="records")


# Calcul des proportions de retards dans différentes catégories (retards courts, moyens, longs)
def delay_proportions_sql():
    query = """
    SELECT 
        CASE 
            WHEN DepDelayMinutes <= 15 THEN 'OnTime_SmallDelay'
            WHEN DepDelayMinutes > 15 AND DepDelayMinutes <= 45 THEN 'MediumDelay'
            WHEN DepDelayMinutes > 45 THEN 'LargeDelay'
            ELSE 'Cancelled'
        END AS DelayCategory,
        COUNT(*) AS Count,
        (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER () AS Proportion
    FROM flights
    GROUP BY DelayCategory
    """
    result = spark.sql(query)
    result.show()
    return result.toPandas().to_dict(orient="records")


# Nombre total de vols par année
def flights_per_year_sql():
    query = """
    SELECT 
        YEAR(FlightDate) AS Year,
        COUNT(*) AS TotalFlights
    FROM flights
    GROUP BY YEAR(FlightDate)
    ORDER BY Year
    """
    result = spark.sql(query)
    result.show()
    return result.toPandas().to_dict(orient="records")


# Résultats des vols par année et catégorie de retard
def flight_results_by_year_sql():
    query = """
    SELECT 
        YEAR(FlightDate) AS Year,
        DelayCategory,
        COUNT(*) AS Count,
        (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY YEAR(FlightDate)) AS Percentage
    FROM (
        SELECT 
            *,
            CASE 
                WHEN DepDelayMinutes <= 15 THEN 'OnTime_SmallDelay'
                WHEN DepDelayMinutes > 15 AND DepDelayMinutes <= 45 THEN 'MediumDelay'
                WHEN DepDelayMinutes > 45 THEN 'LargeDelay'
                ELSE 'Cancelled'
            END AS DelayCategory
        FROM flights
    )
    GROUP BY YEAR(FlightDate), DelayCategory
    ORDER BY Year, DelayCategory
    """
    result = spark.sql(query)
    result.show()
    return result.toPandas().to_dict(orient="records")


# Résultats des vols par mois et catégorie de retard
def flight_results_by_month_sql():
    query = """
    SELECT 
        MONTH(FlightDate) AS Month,
        DelayCategory,
        COUNT(*) AS Count,
        (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER (PARTITION BY MONTH(FlightDate)) AS Percentage
    FROM (
        SELECT 
            *,
            CASE 
                WHEN DepDelayMinutes <= 15 THEN 'OnTime_SmallDelay'
                WHEN DepDelayMinutes > 15 AND DepDelayMinutes <= 45 THEN 'MediumDelay'
                WHEN DepDelayMinutes > 45 THEN 'LargeDelay'
                ELSE 'Cancelled'
            END AS DelayCategory
        FROM flights
    )
    GROUP BY MONTH(FlightDate), DelayCategory
    ORDER BY Month, DelayCategory
    """
    result = spark.sql(query)
    result.show()
    return result.toPandas().to_dict(orient="records")


# Pourcentage de vols annulés par mois et année
def cancelled_flights_calendar_sql():
    query = """
    SELECT 
        YEAR(FlightDate) AS Year,
        MONTH(FlightDate) AS Month,
        COUNT(CASE WHEN Cancelled = TRUE THEN 1 END) AS CancelledCount,
        COUNT(*) AS TotalFlights,
        (COUNT(CASE WHEN Cancelled = TRUE THEN 1 END) * 100.0) / COUNT(*) AS CancelledPercentage
    FROM flights
    GROUP BY YEAR(FlightDate), MONTH(FlightDate)
    ORDER BY Year, Month
    """
    result = spark.sql(query)
    result.show()
    return result.toPandas().to_dict(orient="records")







def compare_airlines_sql():
    # Most Delays
    most_delays_query = """
    SELECT 
        Airline,
        COUNT(*) AS DelayCount
    FROM flights
    WHERE DepDelayMinutes > 15
    GROUP BY Airline
    """
    most_delays = spark.sql(most_delays_query)

    # Most Cancellations
    most_cancellations_query = """
    SELECT 
        Airline,
        COUNT(*) AS CancelledCount
    FROM flights
    WHERE Cancelled = TRUE
    GROUP BY Airline
    """
    most_cancellations = spark.sql(most_cancellations_query)

    # Most Reliable
    most_reliable_query = """
    SELECT 
        Airline,
        COUNT(*) AS OnTimeCount
    FROM flights
    WHERE DepDelayMinutes <= 15
    GROUP BY Airline
    """
    most_reliable = spark.sql(most_reliable_query)

    # Joindre les trois résultats sur l'Airline
    airlines_comparison = most_delays.join(most_cancellations, "Airline", "outer") \
                                      .join(most_reliable, "Airline", "outer")

    # Remplir les valeurs manquantes avec 0
    airlines_comparison = airlines_comparison.fillna({"DelayCount": 0, "CancelledCount": 0, "OnTimeCount": 0})

    # Créer un score global basé sur les trois critères (plus c'est élevé, plus la compagnie est moins performante)
    airlines_comparison = airlines_comparison.withColumn(
        "GlobalScore",
        (airlines_comparison["DelayCount"] + airlines_comparison["CancelledCount"] - airlines_comparison["OnTimeCount"])
    )

    # Convertir en Pandas pour préparer la heatmap
    pandas_df = airlines_comparison.toPandas()

    # # Recréer un DataFrame pour la heatmap avec les trois critères
    # heatmap_data = pandas_df[["Airline", "DelayCount", "CancelledCount", "OnTimeCount"]]
    # heatmap_data.set_index("Airline", inplace=True)

    # # Générer la heatmap
    # plt.figure(figsize=(10, 8))
    # sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu", cbar=True)
    # plt.title("Comparaison des compagnies aériennes (Retards, Annulations, Fiabilité)")
    # plt.show()

    return pandas_df.to_dict(orient="records")


