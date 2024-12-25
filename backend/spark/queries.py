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
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.cores", "4") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .master("local[4]").getOrCreate()

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
    try :
        query = "SELECT DISTINCT Airline FROM flights"
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}


# Pourcentage de vols annulés pour une année donnée
def cancelled_flights_percentage_year(year: int):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Cancelled_Percentage
            FROM flights 
            WHERE Year = {year} AND Cancelled = 1
        """
    
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}


# Pourcentage de vols annulés depuis une certaine date
def cancelled_flights_percentage_since(date: str):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Cancelled_Percentage
            FROM flights 
            WHERE FlightDate >= '{date}' AND Cancelled = 1
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols retardés pour une année donnée
def delayed_flights_percentage_year(year: int):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Delayed_Percentage
            FROM flights 
            WHERE Year = {year} AND DepDelay > 0
        """
    
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols retardés depuis une certaine date
def delayed_flights_percentage_since(date: str):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Delayed_Percentage
            FROM flights 
            WHERE FlightDate >= '{date}' AND DepDelay > 0
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Top 10 des compagnies aériennes les plus utilisées
def most_used_airlines():
    try:
        query = """
            SELECT Airline, COUNT(*) AS TotalFlights
            FROM flights
            GROUP BY Airline
            ORDER BY TotalFlights DESC
            LIMIT 10
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Meilleures compagnies aériennes en termes de performance (moins d'annulations et moins de retard)
def best_performing_airlines():
    try:
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
    except Exception as e:
        return {"error": str(e)}

# Classement des états pour un état spécifique, basé sur le nombre de vols, annulations et retards
def ranking_states(state: str):
    try:
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
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols déroutés pour une année donnée
def diverted_flights_percentage_year(year: int):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Diverted_Percentage
            FROM flights 
            WHERE Year = {year} AND Diverted = 1
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols déroutés depuis une certaine date
def diverted_flights_percentage_since(date: str):
    try:
        query = f"""
            SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Diverted_Percentage
            FROM flights 
            WHERE FlightDate >= '{date}' AND Diverted = 1
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Calcul des proportions de retards dans différentes catégories (retards courts, moyens, longs)
def delay_proportions_sql():
    try:
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
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Nombre total de vols par année
def flights_per_year_sql():
    try:
        query = """
        SELECT 
            YEAR(FlightDate) AS Year,
            COUNT(*) AS TotalFlights
        FROM flights
        GROUP BY YEAR(FlightDate)
        ORDER BY Year
        """
        result = spark.sql(query)
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Résultats des vols par année et catégorie de retard
def flight_results_by_year_sql():
    try:
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
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Résultats des vols par mois et catégorie de retard
def flight_results_by_month_sql():
    try:
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
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols annulés par mois et année
def cancelled_flights_calendar(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            YEAR(FlightDate) AS Year,
            MONTH(FlightDate) AS Month,
            COUNT(CASE WHEN Cancelled = TRUE THEN 1 END) AS CancelledCount,
            COUNT(*) AS TotalFlights,
            (COUNT(CASE WHEN Cancelled = TRUE THEN 1 END) * 100.0) / COUNT(*) AS CancelledPercentage
        FROM flights
        WHERE {where_clause}
        GROUP BY YEAR(FlightDate), MONTH(FlightDate)
        ORDER BY Year, Month
        """
        result = spark.sql(query)
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def diverted_flights_calendar(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            YEAR(FlightDate) AS Year,
            MONTH(FlightDate) AS Month,
            COUNT(CASE WHEN Diverted = TRUE THEN 1 END) AS DivertedCount,
            COUNT(*) AS TotalFlights,
            (COUNT(CASE WHEN Diverted = TRUE THEN 1 END) * 100.0) / COUNT(*) AS DivertedPercentage
        FROM flights
        WHERE {where_clause}
        GROUP BY YEAR(FlightDate), MONTH(FlightDate)
        ORDER BY Year, Month
        """
        result = spark.sql(query)
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def delay_calendar(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT 
            YEAR(FlightDate) AS Year,
            MONTH(FlightDate) AS Month,
            COUNT(CASE WHEN DepDelayMinutes > 0  THEN 1 END) AS DelayedCount,
            COUNT(*) AS TotalFlights,
            (COUNT(CASE WHEN DepDelayMinutes > 0 THEN 1 END) * 100.0) / COUNT(*) AS DelayedPercentage
        FROM flights
        WHERE {where_clause}
        GROUP BY YEAR(FlightDate), MONTH(FlightDate)
        ORDER BY Year, Month
        """
        result = spark.sql(query)
        return result.toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Moyenne des retards au départ
def avg_departure_delay(year=None, city=None, airline=None):
    try:
        conditions = ["Cancelled = 0", "Diverted = 0"]
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT Year, OriginCityName, Airline, AVG(DepDelayMinutes) AS Avg_Departure_Delay
            FROM flights
            WHERE {where_clause}
            GROUP BY Year, OriginCityName, Airline
            ORDER BY Year, Avg_Departure_Delay DESC
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Moyenne des retards à l'arrivée
def avg_arrival_delay(year=None, city=None, airline=None):
    try:
        conditions = ["Cancelled = 0", "Diverted = 0"]
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"DestCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT Year, DestCityName, Airline, AVG(ArrDelayMinutes) AS Avg_Arrival_Delay
            FROM flights
            WHERE {where_clause}
            GROUP BY Year, DestCityName, Airline
            ORDER BY Year, Avg_Arrival_Delay DESC
        """
   
        result_df = spark.sql(query).toPandas()

        # Nettoyage des données pour éliminer les valeurs problématiques
        result_df.replace([float('nan'), float('inf'), -float('inf')], None, inplace=True)

        # Conversion en JSON
        return result_df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols annulés
def cancelled_percentage(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT Year, OriginCityName, Airline,
                COUNT(*) AS Total_Flights,
                SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS Cancelled_Percentage
            FROM flights
            WHERE {where_clause}
            GROUP BY Year, OriginCityName, Airline
            ORDER BY Cancelled_Percentage DESC
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Pourcentage de vols déroutés
def diverted_percentage(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT Year, OriginCityName, Airline,
                COUNT(*) AS Total_Flights,
                SUM(CASE WHEN Diverted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS Diverted_Percentage
            FROM flights
            WHERE {where_clause}
            GROUP BY Year, OriginCityName, Airline
            ORDER BY Diverted_Percentage DESC
        """
    
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Retards moyens par carte des États-Unis (état et ville)
def us_map_delay_cancellations():
    try:
        query = """
            SELECT OriginStateName, OriginCityName,
                AVG(DepDelayMinutes) AS Avg_Departure_Delay,
                SUM(CASE WHEN Cancelled = 1 THEN 1 ELSE 0 END) AS Total_Cancellations
            FROM flights
            GROUP BY OriginStateName, OriginCityName
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Nombre total de vols
def total_flights(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT Year, OriginCityName, Airline, COUNT(*) AS Total_Flights
            FROM flights
            WHERE {where_clause}
            GROUP BY Year, OriginCityName, Airline
            ORDER BY Total_Flights DESC
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}



def avg_distance(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, AVG(Distance) AS Avg_Distance
        FROM flights
        WHERE {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Avg_Distance DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def flight_distribution_by_airline():
    try:
        query= """
        SELECT Airline, COUNT(*) AS Total_Flights
        FROM flights
        GROUP BY Airline
        ORDER BY Total_Flights DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}




def avg_flight_time(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, AVG(ActualElapsedTime) AS Avg_Flight_Time
        FROM flights
        WHERE {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Avg_Flight_Time DESC;
        """
   
        result_df = spark.sql(query).toPandas()

        # Nettoyage des données pour éliminer les valeurs problématiques
        result_df.replace([float('nan'), float('inf'), -float('inf')], None, inplace=True)

        # Conversion en JSON
        return result_df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}

def cancelled_calendar():
    try:
        query= """
        SELECT FlightDate, Airline, OriginCityName, DestCityName
        FROM flights
        WHERE Cancelled = 1
        ORDER BY FlightDate ASC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}


def avg_taxi_out(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, AVG(TaxiOut) AS Avg_TaxiOut_Time
        FROM flights
        WHERE {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Avg_TaxiOut_Time DESC;
        """
   
        result_df = spark.sql(query).toPandas()

        # Nettoyage des données pour éliminer les valeurs problématiques
        result_df.replace([float('nan'), float('inf'), -float('inf')], None, inplace=True)

        # Conversion en JSON
        return result_df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def avg_taxi_in(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, DestCityName, Airline, AVG(TaxiIn) AS Avg_TaxiIn_Time
        FROM flights
        WHERE {where_clause}
        GROUP BY Year, DestCityName, Airline
        ORDER BY Avg_TaxiIn_Time DESC;
        """
   
        result_df = spark.sql(query).toPandas()

        # Nettoyage des données pour éliminer les valeurs problématiques
        result_df.replace([float('nan'), float('inf'), -float('inf')], None, inplace=True)

        # Conversion en JSON
        return result_df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def flights_delayed_15_plus(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, COUNT(*) AS Flights_Delayed_15_Plus
        FROM flights
        WHERE DepDel15 = 1
        AND {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Flights_Delayed_15_Plus DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def flights_delayed_less_15(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, COUNT(*) AS Flights_Delayed_Less_15
        FROM flights
        WHERE DepDel15 = 0 AND DepDelayMinutes > 0
        AND {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Flights_Delayed_Less_15 DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def diverted_flights(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, COUNT(*) AS Diverted_Flights
        FROM flights
        WHERE Diverted = 1
        AND {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Diverted_Flights DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def cancelled_flights(year=None, city=None, airline=None):
    try:
        conditions = []
        if year:
            conditions.append(f"Year = {year}")
        if city:
            conditions.append(f"OriginCityName = '{city}'")
        if airline:
            conditions.append(f"Airline = '{airline}'")
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query= f"""
        SELECT Year, OriginCityName, Airline, COUNT(*) AS Cancelled_Flights
        FROM flights
        WHERE Cancelled = 1
        AND {where_clause}
        GROUP BY Year, OriginCityName, Airline
        ORDER BY Cancelled_Flights DESC;
        """
   
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def execute_raw_query(query):
    try:
        return spark.sql(query).toPandas().to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
