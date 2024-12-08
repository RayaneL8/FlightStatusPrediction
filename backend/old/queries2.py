import duckdb
import os

# Charger les données CSV avec DuckDB
file_path = r"C:/Users/danso/Documents/Bigdata/FlightStatusPrediction/backend/data/Combined_Flights_2020.csv"

# Créer une connexion à DuckDB
conn = duckdb.connect()

# Charger le fichier CSV dans une table temporaire
conn.execute(f"CREATE TABLE flights AS SELECT * FROM read_csv_auto('{file_path}')")

# Fonction pour lister les compagnies aériennes
def list_airlines():
    query = "SELECT DISTINCT Airline FROM flights"
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols annulés par année
def cancelled_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Cancelled_Percentage
        FROM flights 
        WHERE Year = {year} AND Cancelled = 1
    """
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols annulés depuis une date
def cancelled_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Cancelled_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND Cancelled = 1
    """
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols retardés par année
def delayed_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Delayed_Percentage
        FROM flights 
        WHERE Year = {year} AND DepDelay > 0
    """
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols retardés depuis une date
def delayed_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Delayed_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND DepDelay > 0
    """
    return conn.execute(query).fetchall()

# Fonction pour obtenir les 10 compagnies aériennes les plus utilisées
def most_used_airlines():
    query = """
        SELECT Airline, COUNT(*) AS TotalFlights
        FROM flights
        GROUP BY Airline
        ORDER BY TotalFlights DESC
        LIMIT 10
    """
    return conn.execute(query).fetchall()

# Fonction pour obtenir les 10 compagnies aériennes les mieux performantes
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
    return conn.execute(query).fetchall()

# Fonction pour obtenir le classement des états par performances (vols annulés et retardés)
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
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols détournés par année
def diverted_flights_percentage_year(year: int):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE Year = {year}) AS Diverted_Percentage
        FROM flights 
        WHERE Year = {year} AND Diverted = 1
    """
    return conn.execute(query).fetchall()

# Fonction pour calculer le pourcentage de vols détournés depuis une date
def diverted_flights_percentage_since(date: str):
    query = f"""
        SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM flights WHERE FlightDate >= '{date}') AS Diverted_Percentage
        FROM flights 
        WHERE FlightDate >= '{date}' AND Diverted = 1
    """
    return conn.execute(query).fetchall()

# Exemple d'utilisation
# Vous pouvez maintenant appeler ces fonctions pour obtenir les résultats.
# Par exemple, obtenir les 10 compagnies aériennes les plus utilisées :
# print(most_used_airlines())
