import pandas as pd
from services import preset_requests as Spreset_requests
from itertools import product

def extract_performance_metrics(data):
    avg_departure_delay = 0
    avg_arrival_delay = 0
    cancelled_percentage = 0
    diverted_percentage = 0

    # Gestion des valeurs reçues
    if isinstance(data.get("avg_departure_delay"), list) and len(data["avg_departure_delay"]) > 0:
        avg_departure_delay = data["avg_departure_delay"][0].get("Avg_Departure_Delay", 0)

    if isinstance(data.get("avg_arrival_delay"), list) and len(data["avg_arrival_delay"]) > 0:
        avg_arrival_delay = data["avg_arrival_delay"][0].get("Avg_Arrival_Delay", 0)

    if isinstance(data.get("cancelled_percentage"), list) and len(data["cancelled_percentage"]) > 0:
        cancelled_percentage = data["cancelled_percentage"][0].get("Cancelled_Percentage", 0)

    if isinstance(data.get("diverted_percentage"), list) and len(data["diverted_percentage"]) > 0:
        diverted_percentage = data["diverted_percentage"][0].get("Diverted_Percentage", 0)

    # Retour des métriques structurées
    return {
        "Avg_Departure_Delay": avg_departure_delay,
        "Avg_Arrival_Delay": avg_arrival_delay,
        "Cancelled_Percentage": cancelled_percentage,
        "Diverted_Percentage": diverted_percentage,
    }


def get_performance(years: list[int], cities: list[str], airlines: list[str]):
    aggregated_metrics = {airline: {"Avg_Departure_Delay": 0, "Avg_Arrival_Delay": 0, "Cancelled_Percentage": 0, "Diverted_Percentage": 0, "count": 0} for airline in airlines}
    
    data = Spreset_requests.get_performance_multi(years=years, cities=cities, airlines=airlines)
                
    try:
        for d in data:
            print(data)
            if d:
                metrics = extract_performance_metrics(data=d)
                aggregated_metrics[airline]["Avg_Departure_Delay"] += metrics["Avg_Departure_Delay"]
                aggregated_metrics[airline]["Avg_Arrival_Delay"] += metrics["Avg_Arrival_Delay"]
                aggregated_metrics[airline]["Cancelled_Percentage"] += metrics["Cancelled_Percentage"]
                aggregated_metrics[airline]["Diverted_Percentage"] += metrics["Diverted_Percentage"]
                aggregated_metrics[airline]["count"] += 1
                
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

    # Calculer les moyennes
    for airline in aggregated_metrics:
        count = aggregated_metrics[airline]["count"]
        if count > 0:
            aggregated_metrics[airline]["Avg_Departure_Delay"] /= count
            aggregated_metrics[airline]["Avg_Arrival_Delay"] /= count
            aggregated_metrics[airline]["Cancelled_Percentage"] /= count
            aggregated_metrics[airline]["Diverted_Percentage"] /= count

    return aggregated_metrics

#ANIMATED
def fetch_performance_metrics(year, cities, airlines):
    """
    Récupère les métriques pour une année donnée, une liste de villes et une liste de compagnies aériennes.
    """
    aggregated_metrics = get_performance(year, cities, airlines)
    return aggregated_metrics


def transform_to_dataframe(start_year, end_year, cities, airlines):
    """
    Transforme les données récupérées en un DataFrame structuré pour Plotly.
    """
    all_data = []

    for year in range(start_year, end_year + 1):
        aggregated_metrics = fetch_performance_metrics(year, cities, airlines)
        
        for airline, metrics in aggregated_metrics.items():
            all_data.append({
                "Airline": airline,
                "Year": year,
                "Parameter": "Avg_Departure_Delay",
                "Value": metrics["Avg_Departure_Delay"] or 0  # Valeur par défaut
            })
            all_data.append({
                "Airline": airline,
                "Year": year,
                "Parameter": "Avg_Arrival_Delay",
                "Value": metrics["Avg_Arrival_Delay"] or 0
            })
            all_data.append({
                "Airline": airline,
                "Year": year,
                "Parameter": "Cancelled_Percentage",
                "Value": metrics["Cancelled_Percentage"] or 0  # Ne pas multiplier par 100
            })
            all_data.append({
                "Airline": airline,
                "Year": year,
                "Parameter": "Diverted_Percentage",
                "Value": metrics["Diverted_Percentage"] or 0  # Ne pas multiplier par 100
            })

    # Conversion en DataFrame
    df = pd.DataFrame(all_data)
    df["Parameter"] = df["Parameter"].astype("category")  # Assurez-vous que Parameter est une catégorie

    # Vérification et correction des valeurs aberrantes
    df["Value"] = df["Value"].clip(upper=100)  # Limiter les valeurs à 100%
    print("Données après correction des anomalies :")
    print(df.head())

    return df