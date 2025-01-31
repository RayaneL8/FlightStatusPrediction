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
    """
    Récupère et organise les données de performances par années, villes et compagnies aériennes.
    """
    # Récupération brute des données via l'API
    raw_data = Spreset_requests.get_performance_multi(years=years, cities=cities, airlines=airlines)
    if not raw_data:
        print("Aucune donnée récupérée.")
        return {}

    # Organisation des données
    organized_metrics = transform_to_dataframe(raw_data)
    return organized_metrics

def transform_to_dataframe(data):
    """
    Transforme les données brutes de l'API en un DataFrame structuré pour Plotly, avec normalisation des valeurs.
    """
    # Vérifier si les données brutes sont valides
    if data is None or not isinstance(data, dict) or len(data) == 0:
        print("Aucune donnée reçue ou données invalides.")
        return pd.DataFrame()

    # Initialisation de la liste pour stocker les données formatées
    all_data = []
    categories = ["avg_departure_delay", "avg_arrival_delay", "cancelled_percentage", "diverted_percentage"]

    for category in categories:
        if category in data:  # Vérifier que la clé existe dans les données
            for entry in data[category]:
                airline = entry.get("Airline", "Unknown")
                year = entry.get("Year", "Unknown")

                # Récupérer la valeur pour la catégorie actuelle
                value = entry.get(category.replace("_", " ").title().replace(" ", "_"), 0)

                # Ajouter une ligne pour chaque métrique
                all_data.append({
                    "Airline": airline,
                    "Year": year,
                    "Parameter": category.replace("_", " ").title().replace(" ", "_"),
                    "Value": value,
                })

    # Vérifier si aucune donnée n'a été collectée
    if not all_data:
        print("Erreur : aucune donnée formatée pour le DataFrame.")
        return pd.DataFrame()

    # Création du DataFrame
    df = pd.DataFrame(all_data)

    # Vérifier explicitement si le DataFrame est vide
    if df.empty:
        print("Erreur : le DataFrame final est vide.")
        return pd.DataFrame()

    # Appliquer les transformations finales
    df["Parameter"] = df["Parameter"].astype("category")

    # Normaliser les valeurs sur une base de 100
    max_value = df["Value"].max()
    if max_value > 0:  # Éviter une division par zéro
        df["Value"] = (df["Value"] / max_value) * 100

    print("Aperçu des données transformées et normalisées :")
    print(df.head())

    return df



#ANIMATED
def fetch_performance_metrics(year, cities, airlines):
    """
    Récupère les métriques pour une année donnée, une liste de villes et une liste de compagnies aériennes.
    """
    aggregated_metrics = get_performance(year, cities, airlines)
    return aggregated_metrics


