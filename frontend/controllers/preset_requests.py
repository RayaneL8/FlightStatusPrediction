import os
import pandas as pd
from services import preset_requests as Spreset_requests
from itertools import product
from geopy.geocoders import Nominatim
import time


# def extract_performance_metrics(data):
#     avg_departure_delay = 0
#     avg_arrival_delay = 0
#     cancelled_percentage = 0
#     diverted_percentage = 0

#     # Gestion des valeurs reçues
#     if isinstance(data.get("avg_departure_delay"), list) and len(data["avg_departure_delay"]) > 0:
#         avg_departure_delay = data["avg_departure_delay"][0].get("Avg_Departure_Delay", 0)

#     if isinstance(data.get("avg_arrival_delay"), list) and len(data["avg_arrival_delay"]) > 0:
#         avg_arrival_delay = data["avg_arrival_delay"][0].get("Avg_Arrival_Delay", 0)

#     if isinstance(data.get("cancelled_percentage"), list) and len(data["cancelled_percentage"]) > 0:
#         cancelled_percentage = data["cancelled_percentage"][0].get("Cancelled_Percentage", 0)

#     if isinstance(data.get("diverted_percentage"), list) and len(data["diverted_percentage"]) > 0:
#         diverted_percentage = data["diverted_percentage"][0].get("Diverted_Percentage", 0)

#     # Retour des métriques structurées
#     return {
#         "Avg_Departure_Delay": avg_departure_delay,
#         "Avg_Arrival_Delay": avg_arrival_delay,
#         "Cancelled_Percentage": cancelled_percentage,
#         "Diverted_Percentage": diverted_percentage,
#     }


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
    us_map_metrics = transform_us_map_data(data=raw_data)
    us_map_df = add_coordinates(us_map_df=us_map_metrics)
    return organized_metrics, us_map_df

def transform_to_dataframe(data):
    """
    Transforme les données brutes de l'API en un DataFrame structuré pour Plotly.
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

    # Normalisation des valeurs par catégorie
    df["Value"] = df.groupby("Parameter")["Value"].transform(lambda x: (x / x.max()) * 100)

    print("Aperçu des données transformées et normalisées :")
    print(df.head())

    return df

def transform_us_map_data(data):
    """
    Transforme les données 'us-map' en un DataFrame structuré pour Plotly.
    """
    us_map_data = data.get("us-map", [])
    if not us_map_data or not isinstance(us_map_data, list):
        print("Aucune donnée 'us-map' valide trouvée.")
        return pd.DataFrame()

    formatted_data = []
    for entry in us_map_data:
        formatted_data.append({
            "State": entry.get("OriginStateName", "Unknown"),
            "City": entry.get("OriginCityName", "Unknown").split("/")[0].split(",")[0],
            "Avg_Departure_Delay": entry.get("Avg_Departure_Delay", 0),
            "Total_Cancellations": entry.get("Total_Cancellations", 0)
        })

    df = pd.DataFrame(formatted_data)
    if df.empty:
        print("Erreur : Le DataFrame 'us-map' est vide.")
        return pd.DataFrame()

    print("Aperçu des données 'us-map' transformées :")
    print(df.head())
    return df

def add_coordinates(us_map_df):
    """
    Ajoute les coordonnées (latitude, longitude) aux données des villes et stocke dans un CSV.
    """
    CSV_FILE = "us_map_data.csv"
    if os.path.exists(CSV_FILE):
        print("Chargement des coordonnées depuis le CSV...")
        return pd.read_csv(CSV_FILE)

    geolocator = Nominatim(user_agent="us_map_locator")
    latitudes, longitudes = [], []

    for city in us_map_df["City"]:
        print("Ville: ", city)
        try:
            location = geolocator.geocode(city + ", USA")
            print(location)
            if location:
                latitudes.append(location.latitude)
                longitudes.append(location.longitude)
            else:
                latitudes.append(None)
                longitudes.append(None)
        except Exception as e:
            print(f"Erreur lors de la géolocalisation de {city}: {e}")
            latitudes.append(None)
            longitudes.append(None)
        time.sleep(1)  # Pause pour éviter de dépasser les limites d'API

    us_map_df["Latitude"] = latitudes
    us_map_df["Longitude"] = longitudes

    us_map_df.to_csv(CSV_FILE, index=False)
    print(f"Données enregistrées dans {CSV_FILE}")
    return us_map_df

#ANIMATED
def fetch_performance_metrics(year, cities, airlines):
    """
    Récupère les métriques pour une année donnée, une liste de villes et une liste de compagnies aériennes.
    """
    aggregated_metrics = get_performance(year, cities, airlines)
    return aggregated_metrics


#STATISTICS

def get_statistics(years: list[int], cities: list[str], airlines: list[str]):
    """
    Récupère et organise les données de qualité par années, villes et compagnies aériennes.
    """
    # Récupération brute des données via l'API
    raw_data = Spreset_requests.get_statistics_multi(years=years, cities=cities, airlines=airlines)
    if not raw_data:
        print("Aucune donnée récupérée.")
        return {}

    return raw_data

def prepare_calendar_data(calendar_data, category):
    """ Transforme les données JSON en DataFrame pour Plotly. """
    df = pd.DataFrame(calendar_data)
    df["Date"] = pd.to_datetime(df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01")

    # Trouver la bonne colonne de pourcentage
    possible_columns = [f"{category}Percentage", "Percentage"]
    for col in possible_columns:
        if col in df.columns:
            df = df.rename(columns={col: "Percentage"})
            break
    else:
        raise KeyError(f"Aucune colonne de pourcentage trouvée pour {category} dans {df.columns}")

    # S'assurer que Percentage est bien numérique
    df["Percentage"] = pd.to_numeric(df["Percentage"], errors="coerce").fillna(0)

    # Ajouter une colonne "Month_Name" pour un affichage correct
    df["Month_Name"] = df["Date"].dt.strftime("%b")  # Ex: "Jan", "Feb", ...

    return df[["Date", "Year", "Month_Name", "Percentage"]]
