import requests, urllib.parse

from settings import settings

def get_performance(year: int, city: str, airline: str):
    try:
        # Effectuer une requête GET
        url = f"{settings.API_URL}performance/"
        params = {"year": year, "city": city, "airline": airline}  # Paramètres de la requête GET
        response = requests.get(url, params=params)
        
        # Vérifier si la requête a réussi (code 200)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        
        #print("RESPONSE: ", response.json())
        # Retourner le contenu de la réponse
        return response.json()  # Ou response.text si vous attendez du texte brut
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Gérer les erreurs HTTP
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")  # Gérer les erreurs de connexion
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")  # Gérer les dépassements de délai
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")  # Gérer d'autres erreurs liées à requests
    except Exception as e:
        print(f"An unexpected error occurred: {e}")  # Gérer toutes les autres exceptions
    return None  # Retourner None si une erreur s'est produite

def get_performance_multi(years: list[int], cities: list[str], airlines: list[str]):
    """
    Effectue une requête GET pour récupérer les données pour plusieurs années, villes et compagnies aériennes.
    """
    try:
        # Préparer les paramètres pour la requête
        url = "http://localhost:8000/performance"  # Mettre l'URL de l'API
        params = {
            "year": years,
            "city": cities,
            "airline": airlines,
        }
        response = requests.get(url, params=params)

        # Vérifier la réponse
        response.raise_for_status()
        return response.json()  # Retourner les données JSON
    except requests.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None
    



#STATISTICS

def get_statistics_multi(years: list[int], cities: list[str], airlines: list[str]):
    """
    Effectue une requête GET pour récupérer les données qualité (retards, détournements, annulations).
    """
    try:
        url = "http://localhost:8000/statistics"  # Endpoint de l'API
        params = {
            "year": years,
            "city": cities,
            "airline": airlines,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()  # Retour des données JSON
    except requests.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None
    
#QUALITY

def get_quality_multi(years: list[int], cities: list[str], airlines: list[str]):
    """
    Effectue une requête GET pour récupérer les données de statistiques sur plusieurs années, villes et compagnies aériennes.
    """
    try:
        url = "http://localhost:8000/quality"  # URL de l'API
        params = {
            "year": years,
            "city": cities,
            "airline": airlines,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()  # Retourne les données JSON
    except requests.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return None