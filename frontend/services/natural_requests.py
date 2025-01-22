import requests, urllib.parse

from settings import settings

def send_request(content: str):
    print("Send Req")
    print("Print API URL : ", settings.API_URL)
    print("CONTENT: ", content)
    try:
        # Effectuer une requête GET
        url = f"{settings.API_URL}nlp/{urllib.parse.quote(content)}"
        print("FULL URL: ", url)
        response = requests.get(url)
        
        # Vérifier si la requête a réussi (code 200)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        
        #print("RESPONSE: ", response.json())
        # Retourner le contenu de la réponse
        return response.json(), response.elapsed.total_seconds()  # Ou response.text si vous attendez du texte brut
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
    return None, None  # Retourner None si une erreur s'est produite