from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API Flight Delay"}

@app.get("/data")
def get_data():
    # Lire le fichier Parquet
    try:
        df = pd.read_parquet("data/flight_data.parquet", engine="pyarrow")
        # Retourner les premières lignes en format JSON
        return {"data": df.head().to_dict()}
    except Exception as e:
        return {"error": str(e)}
