from fastapi import FastAPI
import spark.queries as queries

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bienvenue dans l'API Big Data avec Spark et FastAPI !"}


@app.get("/list-airlines")
def list_airlines():
    return queries.list_airlines()


@app.get("/cancelled-flights-percentage-year/{year}")
def cancelled_flights_percentage_year(year: int):
    return queries.cancelled_flights_percentage_year(year)


@app.get("/cancelled-flights-percentage-since/{date}")
def cancelled_flights_percentage_since(date: str):
    return queries.cancelled_flights_percentage_since(date)


@app.get("/delayed-flights-percentage-year/{year}")
def delayed_flights_percentage_year(year: int):
    return queries.delayed_flights_percentage_year(year)


@app.get("/delayed-flights-percentage-since/{date}")
def delayed_flights_percentage_since(date: str):
    return queries.delayed_flights_percentage_since(date)


@app.get("/most-used-airlines")
def most_used_airlines():
    return queries.most_used_airlines()


@app.get("/best-performing-airlines")
def best_performing_airlines():
    return queries.best_performing_airlines()


@app.get("/ranking-states/{state}")
def ranking_states(state: str):
    return queries.ranking_states(state)


@app.get("/diverted-flights-percentage-year/{year}")
def diverted_flights_percentage_year(year: int):
    return queries.diverted_flights_percentage_year(year)


@app.get("/diverted-flights-percentage-since/{date}")
def diverted_flights_percentage_since(date: str):
    return queries.diverted_flights_percentage_since(date)



@app.get("/delay-proportions")
def delay_proportions():
    # Appelle la fonction SQL pour obtenir les proportions de retards
    return queries.delay_proportions_sql()


@app.get("/flights-per-year")
def flights_per_year():
    # Appelle la fonction SQL pour obtenir le nombre de vols par année
    return queries.flights_per_year_sql()


@app.get("/flight-results-by-year")
def flight_results_by_year():
    # Appelle la fonction SQL pour obtenir les résultats des vols par année
    return queries.flight_results_by_year_sql()


@app.get("/flight-results-by-month")
def flight_results_by_month():
    # Appelle la fonction SQL pour obtenir les résultats des vols par mois
    return queries.flight_results_by_month_sql()


@app.get("/cancelled-flights-calendar")
def cancelled_flights_calendar():
    # Appelle la fonction SQL pour obtenir les pourcentages de vols annulés par mois
    return queries.cancelled_flights_calendar_sql()


@app.get("/compare-airlines")
def compare_airlines():
    # Appelle la fonction SQL pour comparer les compagnies aériennes sur plusieurs critères
    return queries.compare_airlines_sql()