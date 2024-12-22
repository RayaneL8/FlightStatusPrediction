from fastapi import FastAPI
import spark.queries as queries
from fastapi import FastAPI, Query
from typing import Optional
import spark.queries as queries
import nlp.sql_parser_v2 as parser

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bienvenue dans l'API Big Data avec Spark et FastAPI !"}


@app.get("/list-airlines")
def list_airlines():
    return queries.list_airlines()

@app.get("/performance")
def performance_queries(
    year: Optional[int] = Query(None), 
    city: Optional[str] = Query(None), 
    airline: Optional[str] = Query(None)
):
    return {
        "avg_departure_delay": queries.avg_departure_delay(year=year, city=city, airline=airline),
        "avg_arrival_delay": queries.avg_arrival_delay(year=year, city=city, airline=airline),
        "cancelled_percentage": queries.cancelled_percentage(year=year, city=city, airline=airline),
        "diverted_percentage": queries.diverted_percentage(year=year, city=city, airline=airline),
        "us-map" : queries.us_map_delay_cancellations()
    }

@app.get("/statistics")
def statistical_queries(
    year: Optional[int] = Query(None), 
    city: Optional[str] = Query(None), 
    airline: Optional[str] = Query(None)
):
    return {
        "total_flights": queries.total_flights(year=year, city=city, airline=airline),
        "avg_distance": queries.avg_distance(year=year, city=city, airline=airline),
        "flight_distribution_by_airline": queries.flight_distribution_by_airline(),
        "delay_calendar" : queries.delay_calendar(year=year, city=city, airline=airline),
        "diverted_calendar" : queries.diverted_flights_calendar(year=year, city=city, airline=airline),
        "cancelled_calendar" : queries.cancelled_flights_calendar(year=year, city=city, airline=airline),
        "avg_flight_time": queries.avg_flight_time(year=year, city=city, airline=airline),
    }

@app.get("/quality")
def quality_queries(
    year: Optional[int] = Query(None), 
    city: Optional[str] = Query(None), 
    airline: Optional[str] = Query(None)
):
    return {
        "avg_taxi_out": queries.avg_taxi_out(year=year, city=city, airline=airline),
        "avg_taxi_in": queries.avg_taxi_in(year=year, city=city, airline=airline),
        "flights_delayed_15_plus": queries.flights_delayed_15_plus(year=year, city=city, airline=airline),
        "flights_delayed_less_15": queries.flights_delayed_less_15(year=year, city=city, airline=airline),
        "cancelled_flights": queries.cancelled_flights(year=year, city=city, airline=airline),
        "diverted_flights": queries.diverted_flights(year=year, city=city, airline=airline)
    }

#route for my sql parser
@app.get("/nlp/{query}")
def sql_query(query: str):
    p= parser.SQLParser()
    quer= p.parse_to_sql(query)
    print ('genarated SQL query :',quer)
    return queries.execute_raw_query(quer)
    



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
