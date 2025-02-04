from fastapi import FastAPI, Query
from typing import List, Optional
import spark.queries as queries
import nlp.sql_parser_v2 as parser

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Bienvenue dans l'API Big Data avec Spark et FastAPI !"}

# Helper function to check if the query result is empty
def check_result(data):
    if not data or len(data) == 0:
        return {"message": "No data to display :(, try a different request"}
    return data

@app.get("/list-airlines")
def list_airlines():
    return check_result(queries.list_airlines())

@app.get("/list-cities")
def list_cities():
    return check_result(queries.list_cities())

@app.get("/performance")
def performance_queries(
    year: Optional[list[int]] = Query(None), 
    city: Optional[list[str]] = Query(None), 
    airline: Optional[list[str]] = Query(None)
):
    return {
        "avg_departure_delay": check_result(queries.avg_departure_delay(year=year, city=city, airline=airline)),
        "avg_arrival_delay": check_result(queries.avg_arrival_delay(year=year, city=city, airline=airline)),
        "cancelled_percentage": check_result(queries.cancelled_percentage(year=year, city=city, airline=airline)),
        "diverted_percentage": check_result(queries.diverted_percentage(year=year, city=city, airline=airline)),
        "us-map": check_result(queries.us_map_delay_cancellations())
    }

@app.get("/statistics")
def statistical_queries(
    year: Optional[list[int]] = Query(None), 
    city: Optional[list[str]] = Query(None), 
    airline: Optional[list[str]] = Query(None)
):
    return {
        "total_flights": check_result(queries.total_flights(year=year, city=city, airline=airline)),
        "avg_distance": check_result(queries.avg_distance(year=year, city=city, airline=airline)),
        "flight_distribution_by_airline": check_result(queries.flight_distribution_by_airline()),
        "delay_calendar": check_result(queries.delay_calendar(year=year, city=city, airline=airline)),
        "diverted_calendar": check_result(queries.diverted_flights_calendar(year=year, city=city, airline=airline)),
        "cancelled_calendar": check_result(queries.cancelled_flights_calendar(year=year, city=city, airline=airline)),
        "avg_flight_time": check_result(queries.avg_flight_time(year=year, city=city, airline=airline))
    }

@app.get("/quality")
def quality_queries(
    year: Optional[list[int]] = Query(None), 
    city: Optional[list[str]] = Query(None), 
    airline: Optional[list[str]] = Query(None)
):
    return {
        "avg_taxi_out": check_result(queries.avg_taxi_out(year=year, city=city, airline=airline)),
        "avg_taxi_in": check_result(queries.avg_taxi_in(year=year, city=city, airline=airline)),
        "flights_delayed_15_plus": check_result(queries.flights_delayed_15_plus(year=year, city=city, airline=airline)),
        "flights_delayed_less_15": check_result(queries.flights_delayed_less_15(year=year, city=city, airline=airline)),
        "cancelled_flights": check_result(queries.cancelled_flights(year=year, city=city, airline=airline)),
        "diverted_flights": check_result(queries.diverted_flights(year=year, city=city, airline=airline))
    }

@app.get("/nlp/{query}")
def sql_query(query: str):
    p = parser.SQLParser()
    quer = p.parse_to_sql(query)
    print('Generated SQL query:', quer)
    print('ici')
    result = queries.execute_raw_query(quer)
    print('ici')
    data = result["data"] 
    if not data or len(data) == 0:
        return {"content": None , "sql_query": quer, "valid": False, "elapsed_time": result["elapsed_time"]} 

    res= {"content": result["data"] , "sql_query": quer, "valid": True, "elapsed_time": result["elapsed_time"]} 
    return res



# "message": "No data to display :(, try a different request"

@app.get("/cancelled-flights-percentage-year/{year}")
def cancelled_flights_percentage_year(year: int):
    return check_result(queries.cancelled_flights_percentage_year(year))

@app.get("/cancelled-flights-percentage-since/{date}")
def cancelled_flights_percentage_since(date: str):
    return check_result(queries.cancelled_flights_percentage_since(date))

@app.get("/delayed-flights-percentage-year/{year}")
def delayed_flights_percentage_year(year: int):
    return check_result(queries.delayed_flights_percentage_year(year))

@app.get("/delayed-flights-percentage-since/{date}")
def delayed_flights_percentage_since(date: str):
    return check_result(queries.delayed_flights_percentage_since(date))

@app.get("/most-used-airlines")
def most_used_airlines():
    return check_result(queries.most_used_airlines())

@app.get("/best-performing-airlines")
def best_performing_airlines():
    return check_result(queries.best_performing_airlines())

@app.get("/ranking-states/{state}")
def ranking_states(state: str):
    return check_result(queries.ranking_states(state))

@app.get("/diverted-flights-percentage-year/{year}")
def diverted_flights_percentage_year(year: int):
    return check_result(queries.diverted_flights_percentage_year(year))

@app.get("/diverted-flights-percentage-since/{date}")
def diverted_flights_percentage_since(date: str):
    return check_result(queries.diverted_flights_percentage_since(date))

@app.get("/delay-proportions")
def delay_proportions():
    return check_result(queries.delay_proportions_sql())

@app.get("/flights-per-year")
def flights_per_year():
    return check_result(queries.flights_per_year_sql())

@app.get("/flight-results-by-year")
def flight_results_by_year():
    return check_result(queries.flight_results_by_year_sql())

@app.get("/flight-results-by-month")
def flight_results_by_month():
    return check_result(queries.flight_results_by_month_sql())

@app.get("/cancelled-flights-calendar")
def cancelled_flights_calendar():
    return check_result(queries.cancelled_flights_calendar_sql())
