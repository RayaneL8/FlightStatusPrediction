from services import initiators as Sinitiators

def get_cities():
    airlines = []
    try:
        cities = Sinitiators.get_cities()  # Assuming this returns the provided data
        print("Raw cities data:", airlines)
    except Exception as e:
        print(f"Error fetching cities: {e}")
        return []
    
    # Map the data to the required format
    return [{"label": city["OriginCityName"], "value": city["OriginCityName"]} for city in cities]


def get_airlines():
    airlines = []
    try:
        airlines = Sinitiators.get_airlines()  # Assuming this returns the provided data
        print("Raw airlines data:", airlines)
    except Exception as e:
        print(f"Error fetching airlines: {e}")
        return []
    
    # Map the data to the required format
    return [{"label": airline["Airline"], "value": airline["Airline"]} for airline in airlines]

