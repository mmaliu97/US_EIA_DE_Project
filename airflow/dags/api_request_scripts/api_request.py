import requests
import os

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("EIA_API_KEY is not set")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"

EIA_PARAMS = {
    "frequency": "daily",
    "data[0]": "value",
    "facets[fueltype][]": ["BAT","COL","GEO","NG","NUC","OIL","SNB","SUN","WAT","WND"],
    "facets[respondent][]": ["CISO","ERCO","NYIS"],
    "start": "2025-01-01",
    "end": "2025-02-28",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000
}

def fetch_data(params: dict = None):
    """
    Fetch data from the EIA API using provided params.
    
    If no params are provided, default EIA_PARAMS are used.
    """
    if params is None:
        params = EIA_PARAMS.copy()  # avoid accidental mutation

    # Add API key
    params["api_key"] = API_KEY

    print("Fetching data from EIA API...")

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        print("API response received successfully.")

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while calling EIA API: {e}")
        raise
