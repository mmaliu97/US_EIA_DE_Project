import requests
import os
from datetime import date
from dateutil.relativedelta import relativedelta  # pip install python-dateutil

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("EIA_API_KEY is not set")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"

# ---- dynamic dates ----
end_date = date.today()
start_date = end_date - relativedelta(months=1)

EIA_PARAMS = {
    "frequency": "daily",
    "data[0]": "value",
    "facets[fueltype][]": ["BAT","COL","GEO","NG","NUC","OIL","SNB","SUN","WAT","WND"],
    "facets[respondent][]": ["CISO","ERCO","NYIS"],
    "start": start_date.isoformat(),  # YYYY-MM-DD
    "end": end_date.isoformat(),
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
        params = EIA_PARAMS.copy()

    params["api_key"] = API_KEY

    print(f"Fetching data from {params['start']} to {params['end']}...")

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        print("API response received successfully.")
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error occurred while calling EIA API: {e}")
        raise
