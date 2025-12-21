import requests
import os
from datetime import date
from dateutil.relativedelta import relativedelta

API_KEY = os.environ.get("EIA_API_KEY")

if not API_KEY:
    raise RuntimeError("EIA_API_KEY is not set")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"

BASE_PARAMS = {
    "frequency": "daily",
    "data[0]": "value",
    "facets[fueltype][]": ["BAT","COL","GEO","NG","NUC","OIL","SNB","SUN","WAT","WND"],
    "facets[respondent][]": ["CISO","ERCO","NYIS"],
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000
}


def fetch_month_data(start_date: date, end_date: date):
    params = BASE_PARAMS.copy()
    params.update({
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "api_key": API_KEY
    })

    print(f"Fetching {start_date} → {end_date}")

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    return response.json()
