import requests
import json

from datetime import datetime

def create_api_connection():
    # Construct the API URL for fetching OHLC data
    url = "https://api.kraken.com/0/public/OHLC?pair=XXBTZUSD&interval=1440"

    try:
        # Make the request to the Kraken API
        response = requests.request(
            "GET",
            url,
            headers={'Accept': 'application/json'},
            data={},
            timeout=10
        )
    except requests.exceptions.Timeout as e:
        # Print timeout exception message
        print(e)

    return json.loads(response.text)['result']["XXBTZUSD"]


def convert_api_response(data):
    column_names = ["date", "open", "high", "low", "close"]

    json_structure = [
        {
            column_names[0]: datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d"),
            **{k:float(v) for k, v in zip(column_names[1:], row[1:])}
        } for row in data
    ]

    return json.dumps(json_structure)


def validate_api_response(json_data):
    from pydantic import BaseModel
    
    data = json.loads(json_data)
    
    class APIStructure(BaseModel):
        date: str
        open: float
        high: float
        low: float
        close: float

    error_bool = False
    for row in data:
        try:
            APIStructure.model_validate(row)
        except ValueError as e:
            print(f"Error with row {row}: {e}")
            error_bool = True
            
    return error_bool