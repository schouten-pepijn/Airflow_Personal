import requests
import json

from datetime import datetime

def fetch_ohlc_data(
    ticker: str = "XXBTZUSD",
    interval: str = "1d"
) -> list:
    """
    Fetches OHLC data from the Kraken API and returns it as a list of lists,
    where each sublist contains the following elements in this order:
    [time, open, high, low, close, vwap, volume, count]
    """
    
    # Daily interval
    if interval == "1d":
        interval_num = str(60*24)
    
    # Construct the API URL for fetching OHLC data
    url = f"https://api.kraken.com/0/public/OHLC?pair={ticker}&interval={interval_num}"

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

    data = json.loads(response.text)['result'][ticker]

    return data


def convert_to_json(
    data: list,
    column_names: list
) -> str:
    
    json_structure = [
        {
            column_names[0]: datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d"),
            **{k:float(v) for k, v in zip(column_names[1:], row[1:])}
        } for row in data
    ]
    
    json_data = json.dumps(json_structure)
    
    with open('btc_data.json', 'w') as f:
        f.write(json_data)
    
    return json_data
    