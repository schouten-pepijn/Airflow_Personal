import requests
import pandas as pd

def get_market_data(
    ticker: str = "XXBTZUSD"
) -> pd.DataFrame:
    
    # Daily interval
    interval = str(60*24)

    # Construct the API URL for fetching OHLC data
    url = f"https://api.kraken.com/0/public/OHLC?pair={ticker}&interval={interval}"

    # Set up request headers
    headers = {'Accept': 'application/json'}

    try:
        # Make the request to the Kraken API
        response = requests.request(
            "GET", url, headers=headers,
            data={}, timeout=10
        )
    except requests.exceptions.Timeout as e:
        # Print timeout exception message
        print(e)


    return response.json()['result'][ticker]


def convert_json_to_df(json_data):
    
    
        # Parse the JSON response into a DataFrame
    df = pd.DataFrame(
        json_data,
        columns=['timestamp', 'open', 'high', 'low',
                    'close', 'vwap', 'volume', 'count'],
        dtype=float
    )

    # Convert timestamps to datetime and set as index
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    return df
