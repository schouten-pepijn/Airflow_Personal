import requests
import json
from datetime import datetime
from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np


def extract_data():
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

    # push to xcom
    return json.loads(response.text)['result']["XXBTZUSD"]
    
    
def convert_data(**kwargs):
    # pull api data from xcom
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.extract_data_task'
    )
    # hard coded column names
    column_names = ["date", "open", "high", "low", "close"]
    # convert to desired json structure
    json_structure = [
        {
            column_names[0]: datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d"),
            **{k:float(v) for k, v in zip(column_names[1:], row[1:])}
        } for row in data
    ]
    # push to xcom
    return json.dumps(json_structure)


def validate_data(**kwargs):
    # data type validation model
    class DataStructure(BaseModel):
        date: str
        open: float
        high: float
        low: float
        close: float
    # pull data from xcom
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.convert_data_task'
    )
    # validate each data entry
    for row in json.loads(data):
        try:
            data_model = DataStructure.model_validate(row)
            print(data_model)
        # handle validation errors
        except ValidationError as e:
            raise ValueError(f"Data validation failed for row {row}: {e}") from e
     
        
def create_sql_table(**kwargs):
    # connect to postgres
    conn_uri = kwargs['conn_uri']
    engine = create_engine(conn_uri)
    # hard codedcreate table query
    query = """
            CREATE TABLE IF NOT EXISTS btc_data (
                date DATE PRIMARY KEY,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL
            );
            """
    # execute the create query
    with engine.connect() as conn:
        conn.execute(query)


def store_to_sql_table(**kwargs):
    # pull data from xcom
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.convert_data_task'
    )
    # connect to postgres and retrieve table
    engine = create_engine(kwargs['conn_uri'])
    table = Table("btc_data", MetaData(), autoload_with=engine)
    # create the insert statement
    insert_statement = (
        insert(table)
        .values(json.loads(data))
        .on_conflict_do_nothing(index_elements=["date"])
    )
    # execute the insert query
    with engine.connect() as conn:
        conn.execute(insert_statement)
        
        
def plot_data(**kwargs):
    # connect to postgres and retrieve table
    df = pd.read_sql_table("btc_data", kwargs['conn_uri'])
    # plot the data and write to html
    fig = px.line(df, x='date', y='close', title=f"BTCUSD_price_data")
    fig.write_html("btc_historical_close_price.html")
    

def plot_predictions(**kwargs):
    # pull data from xcom
    data = kwargs['ti'].xcom_pull(
        task_ids='ml_taskgroup.train_eval_model_task'
    )
    # convert to dataframe
    df = pd.DataFrame([json.loads(item) for item in data])
    # 45 degree line
    xy_line = np.linspace(0.01, 0.05, 100)
    # plot the data
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df['volatility'], y=df['prediction'],
            mode='markers',
            marker=dict(color='blue')
        )
    )
    fig.add_trace(
        go.Scatter(
            x=xy_line, y=xy_line,
            mode='lines',
            line=dict(dash='dash', color='green')
        )
    )
    fig.update_layout(
        title="True vs predicted volatility",
        xaxis_title="True volatility",
        yaxis_title="Predicted volatility",
        showlegend=False
    )
    # write to html
    fig.write_html("predictions_scatter_plot.html")
    
    

    