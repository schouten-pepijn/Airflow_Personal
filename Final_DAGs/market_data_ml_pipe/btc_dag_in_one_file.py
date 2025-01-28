from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

import json
import requests

from pydantic import BaseModel, ValidationError

import pandas as pd

import plotly.express as px

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
    
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.extract_data_task'
    )
    
    column_names = ["date", "open", "high", "low", "close"]

    json_structure = [
        {
            column_names[0]: datetime.fromtimestamp(row[0]).strftime("%Y-%m-%d"),
            **{k:float(v) for k, v in zip(column_names[1:], row[1:])}
        } for row in data
    ]

    # push to xcom
    return json.dumps(json_structure)


class DataStructure(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float


def validate_data(**kwargs):
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.convert_data_task'
    )

    for row in json.loads(data):
        try:
            data_model = DataStructure.model_validate(row)
            print(data_model)
        except ValidationError as e:
            raise ValueError(f"Data validation failed for row {row}: {e}") from e
    
    

def create_sql_table(**kwargs):
    
    conn_uri = kwargs['conn_uri']
  
    
    engine = create_engine(conn_uri)
    
    
    query = """
            CREATE TABLE IF NOT EXISTS btc_data (
                date DATE PRIMARY KEY,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL
            );
            """
    
    with engine.connect() as conn:
        conn.execute(query)

def store_to_sql_table(**kwargs):
    data = kwargs['ti'].xcom_pull(
        task_ids='api_taskgroup.convert_data_task'
    )
  
    engine = create_engine(kwargs['conn_uri'])
    
    table = Table("btc_data", MetaData(), autoload_with=engine)
    
    insert_statement = (
        insert(table)
        .values(json.loads(data))
        .on_conflict_do_nothing(index_elements=["date"])
    )

    with engine.connect() as conn:
        conn.execute(insert_statement)


def plot_data(**kwargs):
    df = pd.read_sql_table("btc_data", kwargs['conn_uri'])
    fig = px.line(df, x='date', y='close', title=f"BTCUSD_price_data")
    fig.write_html("btc_historical_close_price.html")


def create_features(**kwargs):
    
    # df = pd.read_sql_table("btc_data", kwargs['conn_uri'])
    # df = df.set_index("date")
    
    # df = df.drop(['open', 'high', 'low'], axis=1)
    
    # df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
    # df['rolling_std_20'] = df['log_returns'].rolling(20).std()
    
    # df = df.dropna()
    
    # # X_train, X_test, y_train, y_test = train_test_split(df['rolling_std_20'])
    
    # print(df.head())
    pass


conn_uri = "postgresql+psycopg2:" \
                "//airflow_user:" \
                    "airflow_pass@localhost:" \
                        "5432/airflow_db"

default_args = {
    'owner': 'pepijnschouten',
    'depends_on_past': False,
    'retries': 0,
}
with DAG(
    'btc_data_dag',
    default_args=default_args,
    description='Extract, transform, load BTC data and predict volatility with boosted tree',
    schedule=None,  # Manual trigger
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["Pepijn"]
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )
    
    with TaskGroup("api_taskgroup") as api_taskgroup:
        extract_data_task = PythonOperator(
            task_id='extract_data_task',
            python_callable=extract_data
        )
        
        convert_data_task = PythonOperator(
            task_id='convert_data_task',
            python_callable=convert_data
        )
        
        validate_data_task = PythonOperator(
            task_id='validate_data_task',
            python_callable=validate_data
        )
        
        extract_data_task >> convert_data_task >> validate_data_task
    
    with TaskGroup("sql_taskgroup") as sql_taskgroup:
        create_sql_table_task = PythonOperator(
            task_id='create_sql_table_task',
            python_callable=create_sql_table,
            op_kwargs={'conn_uri': conn_uri}
        )
        
        store_to_sql_task = PythonOperator(
            task_id='store_to_sql_table_task',
            python_callable=store_to_sql_table,
            op_kwargs={'conn_uri': conn_uri}
        )
        
        create_sql_table_task >> store_to_sql_task
    
    plot_data_task = PythonOperator(
        task_id='plot_data_task',
        python_callable=plot_data,
        op_kwargs={'conn_uri': conn_uri}
    )
    
    with TaskGroup("machine_learning_pipeline_taskgroup") as ml_pipe_taskgroup:
        
        
        create_features_task = PythonOperator(
            task_id='create_features_task',
            python_callable=create_features,
            op_kwargs={'conn_uri': conn_uri}
        )
        
        create_features_task
        
    
    # flow dependencies
    start_task >> api_taskgroup >> sql_taskgroup 
    sql_taskgroup >> plot_data_task
    sql_taskgroup >> ml_pipe_taskgroup
    
    # api_taskgroup.validate_data_task >> create_sql_table_task >> store_to_sql_task
    
    # store_to_sql_task >> plot_data_task
    
    # create_df_task >> [plot_data_task, create_features_task]
    
    
    # create_df_task >> 


if __name__ == "__main__":
    dag.test()