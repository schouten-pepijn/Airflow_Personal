from airflow import DAG
from datetime import datetime
from airflow.decorators import task

import joblib, json

from api_extraction_taskgroup import api_extraction_taskgroup


default_args = {
    'owner': 'pepijnschouten',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'btc_data_dag',
    default_args=default_args,
    description='Extract, transform, load and predict volatility BTC data',
    schedule=None,  # Manual trigger
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["Pepijn"]
) as dag:
        
    @task
    def print_data(data):
        print("---------")
        print(data)
        print("---------")
        
    @task
    def print_data_2(data):
        print("---------")
        print(data)
        print("---------")
        
    extraction_tg = api_extraction_taskgroup()
        

extraction_tg
# print_data(extraction_tg)
# print_data_2(extraction_tg)

        


if __name__ == "__main__":
    dag.test()