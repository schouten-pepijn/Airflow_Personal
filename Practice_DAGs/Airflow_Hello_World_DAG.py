from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# hello world function to execute
def hello_world():
    print('Hello World!')
    
# default arguments for DAG
default_args = {
    'owner': 'pepijnschouten',
    'depends_on_past': False,
    'retries': 0,
}

# instantiate DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule=None,  # Manual trigger
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    # define the task
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world,
    )
    
# set task dependencies
hello_task
