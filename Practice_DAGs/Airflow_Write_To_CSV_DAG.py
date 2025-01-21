import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# function to write to csv
def write_csv():
    
    data = [
        ["Name", "Age", "City"],
        ["John", 25, "New York"],
        ["Jane", 30, "San Francisco"],
        ["Bob", 35, "Seattle"],
        ["Alice", 28, "Los Angeles"],
    ]
    
    file_path = "/tmp/write_to_csv_dag.csv"
    
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)
   
    print(f"csv saved to {file_path}")
    

# default arguments for DAG
default_args = {
    'owner': 'pepijnschouten',
    'depends_on_past': False,
    'retries': 0,
}

# instantiate DAG
with DAG(
    "write_csv_dag",
    default_args=default_args,
    description="A simple CSV write DAG",
    schedule=None,  # Manual trigger
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["Pepijn"]
) as dag:
    
    # define the task
    write_csv_task = PythonOperator(
        task_id='write_csv_task',
        python_callable=write_csv,
    )
    
# set task dependencies
write_csv_task