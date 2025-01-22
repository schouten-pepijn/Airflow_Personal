from datetime import datetime
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# writer function
def write_csv(data, **kwargs):
    
    # save paths
    dir_path = "/Users/pepijnschouten/Documents/Airflow_Outputs/"
    file_path = dir_path + "editted_write_to_csv_dag.csv"
    
    # write to csv
    field_names = data[0].keys()
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(data)

    print(f"csv saved to {file_path}")

# reader function
def read_csv(**kwargs):
    # file path
    file_path = "/Users/pepijnschouten/Documents/Airflow_Outputs/write_to_csv_dag.csv"
    
    # read csv or default
    if os.path.exists(file_path):
        with open(file_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
    else:
        data = [
            {'Name': 'John', 'Age': 25, 'City': 'New York'},
            {'Name': 'Jane', 'Age': 30, 'City': 'San Francisco'},
            {'Name': 'Bob', 'Age': 35, 'City': 'Seattle'},
            {'Name': 'Alice', 'Age': 28, 'City': 'Los Angeles'}
        ]

    return data


# default arguments for DAG
default_args = {
    'owner': 'pepijnschouten',
    'depends_on_past': False,
    'retries': 0,
}

# instantiate DAG
with DAG(
    "read_write_csv_dag",
    default_args=default_args,
    description="A simple CSV read/write DAG",
    schedule=None,  # Manual trigger
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["Pepijn"]
) as dag:
    
    # read task
    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
    )

    # write task
    write_csv_task = PythonOperator(
        task_id='write_csv_task',
        python_callable=write_csv,
        op_kwargs={"data": read_csv_task.output},  # pass optional arg
    )

# set task dependencies
read_csv_task >> write_csv_task