from airflow.decorators import task_group, task


@task()
def create_sql_database_task():
    create_table = PostgresOperator(
        task_id="create_table",
        postgress_conn_id="postgres",
    )
    
@task()
def store_sql_database_task(data):

@task_group(group_id="store_to_sql_taskgroup")
def store_to_sql_taskgroup(data):
    create_sql_database_task()
    store_sql_database_task(data)