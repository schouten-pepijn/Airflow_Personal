from airflow.decorators import task_group, task
from api_funcs import create_api_connection, convert_api_response, validate_api_response



@task(task_id="extract_from_api_task")
def extract_from_api_task():
    return create_api_connection()

@task(task_id="convert_response_task")
def convert_response_task(data):
    return convert_api_response(data)

@task(task_id="validate_response_task")
def validate_response_task(data):
    return validate_api_response(data)
    

@task_group(group_id="api_extraction_taskgroup")
def api_extraction_taskgroup():
    
    api_response = extract_from_api_task()
    converted_response = convert_response_task(api_response)
    error = validate_response_task(converted_response)
    print(error)
    if error:
        raise ValueError("Error with response column types")
    else:
        return converted_response
        

