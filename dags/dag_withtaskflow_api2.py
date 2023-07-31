# to return first_name & last_name instead of name in our get_name task

from airflow.decorators import dag, task
from datetime import timedelta, datetime

default_args = {
    'owner':'Nikita',
    'retries':5,
    'retry_delta':timedelta(minutes=5)
}

@dag(
    dag_id='airflow_api_2',
    default_args=default_args,
    start_date=datetime(2023,6,26),
    schedule_interval='@daily',
    catchup=False)

def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Jerry',
            'last_name':'Fridman'
        }

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name,last_name,age):
        print(f"hello World! My Name is {first_name} {last_name}" 
        f"& I am {age} years old!")

    name_dict = get_name()
    age = get_age()

    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
          age=age)

greet_dag = hello_world_etl()