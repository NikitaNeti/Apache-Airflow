from airflow.decorators import dag, task
from datetime import timedelta, datetime

default_args = {
    'owner':'Nikita',
    'retries':5,
    'retry_delta':timedelta(minutes=5)
}

@dag(
    dag_id='airflow_api_1',
    default_args=default_args,
    start_date=datetime(2023,6,28),
    schedule_interval='@daily')

def hello_world_etl():
    
    @task()
    def get_name():
        return 'Jerry'

    @task()
    def get_age():
        return 19

    @task()
    def greet(name,age):
        print(f"hello World! My Name is {name}" 
        f"& I am {age} years old!")

    name = get_name()
    age = get_age()
    greet(name=name,age=age)

greet_dag = hello_world_etl()