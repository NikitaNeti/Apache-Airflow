from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Nikita',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def greet(name,age):
    print(f"Hello World! My name is {name},"
          f"& I am {age} years old!")

with DAG(
    default_args=default_args,
    dag_id='python_operator_2',
    description='First dag using python operator',
    start_date=datetime(2023,6,28),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'name':'Nikita','age':24}
    )

    task1