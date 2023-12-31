# we want to push multiple values into xcoms on one function

from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Nikita',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name',key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name',key='last_name')
    age = ti.xcom_pull(task_ids='get_age',key='age')
    print(f"Hello World! My name is {first_name} {last_name},"
          f"& I am {age} years old!")

def get_name(ti):
    ti.xcom_push(key='first_name',value='Nisha')
    ti.xcom_push(key='last_name',value='Neti')

def get_age(ti):
    ti.xcom_push(key='age',value=27)

with DAG(
    default_args=default_args,
    dag_id='airflow_xcoms_3',
    description='First dag using python operator',
    start_date=datetime(2023,6,28),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
    
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
    )

    [task2,task3] >> task1