from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Nikita',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def greet(age,ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello World! My name is {name},"
          f"& I am {age} years old!")

def get_name():
    return 'Jerry'

with DAG(
    default_args=default_args,
    dag_id='airflow_xcoms_1',
    description='First dag using python operator',
    start_date=datetime(2023,6,28),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'age':24}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        
    )

    task2 >> task1