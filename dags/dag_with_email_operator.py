from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

    # Setting up Triggers
#from airflow.utils.trigger_rule import TriggerRule


def first_function(**context):
    print("Hello world this works ")


def on_failure_callback(context):
    print("Fail works  !  ")


with DAG(dag_id="Email_dag1",
         schedule_interval="@once",
         default_args={
             "owner": "airflow",
             "start_date": datetime(2023, 6, 2),
             "retries": 5,
             "retry_delay": timedelta(minutes=1),
             'on_failure_callback': on_failure_callback,
             'email': ['nikitaneti21@gmail.com'],
             'email_on_failure': False,
             'email_on_retry': False,
         },
         catchup=False) as dag:

    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
    )

    email = EmailOperator(
        task_id='send_email',
        conn_id='Email_default',  
        to='nikitaneti19@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test Airflow </h3> """,
    )


first_function >> email