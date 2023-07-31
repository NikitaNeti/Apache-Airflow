from airflow import DAG
from datetime import datetime ,timedelta
from airflow.operators.bash import BashOperator

default_args={
    'owner':'airflow',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}
with DAG(
    dag_id='Dag_v3',
    default_args=default_args,
    description='airflow dag with bash operator',
    start_date= datetime(2023,6,28),
    schedule_interval='@daily',
     catchup=False,
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo "Hello, Airflow!"',
    )

    task2 = BashOperator(
        task_id = 'second_task',
        #bash_command="echo,i am task2 & will be running after task1!",
        bash_command='echo "Hello, Airflow!"',
    )

    task3 = BashOperator(
        task_id = 'third_task',
        #bash_command="echo,i am task2 & will be running after task1!",
        bash_command='echo "Hello, Airflow task3 & will be running after task1!"',
    )
    
    # Method 1 task dependencies
    """ 
    task1.set_downstream(task2) 
    task1.set_downstream(task3) 
    """
   
    # Method 2 task dependencies
    ''' 
    task1 >> task2
    task1 >> task3
    '''

    # Method 2 task dependencies
    task1 >> [task2,task3]