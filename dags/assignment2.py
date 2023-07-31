from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    "owner": "Nikita",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 2),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["nikitaneti21@gmail.com"],
    "email_on_failure": True
}

dag = DAG(
    "Data_processing",
    default_args=default_args,
    description="DAG to run everyday at 11:00 A.M",
    schedule_interval="0 11 * * *",  
    catchup=False 
)

spark_command = "spark-submit --master local   --jars /home/neosoft/Videos/mysql-connector-j-8.0.33.jar   /home/neosoft/airflow/dags/pyspark2.py "

run_spark = BashOperator(
    task_id="run_spark",
    bash_command=spark_command,
    #bash_command='echo "Hello, Airflow!"',
    dag=dag
)

run_spark




