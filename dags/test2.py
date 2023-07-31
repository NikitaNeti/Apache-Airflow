from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    "owner": "Nikita",
    "start_date": datetime(2023, 6, 2),
    "retries": 2,
    "email_on_failure":True
}

dag = DAG(
    "A",
    default_args=default_args,
    description="DAG to run everyday at 11:00 A.M",
    schedule_interval="0 11 * * *",  
    catchup=False
)

spark_command = "spark-submit --master local   --jars /home/neosoft/Videos/mysql-connector-j-8.0.33.jar   /home/neosoft/airflow/dags/pyspark2.py "
run_spark = BashOperator(
    task_id="run_spark",
    bash_command = f'spark-submit --master local --jars {spark_command}',
    #bash_command='echo "Hello, Airflow!"',
    dag=dag
)

success_email_task = EmailOperator(
    task_id="success_email_task",
    to="nikitaneti19@gmail.com",
    subject="Spark Job Succeeded",
    html_content="The Spark job executed successfully.",
    dag=dag
)

failure_email_task = EmailOperator(
    task_id="failure_email_task",
    to="nikitaneti19@gmail.com",
    subject="Spark Job Failed",
    html_content="The Spark job failed to execute.",
    dag=dag
)

run_spark >> success_email_task
run_spark >> failure_email_task
