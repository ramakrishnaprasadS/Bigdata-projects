from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
default_args={
    'owner':'ram',
    'retries':3,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="YT1",
    default_args=default_args,
    description="this dag is for bash task",
    start_date=datetime(2023,4,13,5,24),
    schedule_interval='@daily'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command='echo hello world,this the first task!'
    )

    task2=BashOperator(
        task_id='second_task',
        bash_command='echo hey this is the second task'
    )
    task3=BashOperator(
        task_id='third_task',
        bash_command='echo hey this is the third task'
    )

task1>>[task2,task3]