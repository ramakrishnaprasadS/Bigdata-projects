from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

default_args={
    'owner':'ram',
    'retries':3,
    'retry_delay': timedelta(minutes=2)
}

def greet(ti):
    name=ti.xcom_pull(task_ids='get_name',key='fname')+" "+ti.xcom_pull(task_ids='get_name',key='lname')
    age=ti.xcom_pull(task_ids='get_age',key='age')
    print(f"Hello world ! My name is {name}, and I am {age} years old")

def get_name(ti):
    ti.xcom_push(key='fname',value='Rama')
    ti.xcom_push(key='lname',value='Krishna')

def get_age(ti):
    ti.xcom_push(key='age',value=21)

with DAG(
default_args=default_args,
dag_id="dag2_pyth",
description="dag for python operator",
start_date=datetime(2023,4,13),
schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
        
    )
    task2=PythonOperator(
        task_id='get_name',
        python_callable=get_name

    )
    task3=PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
[task2,task3]>>task1

