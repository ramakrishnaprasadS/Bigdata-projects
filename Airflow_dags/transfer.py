from datetime import datetime,timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag=DAG(
    'transfer_file',
    default_args=default_args,
    description='A simple DAG to transfer file from one location to another',
    schedule_interval=timedelta(days=1)
)

t1=BashOperator(
    task_id='create_source_file',
    bash_command='echo Hello World >> /mnt/c/Users/miles/Documents/GitHub/materials/files/source_file.txt',
    dag=dag
)

t2=BashOperator(
    task_id='create_destination_dir',
    bash_command='mkdir -p /mnt/c/Users/miles/Documents/GitHub/materials/files2',
    dag=dag
)

def transfer_file():
    with open('/mnt/c/Users/miles/Documents/GitHub/materials/files/source_file.txt','r') as f1, open('/mnt/c/Users/miles/Documents/GitHub/materials/files2/output_file.txt','w') as f2:
        data=f1.read()
        f2.write(data)

t3=PythonOperator(
    task_id='transfer_file',
    python_callable=transfer_file,
    dag=dag
)

t1>>t2>>t3