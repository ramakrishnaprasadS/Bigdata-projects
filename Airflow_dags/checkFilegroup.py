from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from groups.DownloadGroup import download_tasks
from groups.TransformGroup import transform_tasks

with DAG(dag_id='task_group_dag',start_date=datetime(28,1,1),schedule_interval='@daily',catchup=False) as dag:

    downloads=download_tasks()

    check_file=BashOperator(
        task_id='check_file',
        bash_command='sleep 10'
    )

    transforms=transform_tasks()

    downloads >> check_file >> transforms

    