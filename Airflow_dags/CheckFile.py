from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime

from subdags.DownloadSubDAG import download_subdag
from subdags.TransformSubDAG import transform_subdag

with DAG(dag_id='group_dag',start_date=datetime(28,1,1),schedule_interval='@daily',catchup=False) as dag:
    args={
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }

    downloads=SubDagOperator(
        task_id='download_subdag',
        subdag=download_subdag(dag.dag_id,'download_subdag',args)
    )

    check_file=BashOperator(
        task_id='check_file',
        bash_command='sleep 10'
    )

    transforms=SubDagOperator(
        task_id='transform_subdag',
        subdag=transform_subdag(dag.dag_id,'transform_subdag',args)
    )

    downloads >> check_file >> transforms