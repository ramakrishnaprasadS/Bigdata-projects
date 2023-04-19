from airflow import DAG
from airflow.operators.bash import BashOperator

def download_subdag(parent_id,child_id,args):

    with DAG(dag_id=f'{parent_id}.{child_id}', start_date=args['start_date'],schedule_interval=args['schedule_interval'],catchup=args['catchup']) as dag:

        download_a=BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b=BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c=BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

        return dag

        