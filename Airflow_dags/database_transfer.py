from airflow import DAG
from datetime import datetime
import psycopg2

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

def create_csv_file():
    conn=psycopg2.connect("host=postgres dbname=airflow user=airflow password=airflow")
    cur=conn.cursor()
    sql="COPY (SELECT * FROM newUsers) TO STDOUT WITH CSV DELIMITER ','"
    with open('/tmp/table_data.csv', 'w') as f:
        cur.copy_expert(sql,f)  

with DAG('database_transfer',start_date=datetime(2023,1,1),schedule_interval='@daily',catchup=False) as dag:
    create_table=PostgresOperator(
        task_id='create_table',
        postgres_conn_id='airflow',
        sql='''
            CREATE TABLE IF NOT EXISTS newUsers (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    transfer_table=BashOperator(
        task_id='transfer_table',
        bash_command='PGPASSWORD=airflow psql -h postgres -p 5432 -U airflow -d airflow -c "INSERT INTO newUsers (SELECT * FROM users)"'
    )

    copy_data=PythonOperator(
        task_id='copy_data',
        python_callable=create_csv_file
    )

    check_file=FileSensor(
        task_id="check_file",
        filepath='/tmp/table_data.csv',
        fs_conn_id='fs_local'
    )

    create_table >> transfer_table >> copy_data >> check_file