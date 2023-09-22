import os

from airflow import DAG
from datetime import datetime, timedelta 

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def perm():
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/ride_data")
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/ride_data/*")
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/customer_data")
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/customer_data/*")
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/driver_data")
    os.system("sudo chown -R hadoop:hadoop /home/hadoop/dataset/driver_data/*")


default_args = {'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023,6,22),
        'email':['saksham84a@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }

with DAG(dag_id='permission',
        default_args=default_args,
        description='cap',
        # start_date=datetime(),
        schedule_interval='@daily',
        catchup=False,
        # tags=['']
) as dag:
    
    permission = PythonOperator(
        task_id='permiss',
        python_callable=perm
    )
    

