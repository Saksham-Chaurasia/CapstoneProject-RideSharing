from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime,timedelta


default_args = {'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023,6,22),
        'email':['saksham84a@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }

with DAG(dag_id='hive_store',
        default_args=default_args,
        description='capstone',
        # start_date=datetime(),
        schedule_interval='@daily',
        catchup=False,
        # tags=['']
) as dag:
    
    spark_task = SparkSubmitOperator(
    task_id='spark_task',
    application='usr/local/spark/spark_hive_storing.py', 
    conn_id='spark_default',  
    verbose=False,
    #application_args=['arg1', 'arg2'],  
    )
    


    spark_task
    


    