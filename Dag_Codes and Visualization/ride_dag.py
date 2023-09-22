from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from spark_dataset_generation import spark_generate
from spark_transformation_dataset import spark_transform
from airflow.operators.http_operator import SimpleHttpOperator
from nifi_data_transfer import start_get_file_processor,stop_get_file_processor
from nifi_data_transfer import start_put_hdfs_processor,stop_put_hdfs_processor
from spark_hive_storing import spark_hive_store



default_args = {'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023,6,22),
        'email':['saksham84a@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }

with DAG(dag_id='ride_sharing',
        default_args=default_args,
        description='capstone',
        # start_date=datetime(),
        schedule_interval='@daily',
        catchup=False,
        # tags=['']
) as dag:
    
    start_data_generation = DummyOperator(task_id='start_data_generation')

    spark_generation = PythonOperator(
        task_id='spark_generation_dataset',
        python_callable=spark_generate
    )
    
    nifi_get_file_start= PythonOperator(
        task_id='Start_Get_File',
        python_callable=start_get_file_processor
    )

    nifi_put_hdfs_start=PythonOperator(
        task_id='Start_put_hdfs',
        python_callable=start_put_hdfs_processor
    )

    nifi_get_file_stop=PythonOperator(
        task_id='Stop_Get_File',
        python_callable=stop_get_file_processor
       
    )

    nifi_put_hdfs_stop=PythonOperator(
        task_id='Stop_put_hdfs',
        python_callable=stop_put_hdfs_processor
    )

    spark_transformation = PythonOperator(
        task_id = 'Spark_transform',
        python_callable=spark_transform

    )

    spark_hive_storing = PythonOperator(
        task_id = 'Hive_Storing',
        python_callable=spark_hive_store
    )
    
    

    start_data_generation >> spark_generation 

    spark_generation  >> nifi_get_file_start >> nifi_put_hdfs_start

    nifi_get_file_start >> nifi_get_file_stop

    nifi_put_hdfs_start >> nifi_put_hdfs_stop >> spark_transformation >> spark_hive_storing
    


    