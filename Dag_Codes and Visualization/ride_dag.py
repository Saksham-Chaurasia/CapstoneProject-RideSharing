from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from spark_dataset_generation import spark_generate
from spark_transformation_dataset import spark_transform
from nifi_data_transfer import start_get_file_processor,stop_get_file_processor
from nifi_data_transfer import start_put_hdfs_processor,stop_put_hdfs_processor
from spark_hive_storing import spark_hive_store
from spark_Data_quality_checks import quality_checks
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import BranchPythonOperator


def run_storing(**kwargs):
    ti = kwargs['ti']
    quality_check_passed = ti.xcom_pull(task_ids='Quality_checks')  

    if quality_check_passed:
        return 'spark_hive_storing'
    else:
        return 'email_fail_task'


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

    spark_quality_checks = PythonOperator(
        task_id = 'Quality_checks',
        python_callable=quality_checks
    )

    branch_operator = BranchPythonOperator(
        task_id='branch_operator',
        python_callable=run_storing,
        provide_context=True,
    )


    email_fail_task = EmailOperator(
        task_id="Alert_Fail",
        to=['saksham84a@gmail.com'],
        subject="Data Quality Checks",
        html_content="<i>Message from Airflow ---> Data Quality --> Fails</i>"
    
    )

    spark_hive_storing = PythonOperator(
        task_id = "spark_hive_storing",
        python_callable = spark_hive_store
    )


    email_success_task = EmailOperator(
        task_id="SuccessFull",
        to=['saksham84a@gmail.com'],
        subject="Data Quality Checks",
        html_content="<i>SuccessFule Airflow Pipeline</i>"
    )


    start_data_generation >> spark_generation 

    spark_generation  >> nifi_get_file_start >> nifi_put_hdfs_start

    nifi_get_file_start >> nifi_get_file_stop

    nifi_put_hdfs_start >> nifi_put_hdfs_stop >> spark_transformation >> spark_quality_checks

    spark_quality_checks >> branch_operator 

    branch_operator >> [spark_hive_storing, email_fail_task] 

    spark_hive_storing >> email_success_task

