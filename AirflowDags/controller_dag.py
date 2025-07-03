import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.api.common.trigger_dag import trigger_dag
from airflow.operators.dummy import DummyOperator
from datetime import timedelta

# Json path
FILE_PATHS = [
    '/opt/airflow/plugins/source_connection_details.json',
    '/opt/airflow/plugins/dest_connection_details.json',
    '/opt/airflow/plugins/mapping.json'
]

def check_and_trigger_dag(**kwargs):
    """Check the number of existing files, trigger the appropriate DAG, and determine the next step."""
    file_count = sum([1 for path in FILE_PATHS if os.path.exists(path)])

    if file_count == 2:
        # Trigger mysql_transfer_dag for the transfer within database
        trigger_dag(dag_id="mysql_transfer_dag", execution_date=kwargs['execution_date'])
        kwargs['ti'].xcom_push(key='triggered_dag_id', value='mysql_transfer_dag')
        print("Triggered 'mysql_transfer_dag'")
        return 'wait_for_dependent_dag'
    elif file_count == 3:
        # Trigger mysql_transfer_dag_different_database for the transfer between two databases
        trigger_dag(dag_id="mysql_transfer_dag_different_database", execution_date=kwargs['execution_date'])
        kwargs['ti'].xcom_push(key='triggered_dag_id', value='mysql_transfer_dag_different_database')
        print("Triggered 'mysql_transfer_dag_different_database'")
        return 'wait_for_dependent_dag'
    else:
        # No files matching the required count, skip to no_file task
        print("No matching DAG triggered as file count is not 2 or 3.")
        return 'no_file'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': -1,  
    'retry_delay': timedelta(minutes=1)  
}

with DAG(
    'controller_dag',
    default_args=default_args,
    description='Controller DAG to monitor files and trigger other DAGs based on file count',
    schedule_interval='* * * * *',  
    max_active_runs=1,  
    catchup=False  
) as dag:

    # To decide which task to run next
    branch_task = BranchPythonOperator(
        task_id='check_and_trigger_dag',
        python_callable=check_and_trigger_dag,
        provide_context=True
    )

    # If no files are there run this task
    no_file = DummyOperator(
        task_id='no_file'
    )

    # Task wait for the completion of the triggered DAG
    wait_for_dependent_dag = ExternalTaskSensor(
        task_id='wait_for_dependent_dag',
        external_dag_id="{{ task_instance.xcom_pull(task_ids='check_and_trigger_dag', key='triggered_dag_id') }}",
        external_task_id=None,  
        timeout=3600,  
        mode='poke',  
        poke_interval=30,  
        soft_fail=True  
    )

    # Defining task flow
    branch_task >> [wait_for_dependent_dag, no_file]
