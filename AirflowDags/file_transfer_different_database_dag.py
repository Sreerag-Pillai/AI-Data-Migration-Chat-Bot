import json
import os
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow import settings
from datetime import timedelta

# JSON file paths
SOURCE_CONNECTION_DETAILS_PATH = '/opt/airflow/plugins/source_connection_details.json'
DESTINATION_CONNECTION_DETAILS_PATH = '/opt/airflow/plugins/dest_connection_details.json'
MAPPING_FILE_PATH = '/opt/airflow/plugins/mapping.json'

# Function to load JSON data
def load_json(file_path):
    with open(file_path) as file:
        return json.load(file)

# Function to create MySQL connection
def create_connection(connection_details_path):
    connection_details = load_json(connection_details_path)
    conn_id = connection_details['conn_id']
    
    session = settings.Session()
    
    # Check if connection already exists and delete if found
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)  
        session.commit()
        print(f"Deleted existing connection with conn_id: {conn_id}")
    
    # Create new connection
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=connection_details['conn_type'],
        host=connection_details['host'],
        login=connection_details['login'],
        password=connection_details['password'],
        schema=connection_details['database'],
        port=connection_details['port']
    )
    
    # Add and commit new connection to session
    session.add(new_conn)
    session.commit()
    print(f"Connection {conn_id} created successfully.")

# Function to transfer data based on mapping.json details
def transfer_data():
    # Load mapping details
    mapping = load_json(MAPPING_FILE_PATH)
    job = mapping['jobs'][0]  
    source = job['source']
    destination = job['destinations'][0]
    
    # Source and destination tables
    source_table = source['sourceProperties']['tableName']
    destination_table = destination['destinationProperties']['tableName']
    
    # MySQL connection IDs for source and destination
    source_conn_id = source['sourceProperties']['connectionId']
    dest_conn_id = destination['destinationProperties']['connectionId']
    
    # Column information for data transfer from joinMetadata
    join_metadata = job.get('joinMetadata', [])
    if join_metadata and join_metadata[0].get('sourceColumnName', '').strip():
        # Specific columns are mentioned for both source and destination
        source_columns = join_metadata[0].get('sourceColumnName', '').split(',')
        destination_columns = join_metadata[0].get('destinationColumnName', '').split(',')
        
        # Create SQL query to select only the source columns
        columns = ", ".join(source_columns)
        query = f"SELECT {columns} FROM {source_table}"
    else:
        # For all columns to be migrated to destination
        columns = "*"
        query = f"SELECT {columns} FROM {source_table}"
        destination_columns = None  

    source_hook = MySqlHook(mysql_conn_id=source_conn_id)
    dest_hook = MySqlHook(mysql_conn_id=dest_conn_id)
    
    # Fetch data from source
    source_data = source_hook.get_pandas_df(query)
    source_data = source_data.fillna('') 

    # Set target fields for insertion into destination table
    target_fields = destination_columns if destination_columns else source_data.columns.tolist()

    # Insert data into destination table
    dest_hook.insert_rows(destination_table, source_data.values.tolist(), target_fields=target_fields)
    print(f"Data transferred from {source_table} in `{source_conn_id}` to {destination_table} in `{dest_conn_id}`.")

# Function to delete all the JSON files after loading the data
def delete_jsons():    
    os.remove(SOURCE_CONNECTION_DETAILS_PATH)
    os.remove(DESTINATION_CONNECTION_DETAILS_PATH)
    os.remove(MAPPING_FILE_PATH)
    print("All files are deleted")

# Define DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='mysql_transfer_dag_different_database',
    default_args=default_args,
    description='Transfer data between different databases in MySQL using JSON config',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task to create MySQL source connection
    create_source_connection = PythonOperator(
        task_id='create_source_connection',
        python_callable=create_connection,
        op_kwargs={'connection_details_path': SOURCE_CONNECTION_DETAILS_PATH}
    )
    
    # Task to create MySQL destination connection
    create_destination_connection = PythonOperator(
        task_id='create_destination_connection',
        python_callable=create_connection,
        op_kwargs={'connection_details_path': DESTINATION_CONNECTION_DETAILS_PATH}
    )
    
    # Task to transfer data from source to destination
    data_transfer_task = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_data
    )

    # Task to delete JSON files
    data_jsons_task = PythonOperator(
        task_id='delete_jsons',
        python_callable=delete_jsons
    )

    # Defining task flow
    [create_source_connection, create_destination_connection] >> data_transfer_task >> data_jsons_task
