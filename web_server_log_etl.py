from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'web_server_log_etl',
    default_args=default_args,
    description='A simple ETL pipeline for web server logs',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

# Task to create PostgreSQL table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS web_logs (
            ip_address VARCHAR(50),
            timestamp TIMESTAMP,
            request VARCHAR(200),
            status_code INT,
            response_size INT
        );
    """,
    dag=dag,
)

# Task to process logs with PySpark
process_logs = SparkSubmitOperator(
    task_id='process_logs',
    application='/opt/airflow/scripts/process_logs.py',
    conn_id='spark_default',
    dag=dag,
)

# Task to load data into PostgreSQL
load_data = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_default',
    sql="""
        COPY web_logs FROM '/opt/airflow/scripts/processed_logs.csv' DELIMITER ',' CSV HEADER;
    """,
    dag=dag,
)

create_table >> process_logs >> load_data
