from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    'redshift_data_load',
    default_args={
        'owner': 'Analytics',
        'start_date': datetime(2023, 9, 1),
        'retries': 1,
    },
    schedule=None,  # Set your desired schedule here
    catchup=False,
)

# Define the RedshiftToCsvOperator task
extract_data_task = S3ToRedshiftOperator(
    task_id='load_redshift_table',
    schema='analytics',
    table='atm_billing',
    copy_options=["csv"],
    redshift_conn_id='redshift_test',
    s3_bucket='cf-airflow-source-bucket',
    s3_key='dags-dev/atm_billing.csv',
    dag=dag,
)

# You can add more tasks for further processing or downstream tasks here

if __name__ == "__main__":
    dag.cli()
