from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from datetime import datetime

# Define your DAG
dag = DAG(
    'redshift_data_extraction',
    default_args={
        'owner': 'Analytics',
        'start_date': datetime(2023, 9, 1),
        'retries': 1,
    },
    schedule=None,  # Set your desired schedule here
    catchup=False,
)

# Define the RedshiftToCsvOperator task
extract_data_task = RedshiftToS3Operator(
    task_id='extract_data_from_redshift',
    schema='analytics',
    table='atm_billing',
    unload_options=['csv'],
    redshift_conn_id='redshift_test',
    s3_bucket='cf-airflow-source-bucket',
    s3_key='logs',
    dag=dag,
)




# You can add more tasks for further processing or downstream tasks here

if __name__ == "__main__":
    dag.cli()
