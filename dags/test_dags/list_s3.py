from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago

# Define the default_args dictionary to configure the DAG.
default_args = {
    'owner': 'analytics',
    'start_date': days_ago(1),  # Adjust the start date as needed.
    'retries': 0,
}

# Create the DAG with the specified default_args.
dag = DAG(
    's3_list_test',
    default_args=default_args,
    description='DAG to list objects in an S3 bucket',
    schedule_interval=None,  # Set the schedule interval as needed.
    catchup=False,  # Disable catch-up to only run the most recent execution.
)

# Specify the S3 bucket and prefix you want to list objects from.
s3_bucket_name = 'cf-airflow-source-bucket'
s3_prefix = 'dags-dev/'

# Create an S3ListOperator to list objects in the S3 bucket.
list_objects_task = S3ListOperator(
    task_id='list_s3_objects',
    bucket=s3_bucket_name,
    prefix=s3_prefix,
    delimiter='',  # You can specify a delimiter if needed.
    aws_conn_id='aws_default',  # Use the default AWS connection.
    do_xcom_push=True,  # Set to True to push the object list to XCom.
    dag=dag,
)

# Set task dependencies.
list_objects_task

if __name__ == "__main__":
    dag.cli()
