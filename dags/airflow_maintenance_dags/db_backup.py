from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator    
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.s3  import (S3DeleteObjectsOperator, S3ListOperator)
from airflow.utils.dates import days_ago
import os
from datetime import datetime,timedelta

previous_month = (datetime.now() -  timedelta(days=30)).strftime("%Y%m%d")
#current_date = {{ds_nodash}}
# Define your DAG
dag = DAG(
    'airflow_db_backup',
    schedule_interval='@daily',  # Set the desired schedule
    start_date=days_ago(1),  # Start date for the DAG
    catchup=False,  # Prevent backfilling of previous days
    tags=['db-backup', 'airflow-maintenance-dags'],
)



backup_task = BashOperator(
    task_id="backup_airflow_db",
    bash_command="pg_dump -U airflow -h localhost -p 5432 -d airflow_db -F c -f /tmp/airflowdb_bkp.dump",
    dag=dag,
)

upload_s3 = LocalFilesystemToS3Operator(
    task_id="upload_backup_s3",
    filename='/tmp/airflowdb_bkp.dump',
    dest_key=f'db_backup/airflow_backup_{datetime.now().strftime("%Y%m%d%H%M%S")}.dump',
    dest_bucket='cf-airflow-source-bucket',
    replace=True,
    dag=dag,
)

delete_old_backup = S3DeleteObjectsOperator(
    task_id="delete_old_backup",
    bucket='cf-airflow-source-bucket',
    prefix=f'db_backup/airflow_backup_{previous_month}'
)

# Set task dependencies
backup_task >> upload_s3 >> delete_old_backup
