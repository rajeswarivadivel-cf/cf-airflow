from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import redshift_connector
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook 
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator,S3DeleteObjectsOperator
from airflow.models import Variable
from airflow.macros import ds_add
import boto3
import logging

logger = logging.getLogger(__name__)
logger.info("This is a log message")

default_args = {
	'owner': 'analytics',
	'depends_on_past': False,
	'email': ['data.analytics@cashflows.com'],
	'email_on_failure': True,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

dag = DAG(
	'export_recap_transactions_from_redshift_to_s3',
	default_args=default_args,
	description='It copies daily transaction data from cashflows to recap sftp (s3 bucket)',
	start_date= datetime(2023, 10, 20, 13, 30),
	schedule='30 13 * * *',
	tags=['sftp', 'RECAP'],
	catchup=False 
)

bucket_name = Variable.get('s3_bucket_name')
key = Variable.get('recap_s3_key')
redshift_key = Variable.get('redshift_iam_key')

start = EmptyOperator(task_id='start', dag=dag)

@task(task_id="transfer_redshift_to_s3")
def transfer_redshift_to_s3(ds='{{ ds }}' ,**kwargs):
	logger.info(f'The bucket name is {bucket_name} and the key name is {key}')
	logger.info(f'The script execution date {ds}' )
	#p_ds = ds_add(ds, -1)
	logger.info(f'The file will be loaded for {ds}' )
	file_name = f'rpt_recap_transactions_{ds}.csv'
	redshift_hook = RedshiftSQLHook('redshift_analytics')
	sql_query = f"unload ('SELECT * from analytics.rpt_recap_daily_transactions WHERE transaction_date = {ds} ')  \
					TO 's3://{bucket_name}/{key}/{file_name}' \
					CREDENTIALS 'aws_iam_role={redshift_key}' \
					DELIMITER ',' \
					ALLOWOVERWRITE \
					HEADER \
					PARALLEL off;"
	redshift_hook.run( sql=sql_query, autocommit=True)

@task(task_id="rename_s3_file")
def rename_s3_file(ds='{{ ds }}',**kwargs):
	s3 = boto3.client('s3')
	#p_ds = ds_add(ds, -1)
	file_name = f'rpt_recap_transactions_{ds}.csv'
	copy_source = {'Bucket': bucket_name, 'Key': f'{key}/{file_name}000'}
	s3.copy_object(Bucket=bucket_name, CopySource=copy_source , Key=f'{key}/{file_name}')
	logger.info(f'Copied in the requested file format {file_name}')
	s3.delete_object(Bucket=bucket_name, Key=f'{key}/{file_name}000')
	logger.info(f'Deleted the partitoned raw file {file_name}000')

end = EmptyOperator(task_id='end', dag=dag) 


start >> transfer_redshift_to_s3 () >> rename_s3_file() >> end
