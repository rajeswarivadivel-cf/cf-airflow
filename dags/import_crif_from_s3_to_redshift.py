
from airflow import DAG
from airflow.decorators import dag,task
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook 
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email

import logging
import sys
import pandas as pd
from tempfile import NamedTemporaryFile
from datetime import datetime,timedelta
import awswrangler as wr
import boto3
import psycopg2

logger = logging.getLogger(__name__)

'''
Read file from s3 source bucket
transform and put into destination bucket
load into s3
'''

default_args = {
	'owner': 'analytics',
	'depends_on_past': False,
	'email': ['data.analytics@cashflows.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

source_bucket = Variable.get('s3_sftp_mirror')
destination_prefix = Variable.get('crif_dest_prefix')
source_prefix = Variable.get('crif_source_prefix')
destination_bucket =Variable.get('s3_bucket_name')
s3_hook = S3Hook()
s3 = boto3.client("s3")

def task_failure_alert(context):
	exception_type = context.get('exception', {}).get('type', 'UnknownError')
	exception_message = context.get('exception', {}).get('exc_message', 'Unknown error occurred')
	task_key =  {context['task_instance_key_str']}
	logger.info(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")
	subject = 'Alert : Error in CRIF daily data load job'
	html_content = """<html> <head></head> <body>
						    <p style="color: red;">An error occurred in daily CRIF data load.</p>
						    <p>Error type: {exception_type}</p>
						    <p>Error message: {exception_message}</p>
						    <p>Task has failed, task_instance_key_str: {task_key}</p>
						    <p>Please correct it and re-run the job.</p>
						</body>
						</html>"""
	formatted_html = html_content.format(
    exception_type=exception_type,
    exception_message=exception_message,
    task_key=task_key
	)
	send_email(to=['data.analytics@cashflows.com'],
			subject=subject,
			html_content=formatted_html)


@dag(
	'import_crif_from_s3_to_redshift',
	default_args=default_args,
	description='It copies daily CRIF fast onboaeding data from  SFTP (S3 bucket) to redshift',
	start_date= datetime(2023, 10, 20, 13, 30),
	schedule=None,
	tags=['s3', 'CRIF'],
	catchup=False ,
	on_failure_callback=task_failure_alert,
)
def taskflow():
	start = EmptyOperator(task_id='start')

	@task(task_id="read_transform_crif_source_file")
	def read_transform_crif_source_file():
		source_keys = wr.s3.list_objects(f's3://{source_bucket}/{source_prefix}/', suffix = '.csv')
		logger.info(source_keys)
		for i  in range(0, len(source_keys)):
			file = source_keys[i]
			filename = file.split('/')[-1]
			logger.info(f'Processing {file}.')
			df_raw = wr.s3.read_csv(file,sep = ',',header=0,index_col=False,path_suffix='.csv')
			logger.info(df_raw.head())            
			logger.info(f'Found {len(df_raw)} reports.')
			filename = file.split('/')[-1]
			df_pivot = pd.melt(df_raw, id_vars=['ApplicationID'], value_vars=['Quotation phase start','Application Initiated phase start', 'Awaiting Documents activity start','Awaiting Contract Signature activity start',
																			  'Underwriting phase start','Additional Information activity start','Underwriting Awaiting Documents activity start',
																			  'Application Closed'], var_name="activity",value_name = "activity_time")
			df_pivot = df_pivot[pd.notnull(df_pivot.activity_time)]
			df_raw = df_raw.drop(['Quotation phase start','Application Initiated phase start', 'Awaiting Documents activity start','Awaiting Contract Signature activity start',
																					  'Underwriting phase start','Additional Information activity start','Underwriting Awaiting Documents activity start',
																					  'Application Closed'], axis=1)
			df_final = df_raw.merge(df_pivot, how='inner')
			df_final['filename'] = filename
			logger.info(f'Number of rows in transformed dataframe  {len(df_final)}')
			logger.info(df_final.info())
			logger.info('Write the transformed file to transformed path')
			dtypes = {'Score': 'int', 'Monthly Card Sales': 'int', 'Average Delivery Days' : 'int'}
			wr.s3.to_csv(df = df_final, path = f's3://{destination_bucket}/{destination_prefix}/transformed_{filename}',index=False,dtype=dtypes)


	@task(task_id="write_to_redshift")
	def write_to_redshift(ti: TaskInstance = None):
		s3_hook = S3Hook()
		credentials = s3_hook.get_credentials()
		sql_queries = [
				"delete  from staging.crif_fast_onboarding_source_stg",
	   			 f"""copy staging.crif_fast_onboarding_source_stg  from  's3://{destination_bucket}/{destination_prefix}/transformed_' 
						CREDENTIALS  'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key};token={credentials.token}' 
						IGNOREHEADER 1
						DATEFORMAT 'YYYY-MM-DD'
						TIMEFORMAT 'auto'
						ACCEPTINVCHARS '^'
						TRIMBLANKS
						DELIMITER ','
						FORMAT CSV""",
				"""insert into analytics.crif_fast_onboarding_source select * from staging.crif_fast_onboarding_source_stg """
				]
		redshift_hook = RedshiftSQLHook('redshift_default')
		logger.info('connection established')
		try:
			for queries in sql_queries:
				logger.info (f'executing :  {queries}')
				redshift_hook.run(sql=queries, autocommit=True)
				cnt =redshift_hook.get_records("select count(*) from staging.crif_fast_onboarding_source_stg ")
				logger.info(f'Number of rows processed today  {cnt}' )
		except Exception as e:
			print(f"An error occurred: {e}")
			raise		
		else:
			#wr.s3.delete_objects('s3://cf-analytics-team/cf-datalake/datalake_etl/datalake')
			print('Data load is sucessful')

	end = EmptyOperator(task_id='end')

	start >> read_transform_crif_source_file() >> write_to_redshift() >> end

taskflow()	

