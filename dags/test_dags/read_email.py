
from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.models import Variable
import pandas as pd
from airflow.utils.email import send_email
import os
#import xlsxwriter
from datetime import datetime, timedelta,date
from dateutil.relativedelta import relativedelta
from models.connections import redshift_connection

OWNER = 'Analytics'
START_DATE = datetime(2023,7,31)
EMAIL = ['data.analytics@cashflows.com']
NUM_RETRIES = 0
DAG_ID = 'test_email_redshift'
SCHEDULE = None



def execute_query(hook,file,html_content,subject,to,cc,business,**kwargs):
	end_date = str(pd.to_datetime('today').date().replace(day=1))
	last_month = pd.to_datetime('today').date().replace(day=1) - relativedelta(months=1)
	start_date = str(last_month)
	file_date =last_month.strftime("%Y%m")
	year = last_month.strftime("%Y")
	month = last_month.strftime("%m")
	conn = redshift_connection()
	print('connection established')
	path = '/home/airflow/airflow_home/scripts/'
	sql_f = os.path.join(path, 'sql', file)
	if business == 'test':
		with open(sql_f, 'r') as query:
			print("Executing -> {}".format(query))
			format_query = ((query.read()).format(year=year,month=month))
			df = pd.read_sql(sql=format_query , con=conn, index_col=None)
			print(df.head())
	elif business == 'Wine Society Limited':
		with open(sql_f, 'r') as query:
			print("Executing -> {}".format(query))
			format_query = ((query.read()).format(start_date=start_date,end_date=end_date))
			df = pd.read_sql(sql=format_query , con=conn, index_col=None)
			print(df.head())
	else:
		print("check the script file format")
	output_file = f'{business}_{file_date}'
	df.to_excel(os.path.join(path, 'sql_output', f'{output_file}.xlsx'))
	result = os.path.join(path,'sql_output', f'{output_file}.xlsx')
	print(result)
	send_email(         
		to,
		subject,
		html_content,
		files=[result],
		cc= cc
	)
	os.remove(result)
	print("%s has been removed successfully" %result)



default_args = {'owner': OWNER,
				'depended_on_past': False,
				'start_date': START_DATE,
				'email': EMAIL,
				'email_on_failure': False,
				'email_on_retry': False,
				'retries': NUM_RETRIES,
				'retry_delay': timedelta(minutes=2)}

dag = DAG(dag_id=DAG_ID,
		  default_args=default_args,
		  schedule=SCHEDULE,
		  max_active_runs=1
		  )


test_email = PythonOperator(task_id="test_email",
						   python_callable=execute_query,
						   op_kwargs={'hook': 'redshfit_test', 'file': 'test_redshift.sql',
						   'html_content' : 'Attached is the test report from airflow test server',
						   'subject' : 'Testing Airflow test cluster',
						   'to': 'rajeswari.vadivel@cashflows.com',
						   'cc' : None,
						   'business': 'test' },
						   dag=dag)


test_email