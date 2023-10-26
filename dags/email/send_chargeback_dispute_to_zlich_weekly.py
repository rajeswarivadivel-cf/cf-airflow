from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.macros import ds_add,datetime
from airflow.utils.email import send_email
import os
import logging



logger = logging.getLogger(__name__)
logger.info("This is a log message")

default_args = {
	'owner': 'analytics',
	'depends_on_past': False,
	'email': ['data.analytics@cashflows.com'],
	'email_on_failure': True,
	'email_on_retry': True,
	'retries': 0,
	'retry_delay': timedelta(minutes=5)
}

dag = DAG(
	'send_chargeback_dispute_to_zlich_weekly',
	default_args=default_args,
	description='It sends out weekly chargeback and dispute report to Zlich',
	start_date= datetime(2023, 10, 23),
	schedule='30 8 * * 1',
	tags=['email', 'ZLICH'],
	catchup=False 
)


def execute_multiple_sql_email(hook, business, frequency, merchant_id, exec_date, to, cc, logical_date, **kwargs):
    # Establish SQL Server connection
    logging.info('execution_date is ', exec_date)
    start_date = logical_date.replace(month=1, day=1).strftime("%Y-%m-%d")
    sql_hook = MsSqlHook(mssql_conn_id=hook)
    subject = f"{business} {frequency} Dispute, Collaboration and Chargeback Report {start_date} to {exec_date}"
    html_content = f"Attached is the report for all dispute,collaboration and chargebacks for '{business}' from {start_date} to {exec_date}"
    query_folder = "/home/airflow/airflow_home/dags/scripts"
    query_map = {"{business}_allocation_report" :  "allocation",
                    "{business}_chargeback_report" : "chargeback",
                    "{business}_collaboration_report" : "collaboration"}
    all_files_name = []
    for query_file in query_map.values():
        query_path = os.path.join(query_folder, 'sql', query_file) + ".sql"
        with open(query_path, 'r') as f:
            logging.info("Executing -> {}".format(f))
            query = f.read().format(merchant_id=merchant_id,start_date=start_date)
        df = sql_hook.get_pandas_df(query,index_col=None)
        output_file = f'{business}_{query_file}_{exec_date}'
        df.to_excel(os.path.join(query_folder, 'sql_output', f'{output_file}.xlsx'), index = False)
        result = os.path.join(query_folder,'sql_output', f'{output_file}.xlsx')
        all_files_name.append(result)
        logging.info(result)
    logging.info(all_files_name)
    
    send_email(      
         to,
         subject,
         html_content,
         files=all_files_name,
         cc= cc)
    for file in all_files_name:     
        os.remove(file)
        logger.info(f"{file} has been removed successfully ")

zlich_weekly_chargeback_dispute_report = PythonOperator(task_id="zlich_weekly_chargeback_dispute_report",
                           python_callable=execute_multiple_sql_email,
                           op_kwargs={'hook': 'mssql_default',
                            'business' : 'Zlich',
                            'frequency' : 'Weekly',
                           'merchant_id' : '5950347',
                           'exec_date' : '{{ ds }}',
                           'to':  "acquirer_issues@payzilch.com", 
                           'cc' : ['malgorzata.karbal@cashflows.com']
                           },
                           dag=dag)

zlich_weekly_chargeback_dispute_report