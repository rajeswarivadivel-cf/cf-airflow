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

dag= DAG(
	'send_chargeback_dispute_to_altercon_monthly',
	default_args=default_args,
	description='It sends out monthly chargeback and dispute report to Altercon',
	start_date= datetime(2023, 10, 1),
	schedule='30 11 1 * *',
	tags=['email', 'ALTERCON'],
	catchup=False 
)




def execute_multiple_sql_email(hook, business, frequency, merchant_id, to, cc, logical_date,data_interval_end):
    # Establish SQL Server connection
    logging.info(f'execution_date is  {logical_date}')
    from_date = logical_date.replace(month=1, day=1).strftime("%Y-%m-%d")
    logging.info(f'The script is  executed on {data_interval_end}')
    to_date = (data_interval_end-timedelta(1)).strftime("%Y-%m-%d")
    sql_hook = MsSqlHook(mssql_conn_id=hook)
    subject = f"{business} {frequency} Dispute, Collaboration and Chargeback Report {from_date} to {to_date}"
    html_content = f"Attached is the report for all dispute,collaboration and chargebacks for '{business}' from {from_date} to {to_date}"
    query_folder = "/home/airflow/airflow_home/dags/scripts"
    query_map = {"{business}_allocation_report" :  "allocation",
                    "{business}_chargeback_report" : "chargeback",
                    "{business}_collaboration_report" : "collaboration"}
    all_files_name = []
    for query_file in query_map.values():
        query_path = os.path.join(query_folder, 'sql', query_file) + ".sql"
        with open(query_path, 'r') as f:
            logging.info("Executing -> {}".format(f))
            query = f.read().format(merchant_id=merchant_id,start_date=to_date)
        df = sql_hook.get_pandas_df(query,index_col=None)
        output_file = f'{business}_{query_file}_{to_date}'
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
         cc= cc,
         conn_id='smtp_default')
    for file in all_files_name:     
        os.remove(file)
        logger.info(f"{file} has been removed successfully ")

altercon_monthly_chargeback_dispute_report = PythonOperator(task_id="altercon_monthly_chargeback_dispute_report",
                           python_callable=execute_multiple_sql_email,
                           op_kwargs={'hook': 'mssql_default',
                            'business' : 'Altercon',
                            'frequency' : 'Monthly',
                           'merchant_id' : '5951788',
                           'exec_date' : '{{ ds }}',
                           'to':   'Irina.a@rockalab.com', 
                           'cc' :  ['illia.hr@rockalab.com', 'malgorzata.karbal@cashflows.com']
                           },
                           provide_context=True,
                           dag=dag)

altercon_monthly_chargeback_dispute_report

