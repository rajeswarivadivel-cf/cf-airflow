from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    'sql_data_extraction',
    default_args={
        'owner': 'Analytics',
        'start_date': datetime(2023, 9, 1),
        'retries': 1,
    },
    schedule=None, 
    catchup=False,
)


extract_data_task = MsSqlOperator(
    task_id='extract_data_from_sqlserver',
    mssql_conn_id='mssql_test',
    sql="""select * from analytics_dev.atm.atm_billing;""",
    dag=dag,
)



# You can add more tasks for further processing or downstream tasks here

if __name__ == "__main__":
    dag.cli()
