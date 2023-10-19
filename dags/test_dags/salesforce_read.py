from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from simple_salesforce import Salesforce
from datetime import datetime

def fetch_salesforce_data():
    # Salesforce credentials
    username = 'api.analytics@cashflows.com.uat'
    password = 'AxrTu45!wsq156'
    security_token = 'TWpps90rzAbc3fhaxAIWY2Bc'
    sf = Salesforce(username=username, password=password, security_token=security_token, domain='test')

    sf_query = """Select  Id,\
                          ParentId,\
                          Parent.name,\
                          Name,\
                          Type
                          from Account
                        where Record_Type_name__c ='Merchants'\
                        and Partner_Manager__c != '' """

    result = sf.query_all(sf_query)

    # Process or store the fetched data as needed
    for record in result['records']:
        # Process the records or store them in a target location
        pass

# Create an Airflow DAG
dag = DAG(
    'salesforce_data_fetch',
    schedule=None,  # Set the desired schedule interval
    catchup=False,  # Set to False if you don't want to backfill
    default_args={
        'start_date': datetime(2023, 9, 1),  # Start date of your DAG
        'retries': 0  # Number of retries on failure
    },
)

# Define a PythonOperator to run the data fetch function
fetch_salesforce_task = PythonOperator(
    task_id='fetch_salesforce_data',
    python_callable=fetch_salesforce_data,
    dag=dag,
)

# You can add further tasks to process or load the fetched data as needed.
