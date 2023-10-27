import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from common.outlook import FetchMessagesOperator

logger = logging.getLogger(__name__)

with DAG(
        dag_id='import_visa_ep747_data_files_from_outlook_mail_to_s3',
        description='Import Visa EP747 data files from Outlook Mail to S3',
        schedule_interval=None,
        start_date=datetime(2023, 10, 1),
        catchup=False,
        user_defined_filters={'fromjson': lambda s: json.loads(s)}
) as dag:
    # "Inbox" / "08 - Visa" / "Visa Financial Reconciliation"
    fetch_outlook = FetchMessagesOperator(
        dag=dag,
        task_id='fetch_visa_financial_reconciliation',
        user_id='finance@cashflows.com',
        mail_folder_id='AAMkAGNkOWI2MWZkLTk5MDAtNDZiNi05OWNlLTRmNmJkMTU2NTAzZQAuAAAAAAC6FYnvEioFRJLA7TjpwnSzAQAlRictDlWaR5BuqOl1YM - pAAAC0bTjAAA = ',
        s3_bucket=Variable.get('s3_bucket_name'),
        s3_prefix='AIRFLOW/visa_ep747/staging'
    )
