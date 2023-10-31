import logging
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.decorators import task_group, task
from airflow.models import TaskInstance
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from common.outlook import FetchMessagesOperator
from common.s3 import GetS3TemporaryPrefixOperator
from common.utils.visa_ep747 import transform_func, metas

logger = logging.getLogger(__name__)

with DAG(
        dag_id='import_visa_ep747_from_outlook_mail_to_redshift',
        description='Import Visa EP747 from Outlook Mail to Redshift',
        schedule_interval=None,
        start_date=datetime(2023, 10, 1),
        catchup=False
) as dag:
    get_s3_tmp = GetS3TemporaryPrefixOperator(
        task_id='get_s3_tmp',
        s3_bucket="{{ var.value.get('s3_bucket_name') }}",
        s3_prefix='AIRFLOW/tmp'
    )

    # "Inbox" / "08 - Visa" / "Visa Financial Reconciliation"
    fetch_outlook = FetchMessagesOperator(
        task_id='fetch_outlook',
        user_id='finance@cashflows.com',
        mail_folder_id='AAMkAGNkOWI2MWZkLTk5MDAtNDZiNi05OWNlLTRmNmJkMTU2NTAzZQAuAAAAAAC6FYnvEioFRJLA7TjpwnSzAQAlRictDlWaR5BuqOl1YM - pAAAC0bTjAAA = ',
        s3_bucket="{{ var.value.get('s3_bucket_name') }}",
        s3_prefix='AIRFLOW/outlook__messages'
    )


    @task(task_id='extract_message_contents')
    def extract_message_contents(ti: TaskInstance = None):
        s3_bucket = Variable.get(key='s3_bucket_name')
        s3_hook = S3Hook()
        for key in ti.xcom_pull(task_ids='fetch_outlook'):
            with NamedTemporaryFile(mode='wb') as f_source, NamedTemporaryFile(mode='wb') as f_dest:
                s3_obj = s3_hook.get_key(bucket_name=s3_bucket, key=key)
                s3_obj.download_fileobj(Fileobj=f_source)
                f_source.flush()
                with open(f_source.name, mode='rb') as s, open(f_dest.name, mode='wb') as d:
                    lines = s.readlines()
                    d.writelines(lines)
                f_dest.flush()
                path = Path(key)
                dest_fn = path.name.replace(path.suffix, '.txt')
                s3_hook.load_file(filename=f_dest.name,
                                  key=f"s3://{s3_bucket}/{ti.xcom_pull(task_ids='get_s3_tmp')}/raw/{dest_fn}")


    transform = transform_func(
        source_bucket="{{ var.value.get('s3_bucket_name') }}",
        source_prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}/raw/",
        dest_bucket="{{ var.value.get('s3_bucket_name') }}",
        dest_prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}/transformed",
        s3_key_to_filename_func=lambda bucket, key: f's3://{bucket}/AIRFLOW/outlook__messages/{Path(key).name}'
    )


    @task_group()
    def s3_to_redshift():
        for meta in metas:
            S3ToRedshiftOperator(
                task_id=meta.redshift_table,
                s3_bucket="{{ var.value.get('s3_bucket_name') }}",
                s3_key=f"{{{{ ti.xcom_pull(task_ids='get_s3_tmp') }}}}/transformed/{meta.redshift_table}/",
                schema='analytics',
                table=meta.redshift_table,
                copy_options=[
                    'format as csv',
                    'ignoreheader as 1'
                ]
            )


    # Archiving the transformed csv files seems not useful as they have been uploaded to Redshift.

    delete_s3_tmp = S3DeleteObjectsOperator(
        task_id='delete_s3_tmp',
        bucket="{{ var.value.get('s3_bucket_name') }}",
        prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}"
    )

    get_s3_tmp >> fetch_outlook >> extract_message_contents() >> transform >> s3_to_redshift() >> delete_s3_tmp
