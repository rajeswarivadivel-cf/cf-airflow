import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.decorators import task_group, task
from airflow.models import TaskInstance, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from common.s3 import GetS3TemporaryPrefixOperator
from common.utils.visa_ep747 import transform_func, metas

logger = logging.getLogger(__name__)


def get_copy_files_task(name: str, prefix: str, patterns: List[str]):
    @task(task_id=name)
    def copy_files(ti: TaskInstance = None):
        s3_hook = S3Hook()
        for key in s3_hook.list_keys(bucket_name=Variable.get('s3_sftp_mirror'), prefix=prefix, delimiter='/'):
            if any(re.match(pattern, key) for pattern in patterns):
                logger.info(f'Copying {key}.')
                s3_hook.copy_object(
                    source_bucket_name=Variable.get(key='s3_sftp_mirror'),
                    source_bucket_key=key,
                    dest_bucket_name=Variable.get(key='s3_bucket_name'),
                    dest_bucket_key=ti.xcom_pull(task_ids='get_s3_tmp') + '/raw/' + key.replace('/', '__')
                )

    return copy_files()


with DAG(
        dag_id='import_visa_ep747_from_s3_to_redshift',
        description='Import Visa EP747 from S3 Share to Redshift',
        schedule_interval=None,
        start_date=datetime(2023, 10, 1),
        catchup=False
) as dag:
    get_s3_tmp = GetS3TemporaryPrefixOperator(
        task_id='get_s3_tmp',
        s3_bucket="{{ var.value.get('s3_bucket_name') }}",
        s3_prefix='AIRFLOW/tmp'
    )


    @task_group()
    def copy_files():
        for name, prefix, patterns in [
            ('loomis', 'loomis/upload/', [
                r'loomis/upload/EP747_[0-9]{8}.txt'
            ]),
            ('notemachine', 'notemachine/upload/', [
                r'notemachine/upload/EP747-EU-INT-[0-9]{6}.TXT',
                r'notemachine/upload/EP747-INT-[0-9]{6}.TXT'
            ])
        ]:
            get_copy_files_task(name=name, prefix=prefix, patterns=patterns)


    transform = transform_func(
        source_bucket="{{ var.value.get('s3_bucket_name') }}",
        source_prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}" + '/raw/',
        dest_bucket="{{ var.value.get('s3_bucket_name') }}",
        dest_prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}/transformed",
        s3_key_to_filename_func=lambda bucket, key: 's3://' + bucket + '/' + Path(key).name.replace('__', '/')
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


    remove_tmp = S3DeleteObjectsOperator(
        task_id='remove_tmp',
        bucket="{{ var.value.get('s3_bucket_name') }}",
        prefix="{{ ti.xcom_pull(task_ids='get_s3_tmp') }}"
    )

    get_s3_tmp >> copy_files() >> transform >> s3_to_redshift() >> remove_tmp
