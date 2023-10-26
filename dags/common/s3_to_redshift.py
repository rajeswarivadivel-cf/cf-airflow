import logging
import re
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

logger = logging.getLogger(__name__)

TRANSFORM_TASK_ID = 'transform'


class S3FilesTransformOperator(BaseOperator):
    def __init__(
            self,
            source_bucket: str,
            source_prefix: str,
            wildcard_key: Optional[str],
            dest_bucket: str,
            dest_prefix: str,
            transform_func: Callable,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_prefix = source_prefix
        self.dest_bucket = dest_bucket
        self.dest_prefix = dest_prefix
        self.transform_func = transform_func
        self.wildcard_key = wildcard_key

    def execute(self, context):
        s3_hook = S3Hook()
        s3_keys = s3_hook.list_keys(bucket_name=self.source_bucket, prefix=self.source_prefix)
        s3_keys = [key for key in s3_keys if not key.endswith('/')]  # Skip folders.
        logger.info(f'Found {len(s3_keys)} keys.')
        if self.wildcard_key:
            s3_keys = [key for key in s3_keys if re.match(self.wildcard_key, key)]
        context['ti'].xcom_push(key='source_bucket', value=self.source_bucket)
        context['ti'].xcom_push(key='source_keys', value=s3_keys)
        for s3_key in s3_keys:
            logger.info(f'Processing \'{s3_key}\'.')
            with NamedTemporaryFile(mode='wb') as f_source, NamedTemporaryFile(mode='wb') as f_dest:
                s3_obj = s3_hook.get_key(bucket_name=self.source_bucket, key=s3_key)
                s3_obj.download_fileobj(Fileobj=f_source)
                f_source.flush()
                self.transform_func(f_source_name=f_source.name, f_dest_name=f_dest.name, source_key=s3_key)
                f_dest.flush()
                path = Path(s3_key)
                dest_fn = path.name.replace(path.suffix, '.csv')
                s3_hook.load_file(filename=f_dest.name, key=f's3://{self.dest_bucket}/{self.dest_prefix}/{dest_fn}')


class ArchiveOperator(BaseOperator):
    def __init__(
            self,
            archive_bucket: str,
            archive_prefix: str,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.archive_bucket = archive_bucket
        self.archive_prefix = archive_prefix

    def execute(self, context):
        source_bucket = context['ti'].xcom_pull(task_ids=TRANSFORM_TASK_ID, key='source_bucket')
        source_keys = context['ti'].xcom_pull(task_ids=TRANSFORM_TASK_ID, key='source_keys')
        s3_hook = S3Hook()
        for source_key in source_keys:
            s3_hook.copy_object(
                source_bucket_name=source_bucket,
                source_bucket_key=source_key,
                dest_bucket_name=self.archive_bucket,
                dest_bucket_key=f'{self.archive_prefix}/{Path(source_key).name}'
            )
            s3_hook.delete_objects(bucket=source_bucket, keys=source_key)


def s3_to_redshift_pipeline(
        dag: DAG,
        s3_bucket: str,
        s3_prefix: str,
        redshift_schema: str,
        redshift_table: str,
        transform_func: Callable,
        pre_operator: Optional[Callable] = None,
        wildcard_key: Optional[str] = None
):
    transform = S3FilesTransformOperator(
        dag=dag,
        task_id=TRANSFORM_TASK_ID,
        source_bucket=s3_bucket,
        source_prefix=f'{s3_prefix}/staging',
        wildcard_key=wildcard_key,
        dest_bucket=s3_bucket,
        dest_prefix=f'{s3_prefix}/tmp/{redshift_table}',
        transform_func=transform_func
    )

    s3_to_redshift = S3ToRedshiftOperator(
        dag=dag,
        task_id='s3_to_redshift',
        schema=redshift_schema,
        table=redshift_table,
        s3_bucket=s3_bucket,
        s3_key=f'{s3_prefix}/tmp/{redshift_table}/',
        copy_options=['REMOVEQUOTES']
    )

    remove_tmp = S3DeleteObjectsOperator(
        dag=dag,
        task_id='remove_tmp',
        bucket=s3_bucket,
        prefix=f'{s3_prefix}/tmp'
    )

    archive = ArchiveOperator(
        dag=dag,
        task_id='archive',
        archive_bucket=s3_bucket,
        archive_prefix=f'{s3_prefix}/archive'
    )

    tasks = [transform, s3_to_redshift, remove_tmp, archive]

    if pre_operator:
        tasks = [pre_operator()] + tasks

    chain(*tasks)
