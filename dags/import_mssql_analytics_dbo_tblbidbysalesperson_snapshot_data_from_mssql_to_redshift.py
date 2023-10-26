import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from custom_operators.mssql_to_redshift import extract_mssql_data_to_s3
from custom_operators.s3_to_redshift_pipeline import s3_to_redshift_pipeline

logger = logging.getLogger(__name__)


def transform_func(f_source_name: str, f_dest_name: str, source_key: str):
    import pandas as pd
    with open(f_source_name, mode='rb') as f_source, open(f_dest_name, mode='wb') as f_dest:
        df = pd.read_csv(f_source, dtype=str).assign(loaded_at=datetime.now(), filename=Path(source_key).name)[[
            'loaded_at',
            'filename',
            'merchant_name',
            'bid',
            'partner_manager_id',
            'partner_manager_name',
            'bid_type',
            'partner_manager_name_secondary'
        ]]
        df.to_csv(f_dest, sep='|', header=False, index=False, escapechar='\\')


with DAG(
        dag_id='import_mssql_analytics_dbo_tblbidbysalesperson_snapshot_data_from_mssql_to_redshift',
        description='Import MSSQL analytics.dbo.tblbidbysalesperson data from Mssql to Redshift',
        schedule_interval='@daily',
        start_date=datetime(2021, 1, 1),
        catchup=False
) as dag:
    s3_bucket = Variable.get('s3_bucket_name')
    s3_prefix = 'AIRFLOW/mssql__analytics_dbo_tblbidbysalesperson'


    @task(task_id='extract')
    def extract(ts_nodash=None, **kwargs):
        return extract_mssql_data_to_s3(
            mssql_hook=MsSqlHook(),
            mssql_query='select * from analytics.dbo.tblbidbysalesperson;',
            s3_hook=S3Hook(),
            s3_bucket=s3_bucket,
            s3_key=f'{s3_prefix}/staging/{ts_nodash}.csv'
        )


    s3_to_redshift = s3_to_redshift_pipeline(
        dag=dag,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        wildcard_key=None,
        redshift_schema='analytics',
        redshift_table='mssql__analytics_dbo_tblbidbysalesperson_snapshots',
        transform_func=transform_func,
        pre_operator=extract
    )
