from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable

from common.s3_to_redshift import s3_to_redshift_pipeline


def transform_func(f_source_name: str, f_dest_name: str, source_key: str):
    import pandas as pd
    with open(f_source_name, mode='rb') as f_source, open(f_dest_name, mode='wb') as f_dest:
        df = pd.read_excel(f_source, dtype={
            'Future Use': str,
            'NTWK': str,
            'Tax Type': str,
            'Sub Invoice': str,
            'Entity ID': str,
            'Mapping': str,
            'Settlement ID': str
        }, parse_dates=['Invoice Date']).assign(filename=Path(source_key).name)
        df.to_csv(f_dest, sep='|', header=False, index=False, escapechar='\\')


with DAG(
        dag_id='import_visa_scheme_fee_data_files_from_s3_to_redshift',
        description='Import Visa scheme fee data files from S3 to Redshift',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False
) as dag:
    s3_to_redshift_pipeline(
        dag=dag,
        s3_bucket=Variable.get('s3_bucket_name'),
        s3_prefix='AIRFLOW/visa_scheme_fee',
        wildcard_key=r'.*\.xlsx',
        redshift_schema='analytics',
        redshift_table='visa__scheme_fees',
        transform_func=transform_func
    )
