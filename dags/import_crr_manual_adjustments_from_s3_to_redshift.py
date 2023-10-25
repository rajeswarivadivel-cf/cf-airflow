from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from custom_operators.s3_to_redshift_pipeline import s3_to_redshift_pipeline


def transform_func(f_source_name: str, f_dest_name: str, source_key: str):
    import pandas as pd
    with open(f_source_name, mode='rb') as f_source, open(f_dest_name, mode='wb') as f_dest:
        df = pd.read_excel(f_source, dtype={
            'type': str,
            'bid': str,
            'comment': str,
            'revenue': float,
            'cos': float
        }, parse_dates=['period']).assign(created_at=datetime.now())
        df.to_csv(f_dest, sep='|', header=False, index=False, escapechar='\\')


with DAG(
        dag_id='import_crr_manual_adjustments_from_s3_to_redshift',
        description='Import CRR manual adjustments from S3 to Redshift',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False
) as dag:
    s3_to_redshift_pipeline(
        dag=dag,
        s3_bucket=Variable.get('s3_bucket_name'),
        s3_prefix='AIRFLOW/crr_manual_adjustments',
        redshift_schema='analytics',
        redshift_table='crr_manual_adjustments',
        transform_func=transform_func
    )
