import csv
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

logger = logging.getLogger(__name__)

mastercard_s3_key = 'AIRFLOW/mastercard_ip755120'
schema = 'analytics'
table = 'mc_clearing_report_ip755120_aa'
pwd = '1234'


def transform_func(**context):
    s3_bucket = Variable.get('s3_bucket_name')
    zip_file_s3_keys = [s3_key for s3_key in context['ti'].xcom_pull(task_ids=s3_list.task_id) if
                        re.match(r'.*\.zip', s3_key)]
    logger.info(f'Found {len(zip_file_s3_keys)} S3 keys ({zip_file_s3_keys}).')

    # Create local staging folder for transformed data files.
    with TemporaryDirectory() as tmp_dir_name:
        s3_hook = S3Hook()

        # Download and extract data files from S3 to local filesystem.
        for s3_key in zip_file_s3_keys:
            # Download zip file.
            fp = os.path.join(tmp_dir_name, Path(s3_key).name)
            with open(fp, mode='wb') as f:
                s3_hook.get_key(bucket_name=s3_bucket, key=s3_key).download_fileobj(f)
            # Unzip file.
            with ZipFile(fp) as zf:
                zf.extractall(tmp_dir_name, pwd=pwd.encode('utf8'))
            # Remove zip file.
            os.remove(fp)

        # Transform local data files.
        raw_fns = os.listdir(tmp_dir_name)
        assert all(re.match(r'IP755120-AA.*\.txt', fn) for fn in raw_fns)
        transformed_fps = []
        for raw_fn in raw_fns:
            raw_fp = os.path.join(tmp_dir_name, raw_fn)
            # Parse data file.
            records = []
            with open(raw_fp, mode='r', encoding='utf8') as f:
                for n, line in enumerate(f.readlines(), start=1):
                    line = line.strip()
                    # Ignore non-detail records.
                    if re.match('^.*IP755120-AA', line):
                        continue
                    assert len(line) == 671, f'Failed processing line {n} with length {len(line)}.'
                    records.append({
                        # TODO: Add detail_record column to Redshift table.
                        # 'detail_record': line[0:1],
                        'distribution_ica': line[1:12],
                        'message_type_indicator': line[12:16],
                        'primary_account_number': line[16:22] + '*' * 10,  # De-PAN.
                        'processing_code': line[35:41],
                        'amount_transaction': int(line[41:53]),
                        'reconciliation_amount': int(line[53:65]),
                        'reconciliation_conversion_rate': line[65:73],
                        'date_and_time_local_transaction': datetime.strptime(line[73:85], '%y%m%d%H%M%S'),
                        'point_of_service_data_code': line[85:97],
                        'function_code': line[97:100],
                        'message_reason_code': line[100:104],
                        'card_acceptor_business_code': line[104:108],
                        'original_amount_transaction': line[108:120],
                        'acquirer_reference_data': line[120:143],
                        'approval_code': line[143:149],
                        'service_code': line[149:152],
                        'card_acceptor_terminal_id': line[152:160].strip(),
                        'card_acceptor_id_code': line[160:175],
                        'card_acceptor_name_and_location': line[175:258].strip(),
                        'card_acceptor_post_code': line[258:268].strip(),
                        'card_acceptor_state_province_or_region_code': line[268:271],
                        'card_acceptor_country_code': line[271:274],
                        'global_clearing_management_system_product_identifier': line[274:277],
                        'licensed_product_identifier': line[277:280],
                        'terminal_type': line[280:283],
                        'message_reversal_indicator': line[283:284],
                        'electronic_commerce_security_level_indicator': line[284:287],
                        'file_id': line[287:312],
                        'fee_type_code': line[312:314],
                        'fee_processing_code': line[314:316],
                        'fee_settlement_indicator': line[316:318],
                        'currency_code': line[318:321],
                        'amounts_transaction_on_fee': int(line[321:333]),
                        'currency_code_fee_reconciliation': line[333:336],
                        'amount_fee_reconciliation': int(line[336:348]),
                        'extended_fee_type_code': line[348:350],
                        'extended_fee_processing_code': line[350:352],
                        'extended_fee_settlement_indicator': line[352:354],
                        'extended_currency_code_fee': line[354:357],
                        'extended_interchange_amount_fee': line[357:375],
                        'extended_currency_code_fee_recon.': line[375:378],
                        'extended_interchange_amount_feerecon.': line[378:396],
                        'currency_exponents': line[396:400],
                        'currency_code_original_transaction_amount': line[400:403],
                        'card_program_identifier': line[403:406],
                        'business_service_arrangement_type_code': line[406:407],
                        'business_service_id_code': line[407:413],
                        'interchange_rate_designator': line[413:415],
                        'central_site_business_date': datetime.strptime(line[415:421], '%y%m%d').date(),
                        'business_cycle': line[421:423],
                        'card_acceptor_classification_override_ind': line[423:424].strip(),
                        'product_class_override_indicator': line[424:427].strip(),
                        'corporate_incentive_rates_apply_indicator': line[427:428],
                        'special_conditions_indicator': line[428:429],
                        'mastercard_assigned_id_override_indicator': line[429:430],
                        'alm_account_category_code': line[430:431],
                        'rate_indicator': line[431:432],
                        'masterpass_incentive_indicator': line[432:433],
                        'settlement_service_transfer_agent_id': line[433:444],
                        'settlement_service_transfer_agent_account': line[444:472].strip(),
                        'settlement_service_level_code': line[472:473].strip(),
                        'settlement_service_id_code': line[473:483],
                        'settlement_foreign_exchange_rate_class_code': line[483:484],
                        'reconciliation_date': datetime.strptime(line[484:490], '%y%m%d').date(),
                        'reconciliation_cycle': line[490:492],
                        'settlement_date': datetime.strptime(line[492:498], '%y%m%d').date(),
                        'settlement_cycle': line[498:500],
                        'transaction_currency_code': line[500:503],
                        'reconciliation_currency_code': line[503:506],
                        'additional_amount': line[506:526],
                        'trace_id': line[526:541],
                        'rcv_mbr_id': line[541:552],
                        'snd_mbr_id': line[552:563],
                        'data_record': line[563:663],
                        'digital_wallet_interchange_override_indicator': line[663:664],
                        'currency_conversion_rate': line[664:670],
                        'currency_conversion_indicator': line[670:671],
                        'filename': raw_fn
                    })
            # Write records to file.
            transformed_fp = raw_fp.replace('.txt', '.csv')
            with open(transformed_fp, mode='w', encoding='utf8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys(), delimiter='|', escapechar='\\')
                writer.writerows(records)
            transformed_fps.append(transformed_fp)
            # Remove raw file.
            os.remove(raw_fp)

        # Upload transformed data files from local to S3.
        for fp in transformed_fps:
            s3_hook.load_file(fp, bucket_name=s3_bucket, key=f'{mastercard_s3_key}/tmp/{table}/{Path(fp).name}')
        logger.info(f'Exported {len(transformed_fps)} files to {mastercard_s3_key}/tmp/{table}.')

        # Clean up.
        s3_hook.delete_objects(bucket=s3_bucket, keys=zip_file_s3_keys)


def archive_func():
    s3_bucket = Variable.get('s3_bucket_name')
    s3_hook = S3Hook()
    for s3_key in s3_hook.list_keys(bucket_name=s3_bucket, prefix=f'{mastercard_s3_key}/tmp/{table}'):
        s3_hook.copy_object(
            source_bucket_name=s3_bucket,
            source_bucket_key=s3_key,
            dest_bucket_name=s3_bucket,
            dest_bucket_key=f'{mastercard_s3_key}/archive/{Path(s3_key).name}'
        )
        s3_hook.delete_objects(bucket=s3_bucket, keys=s3_key)


dag = DAG(
    dag_id='import_mastercard_ip755120_data_files_from_s3_to_redshift',
    description='Import Mastercard IP755120 data files from S3 to Redshift',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False
)

s3_list = S3ListOperator(
    dag=dag,
    task_id='list',
    bucket="{{ var.value.get('s3_bucket_name') }}",
    prefix=f'{mastercard_s3_key}/staging'
)

transform = PythonOperator(
    dag=dag,
    task_id='transform',
    python_callable=transform_func,
    provide_context=True
)

s3_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id='s3_to_redshift',
    schema=schema,
    table=table,
    s3_bucket="{{ var.value.get('s3_bucket_name') }}",
    s3_key=f'{mastercard_s3_key}/tmp/{table}/',
    copy_options=['REMOVEQUOTES']
)

archive = PythonOperator(
    dag=dag,
    task_id='archive',
    python_callable=archive_func
)

s3_list >> transform >> s3_to_redshift >> archive
