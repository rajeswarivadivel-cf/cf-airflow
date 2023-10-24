import csv
import logging
import re
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, NoReturn

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from custom_operators.outlook_operators import FetchMessagesOperator

logger = logging.getLogger(__name__)

HEADER = 'HEADER'
ERROR_HEADER = 'ERROR_HEADER'
ERRORS = 'ERRORS'
MESSAGE_DETAILS_HEADER = 'MESSAGE_DETAILS_HEADER'
MESSAGE_DETAILS = 'MESSAGE_DETAILS'


@dataclass
class Line:
    number: int
    tag: str
    text: str


@dataclass
class Error:
    error_code: str
    description: str
    element_id: str


@dataclass
class MessageDetails:
    key: str
    value: str


@dataclass
class Report:
    run_date: date
    page: int
    file_id: str
    processing_mode: str
    source_message_number: str
    source_amount: float
    mti_function_code: str
    source_currency: str
    errors: List[Error] = field(repr=False)
    message_details: List[MessageDetails] = field(repr=False)


def parse_file(filename: str) -> List[Report]:
    with open(filename) as f:
        raw_lines = [Line(number=n, tag='', text=line) for n, line in enumerate(f.readlines(), start=1)]

    # Split data into reports.
    groups = []
    group = []
    for line in raw_lines:
        if line.text.startswith('1IP857010-AA') and group:
            groups.append(group)
            group = []
        if line.text.strip():
            group.append(line)
    groups.append(group)

    # Skip first report which is caution message.
    # Skip second report which is banner report.
    # Skip last report which is summary.
    reports = []
    for group in groups[2:-1]:

        # Add tags to lines.
        lines = []
        tag = HEADER
        for line in group:
            if not line.text.strip():
                continue
            elif line.text.startswith('1IP857010-AA'):
                lines.append(Line(number=line.number, tag=HEADER, text=line.text))
                tag = HEADER
            elif re.match(r'\sERROR\s+SOURCE', line.text) or re.match(r'\sCODE\s+DESCRIPTION\s+MESSAGE #\s+ELEMENT ID',
                                                                      line.text):
                lines.append(Line(number=line.number, tag=ERROR_HEADER, text=line.text))
                tag = ERRORS
            elif re.match(r'\sMESSAGE DETAILS', line.text):
                lines.append(Line(number=line.number, tag=MESSAGE_DETAILS_HEADER, text=line.text))
                tag = MESSAGE_DETAILS
            else:
                lines.append(Line(number=line.number, tag=tag, text=line.text))

        # Parse lines.
        headers = {}
        errors = []
        message_details = []
        for line in lines:
            if line.tag == HEADER:
                s = re.sub(r':\s+', ': ', line.text)
                for i in re.split(r'\s{2,}', s):
                    if re.match(r'.+:\s+.+', i.strip()):
                        k, v = i.strip().split(': ')
                        headers[k] = v
            elif line.tag == ERRORS:
                x = re.split(r'\s{2,}', line.text.strip())
                if len(x) == 4:
                    errors.append(Error(error_code=x[0], description=x[1], element_id=x[3]))
                else:
                    d = asdict(errors[-1])
                    d['description'] = d['description'] + ' ' + line.text.strip()
                    errors[-1] = Error(**d)
            elif line.tag == MESSAGE_DETAILS:
                x = re.split(r'\s{2,}', line.text.strip())
                message_details.append(MessageDetails(key=x[0], value=x[1] if len(x) == 2 else ''))

        reports.append(Report(
            run_date=datetime.strptime(headers['RUN DATE'], '%Y/%m/%d').date(),
            page=int(headers['PAGE']),
            file_id=headers['FILE ID'],
            processing_mode=headers['PROCESSING MODE'],
            source_message_number=headers['SOURCE MESSAGE #'],
            source_amount=float(headers['SOURCE AMOUNT'].replace(',', '')),
            mti_function_code=headers['MTI-FUNCTION CODE'],
            source_currency=headers['SOURCE CURRENCY'],
            errors=errors,
            message_details=message_details
        ))

    return reports


root_prefix = 'AIRFLOW/mastercard_ip857010_aa'
PARSE_TASK_ID = 'parse'


@task(task_id=PARSE_TASK_ID)
def parse(ti: TaskInstance = None, **kwargs) -> NoReturn:
    bucket_name = Variable.get('s3_bucket_name')
    s3_hook = S3Hook()
    source_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=f'{root_prefix}/staging/')
    ti.xcom_push(key='source_keys', value=source_keys)
    dest_prefix = f'{root_prefix}/tmp/{ti.job_id}'
    ti.xcom_push(key='dest_prefix', value=dest_prefix)
    for n, source_key in enumerate(source_keys, start=1):
        with NamedTemporaryFile(mode='wb') as f_source:
            logger.info(f'Processing {source_key}.')
            source_obj = s3_hook.get_key(bucket_name=bucket_name, key=source_key)
            source_obj.download_fileobj(Fileobj=f_source)
            f_source.flush()
            reports = parse_file(filename=f_source.name)
            logger.info(f'Found {len(reports)} reports.')

        if not reports:
            continue

        def write_data_to_s3(records: List[dict], table: str) -> NoReturn:
            with NamedTemporaryFile(mode='wb') as f_dest:
                with open(f_dest.name, mode='w', encoding='utf8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
                f_dest.flush()
                dest_key = f's3://{bucket_name}/{dest_prefix}/{table}/{n}.csv'
                s3_hook.load_file(filename=f_dest.name, key=dest_key)
                logger.info(f'Created {dest_key}.')

        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        write_data_to_s3(records=[{
            'created_at': created_at,
            'filename': source_key,
            'run_date': report.run_date,
            'page': report.page,
            'file_id': report.file_id,
            'processing_mode': report.processing_mode,
            'source_message_number': report.source_message_number,
            'source_amount': report.source_amount,
            'mti_function_code': report.mti_function_code,
            'source_currency': report.source_currency
        } for report in reports], table='mastercard__ip857010_aa')

        write_data_to_s3(records=[{
            'created_at': created_at,
            'file_id': report.file_id,
            'source_message_number': report.source_message_number,
            'error_code': error.error_code,
            'description': error.description,
            'element_id': error.element_id
        } for report in reports for error in report.errors], table='mastercard__ip857010_aa_errors')

        write_data_to_s3(records=[{
            'created_at': created_at,
            'file_id': report.file_id,
            'source_message_number': report.source_message_number,
            'key': md.key,
            'value': md.value
        } for report in reports for md in report.message_details], table='mastercard__ip857010_aa_message_details')


@task(task_id='archive')
def archive(ti: TaskInstance = None, **kwargs) -> NoReturn:
    bucket_name = Variable.get('s3_bucket_name')
    source_keys = ti.xcom_pull(task_ids=PARSE_TASK_ID, key='source_keys')
    s3_hook = S3Hook()
    for key in source_keys:
        s3_hook.copy_object(
            source_bucket_name=bucket_name,
            source_bucket_key=key,
            dest_bucket_name=bucket_name,
            dest_bucket_key=f'{root_prefix}/archive/{Path(key).name}'
        )
        s3_hook.delete_objects(bucket=bucket_name, keys=key)
        logger.info(f'Archived {key}.')


with DAG(
        dag_id='import_mastercard_ip857010_aa_from_outlook_mail_to_redshift',
        description='Import Mastercard IP857010_AA from Outlook Mail to Redshift',
        schedule_interval='@daily',
        start_date=datetime(2023, 10, 1),
        catchup=True,
        max_active_runs=1
) as dag:
    # "Inbox" / "07 - MasterCard" / "MasterCard Clearing Report"
    fetch_outlook = FetchMessagesOperator(
        dag=dag,
        task_id='fetch_outlook',
        user_id='finance@cashflows.com',
        mail_folder_id='AAMkAGNkOWI2MWZkLTk5MDAtNDZiNi05OWNlLTRmNmJkMTU2NTAzZQAuAAAAAAC6FYnvEioFRJLA7TjpwnSzAQAlRictDlWaR5BuqOl1YM-pAAKBNDcaAAA=',
        s3_bucket=Variable.get('s3_bucket_name'),
        s3_prefix='AIRFLOW/mastercard_ip857010_aa/staging'
    )

    s3_to_redshift = [S3ToRedshiftOperator(
        task_id=table,
        schema='analytics',
        table=table,
        s3_bucket="{{ var.value.get('s3_bucket_name') }}",
        s3_key=f"{{{{ ti.xcom_pull(task_ids='{PARSE_TASK_ID}', key='dest_prefix') }}}}/{table}/",
        copy_options=['format as csv', 'ignoreheader as 1']
    ) for table in [
        'mastercard__ip857010_aa',
        'mastercard__ip857010_aa_errors',
        'mastercard__ip857010_aa_message_details'
    ]]

    fetch_outlook >> parse() >> s3_to_redshift >> archive()
