import logging
import math
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults

from common.mssql import get_mssql_query_as_df, get_base_table_from_mssql_query

logger = logging.getLogger(__name__)


class ReloadOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            s3_bucket: str,
            s3_prefix: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def pre_execute(self, context):
        self.mssql_table = context['dag_run'].conf['mssql_table']
        self.id_column = context['dag_run'].conf.get('id_column')
        self.ts_column = context['dag_run'].conf.get('ts_column')
        self.batch_size = context['dag_run'].conf['batch_size']
        logger.info(f'mssql_table: {self.mssql_table}')
        logger.info(f'id_column: {self.id_column}')
        logger.info(f'ts_column: {self.ts_column}')
        logger.info(f'batch_size: {self.batch_size}')
        assert all(not i or isinstance(i, str) for i in [self.mssql_table, self.id_column, self.ts_column])
        assert isinstance(self.batch_size, int)
        assert sum(bool(i) for i in [self.id_column, self.ts_column]) == 1

    def execute(self, context):
        mssql_hook = MsSqlHook()
        s3_hook = S3Hook()

        if self.id_column:
            mssql_min, mssql_max = mssql_hook.get_first(
                sql=f'select min({self.id_column}), max({self.id_column}) from {self.mssql_table} as t')
            intervals = [(n, min(n + self.batch_size - 1), mssql_min) for n in
                         range(mssql_min, mssql_max, self.batch_size)]
            sql = 'select * from {mssql_table} as t where ' + self.id_column + ' between {start} and {end}'

        elif self.ts_column:
            ts_from, ts_to = mssql_hook.get_first(
                sql=f'select min({self.ts_column}), max({self.ts_column}) from {self.mssql_table} as t')
            ts_from, ts_to = ts_from.replace(minute=0, second=0, microsecond=0), ts_to.replace(minute=0, second=0,
                                                                                               microsecond=0)
            intervals = [(ts_from + timedelta(hours=n), ts_from + timedelta(hours=n + 1)) for n in
                         range(math.ceil((ts_to - ts_from).total_seconds() / 60 / 60) + 1)]
            # Avoid using BETWEEN which may result in loading boundary records twice.
            sql = 'select * from {mssql_table} as t where ' + self.ts_column + ' >= \'{start}\' and ' + self.ts_column + ' < \'{end}\''

        else:
            raise RuntimeError

        for n, (start, end) in enumerate(intervals):
            logger.info(f'Running batch {n} of {len(intervals)}.')
            df = get_mssql_query_as_df(
                mssql=mssql_hook,
                mssql_query=sql.format(mssql_table=self.mssql_table, start=start, end=end)
            )
            if df.empty:
                continue
            with NamedTemporaryFile(mode='w', encoding='utf8') as f:
                df.to_csv(f, index=False)
                f.flush()
                key = f"{self.s3_prefix}/reload/{get_base_table_from_mssql_query(mssql_query=self.mssql_table).replace('.', '_')}/{n}.csv"
                s3_hook.load_file(filename=f.name, bucket_name=self.s3_bucket, key=key)
                logger.info(f'Loaded data file to {key}.')


with DAG(
        dag_id='import_mssql_data_from_mssql_to_s3.py',
        description='Import MSSQL data from Mssql to S3.',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        doc_md='Trigger DAG with below configuration JSON,<br>'
               '```{"mssql_table": "<mssql_table>", "redshift_table": "<redshift_table>", "id_column": "<id_column>" ,"batch_size": <batch_size>}```'
) as dag:
    reload = ReloadOperator(
        dag=dag,
        task_id='reload',
        s3_bucket="{{ var.value.get('s3_bucket_name') }}",
        s3_prefix='AIRFLOW/mssql'
    )
