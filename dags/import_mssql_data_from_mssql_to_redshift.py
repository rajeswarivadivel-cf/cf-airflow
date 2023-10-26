import logging
import math
from datetime import date, datetime, timedelta
from time import sleep
from typing import NoReturn, List, Optional

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.decorators import apply_defaults

from common.mssql_to_s3 import extract_mssql_data_to_s3

logger = logging.getLogger(__name__)


class OperatorBase(BaseOperator):
    template_fields = ('s3_bucket',)

    @apply_defaults
    def __init__(
            self,
            mssql_table: str,
            s3_bucket: str,
            s3_prefix: str,
            redshift_schema: str,
            redshift_table: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.mssql_table = mssql_table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table

    def pre_execute(self, context):
        if self.is_skipped_task(context=context):
            raise AirflowSkipException
        self.mssql_hook = MsSqlHook()
        self.rs_hook = RedshiftSQLHook()
        self.s3_hook = S3Hook()
        self.s3_tmp_files_prefix = f"{self.s3_prefix}/tmp/{context['ti'].job_id}/{self.redshift_table}/"
        logger.info(f'Using tmp folder {self.s3_tmp_files_prefix}.')
        if self.has_s3_tmp_files:
            raise AirflowFailException
        self.insert_record_count_snapshot()

    def execute(self, context):
        raise NotImplementedError

    def post_execute(self, context, result=None):
        self.pre_s3_to_redshift()
        self.s3_to_redshift()
        self.delete_tmp_files()
        self.refresh_latest_record_counts_table()

    def is_skipped_task(self, context: dict) -> bool:
        try:
            return self.task_id != context['dag_run'].conf['select']
        except (KeyError, TypeError):
            return False

    def insert_record_count_snapshot(self, mssql_count: int = None):
        self.rs_hook.insert_rows(
            table=f'{self.redshift_schema}.mssql_table_record_counts',
            rows=[(self.mssql_table,
                   mssql_count or self.mssql_hook.get_first(f'select count_big(*) from {self.mssql_table}')[0])],
            target_fields=['table_name', 'record_count']
        )

    @property
    def s3_tmp_files_keys(self) -> List[str]:
        return self.s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_tmp_files_prefix)

    @property
    def has_s3_tmp_files(self) -> bool:
        return bool(self.s3_tmp_files_keys)

    def extract_mssql_data_to_s3(self, mssql_query: str) -> Optional[str]:
        return extract_mssql_data_to_s3(
            mssql_hook=self.mssql_hook,
            mssql_query=mssql_query,
            s3_hook=self.s3_hook,
            s3_bucket=self.s3_bucket,
            s3_prefix=self.s3_tmp_files_prefix
        )

    def pre_s3_to_redshift(self):
        pass

    def s3_to_redshift(self):
        if self.has_s3_tmp_files:
            credentials = self.s3_hook.get_credentials()
            copy_query = f"""
                COPY {self.redshift_schema}.{self.redshift_table}
                FROM 's3://{self.s3_bucket}/{self.s3_tmp_files_prefix}'
                credentials
                'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key};token={credentials.token}'
                format as csv
                ignoreheader as 1;
            """
            self.log.info('Executing COPY command...')
            self.rs_hook.run(copy_query)
            self.log.info('COPY command complete...')
        else:
            self.log.info('Skip COPY command as there is no data files.')

    def delete_tmp_files(self):
        if self.has_s3_tmp_files:
            self.s3_hook.delete_objects(bucket=self.s3_bucket, keys=self.s3_tmp_files_keys)

    def refresh_latest_record_counts_table(self, days: int = 7 * 52, sample_days: int = 14):
        if isinstance(self, FullLoadOperator):
            mssql_count = self.mssql_hook.get_first(f"select count(*) from {self.mssql_table};")[0]
            rs_count = self.rs_hook.get_first(f"select count(*) from {self.redshift_schema}.{self.redshift_table};")[0]
        elif isinstance(self, IncrementalLoadByDateOperator):
            mssql_count = self.mssql_hook.get_first(
                f"select count(*) from {self.mssql_table} where {self.date_column} >= cast(dateadd(day, {-days}, getdate()) as date);")[
                0]
            rs_count = self.rs_hook.get_first(
                f"select count(*) from {self.redshift_schema}.{self.redshift_table} where {self.date_column} >= cast(current_date - interval '{days} days' as date);")[
                0]
        elif isinstance(self, IncrementalLoadByTimestampOperator):
            rs_count, rs_max_ts = self.rs_hook.get_first(
                f"select count(*), max({self.ts_column}) from {self.redshift_schema}.{self.redshift_table} where cast({self.ts_column} as date) >= cast(current_date - interval '{days} days' as date);")
            mssql_count = self.mssql_hook.get_first(
                f"select count(*) from {self.mssql_table} where {self.ts_column} between cast(cast(dateadd(day, {-days}, getdate()) as date) as datetime) and '{rs_max_ts}';")[
                0]
        elif isinstance(self, IncrementalLoadByIdOperator):
            daily_record_count = self.rs_hook.get_first(f'''
                with 
                    t1 as (
                        select
                            table_name,
                            record_count,
                            created_at
                        from (
                            select
                                *,
                                row_number() over (partition by table_name order by created_at desc) as rank
                            from
                                {self.redshift_schema}.mssql_table_record_counts
                        )
                        where
                            rank = 1
                    ),

                    t2 as (
                        select
                            table_name,
                            record_count,
                            created_at
                        from (
                            select
                                *,
                                row_number() over (partition by table_name order by created_at desc) as rank
                            from
                                {self.redshift_schema}.mssql_table_record_counts
                            where
                                created_at <= current_date - interval '{sample_days} days'
                        )
                        where
                            rank = 1
                    )

                    select 
                        (t1.record_count - t2.record_count) / (datediff(second, t2.created_at, t1.created_at) / 60 / 24) as daily_record_count 
                    from t1 
                    left join t2 
                        on t1.table_name = t2.table_name
                    where
                        t1.table_name = \'{self.mssql_table}\'
            ''')[0]
            if not daily_record_count:
                id_start = self.rs_hook.get_first(
                    f'select min({self.id_column}) from {self.redshift_schema}.{self.redshift_table};')[0]
            else:
                id_start = self.rs_hook.get_first(
                    f'select max({self.id_column}) from {self.redshift_schema}.{self.redshift_table};')[
                               0] - daily_record_count * days
            rs_count, rs_max_id = self.rs_hook.get_first(
                f"select count(*), max({self.id_column}) from {self.redshift_schema}.{self.redshift_table} where {self.id_column} >= {id_start};")
            mssql_count = self.mssql_hook.get_first(
                f"select count(*) from {self.mssql_table} where {self.id_column} between {id_start} and {rs_max_id};")[
                0]
        else:
            return

        table = 'mssql__latest_record_counts'
        self.rs_hook.run(sql=f'delete from {self.redshift_schema}.{table} where mssql_table = \'{self.mssql_table}\';')
        self.rs_hook.insert_rows(
            table=f'{self.redshift_schema}.{table}',
            rows=[(self.mssql_table, self.redshift_table, mssql_count, rs_count, days)],
            target_fields=['mssql_table', 'redshift_table', 'mssql_count', 'redshift_count', 'days']
        )


class FullLoadOperator(OperatorBase):
    ui_color = 'GreenYellow'

    def execute(self, context) -> NoReturn:
        self.extract_mssql_data_to_s3(mssql_query=f'select * from {self.mssql_table}')

    def pre_s3_to_redshift(self):
        self.rs_hook.run(sql=f'truncate {self.redshift_schema}.{self.redshift_table};', autocommit=True)


class IncrementalLoadByDateOperator(OperatorBase):
    ui_color = 'Gold'

    @apply_defaults
    def __init__(
            self,
            date_column: str,
            date_from: date = None,
            date_to: date = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.date_column = date_column
        self.date_from = date_from
        self.date_to = date_to

    def execute(self, context):
        sql = f'select cast(max({self.date_column}) as date) from {self.mssql_table}'
        self.date_to = self.date_to or self.mssql_hook.get_first(sql=sql)[0]

        sql = f'select cast(max({self.date_column}) as date) from {self.redshift_schema}.{self.redshift_table}'
        self.date_from = self.date_from or self.rs_hook.get_first(sql=sql)[0]

        d = self.date_from
        self.delete_date_from = d
        while d <= self.date_to:
            self.extract_mssql_data_to_s3(
                mssql_query=f'select * from {self.mssql_table} where cast({self.date_column} as date) = cast(\'{d}\' as date)')
            d += timedelta(days=1)
        self.delete_date_to = d

    def pre_s3_to_redshift(self):
        self.rs_hook.run(
            sql=f'delete from {self.redshift_schema}.{self.redshift_table} where cast({self.date_column} as date) between \'{self.delete_date_from}\' and \'{self.delete_date_to}\';',
            autocommit=True)


class IncrementalLoadByTimestampOperator(OperatorBase):
    ui_color = 'Gold'

    @apply_defaults
    def __init__(
            self,
            ts_column: str,
            ts_from: datetime = None,
            ts_to: datetime = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ts_column = ts_column
        self.ts_from = ts_from
        self.ts_to = ts_to

    def execute(self, context):
        sql = f'select max({self.ts_column}), count_big(*) from {self.mssql_table}'
        ts_to, mssql_count = self.mssql_hook.get_first(sql=sql)
        ts_to = self.ts_to or ts_to

        self.insert_record_count_snapshot(mssql_count=mssql_count)

        sql = f'select max({self.ts_column}) from {self.redshift_schema}.{self.redshift_table}'
        ts_from = self.ts_from or self.rs_hook.get_first(sql=sql)[0]

        sleep(1)  # Hopefully no more records with timestamps equal to ts_will be written into SQL Server.

        ts = ts_from.replace(microsecond=0, second=0, minute=0)
        self.delete_ts_from = ts
        while ts <= ts_to:
            start_ts = ts
            end_ts = min(ts + timedelta(hours=1), ts_to)
            # Avoid using BETWEEN which may result in loading boundary records twice.
            self.extract_mssql_data_to_s3(
                mssql_query=f'select * from {self.mssql_table} where {self.ts_column} >= \'{start_ts}\' and {self.ts_column} < \'{end_ts}\'')
            ts += timedelta(hours=1)
        # Load boundary records.
        self.extract_mssql_data_to_s3(
            mssql_query=f'select * from {self.mssql_table} where {self.ts_column} = \'{end_ts}\'')
        self.delete_ts_to = end_ts

    def pre_s3_to_redshift(self):
        self.rs_hook.run(
            sql=f'delete from {self.redshift_schema}.{self.redshift_table} where {self.ts_column} between \'{self.delete_ts_from}\' and \'{self.delete_ts_to}\';',
            autocommit=True)


class IncrementalLoadByIdOperator(OperatorBase):
    ui_color = 'Gold'

    @apply_defaults
    def __init__(
            self,
            id_column: str,
            id_from: int = None,
            id_to: int = None,
            batch_size: int = 10000,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.id_column = id_column
        self.id_from = id_from
        self.id_to = id_to
        self.batch_size = batch_size

    def execute(self, context):
        sql = f'select min({self.id_column}), max({self.id_column}), count_big(*) from {self.mssql_table}'
        self.mssql_min, self.mssql_max, mssql_count = self.mssql_hook.get_first(sql=sql)

        self.insert_record_count_snapshot(mssql_count=mssql_count)

        sql = f'select min({self.id_column}), max({self.id_column}) from {self.redshift_schema}.{self.redshift_table}'
        self.rs_min, self.rs_max = self.rs_hook.get_first(sql=sql)

        self.id_to = self.id_to or self.mssql_max
        self.id_from = self.id_from or self.rs_max or self.id_to - self.batch_size + 1

        for n, id_ in enumerate(range(self.id_from, self.id_to, self.batch_size), start=1):
            start, end = id_, min(id_ + self.batch_size - 1, self.id_to)
            logger.info(f'Running batch {n} of {math.ceil((self.id_to - self.id_from) / self.batch_size)}.')
            sql = f'select * from {self.mssql_table} where {self.id_column} between {start} and {end}'
            self.extract_mssql_data_to_s3(mssql_query=sql)

    def pre_s3_to_redshift(self):
        self.rs_hook.run(
            sql=f'delete from {self.redshift_schema}.{self.redshift_table} where {self.id_column} between {self.id_from} and {self.id_to}',
            autocommit=True)


with DAG(
        dag_id='import_mssql_data_from_mssql_to_redshift',
        description='Import MSSQL data from Mssql to Redshift',
        schedule_interval='@daily',
        start_date=datetime(2021, 1, 1),
        catchup=False,
        doc_md='To run specific task only, trigger DAG with configuration ```{"select":"task_id"}```.<br /><br />One way '
               'to create a copy of SQL Server table in Redshift is by modifying the DDL into Redshift syntax and execute '
               'it. Remove foreign key from the DDL to remove the dependency of loading Redshift tables in '
               'particular order. Remove default from DDL to copy the original value from SQL Server.'
) as dag:
    for mssql_table, Operator, kwargs in [
        ('accounts.dbo.acc_account_groups', FullLoadOperator, {}),
        ('accounts.dbo.acc_accounts', IncrementalLoadByIdOperator, {'id_column': 'account_id'}),
        ('accounts.dbo.acc_acquiring_fees', IncrementalLoadByIdOperator, {'id_column': 'fee_id'}),
        ('accounts.dbo.acc_acquiring_fees_blended', IncrementalLoadByIdOperator, {'id_column': 'acq_fee_blended_id'}),
        ('accounts.dbo.acc_acquiring_fees_ic', FullLoadOperator, {}),
        ('accounts.dbo.acc_bank_payees', FullLoadOperator, {}),
        ('accounts.dbo.acc_card_types', FullLoadOperator, {}),
        ('accounts.dbo.acc_countries', FullLoadOperator, {}),
        ('accounts.dbo.acc_currencies', FullLoadOperator, {}),
        ('accounts.dbo.acc_event_types', FullLoadOperator, {}),
        ('accounts.dbo.acc_owners', FullLoadOperator, {}),
        ('accounts.dbo.acc_partners', FullLoadOperator, {}),
        ('accounts.dbo.acc_sale_classes', FullLoadOperator, {}),
        ('accounts.dbo.acc_sale_details', IncrementalLoadByIdOperator, {'id_column': 'transaction_id'}),
        ('accounts.dbo.acc_transaction_costs', IncrementalLoadByIdOperator, {'id_column': 'transaction_cost_id'}),
        ('accounts.dbo.acc_transactions', IncrementalLoadByTimestampOperator, {'ts_column': 'transaction_time'}),
        ('accounts.dbo.acc_transfers', IncrementalLoadByIdOperator, {'id_column': 'transfer_id'}),
        ('accounts.dbo.cst_designators', FullLoadOperator, {}),
        ('accounts.dbo.fx_rates', IncrementalLoadByTimestampOperator, {'ts_column': 'rate_date'}),
        ('accounts.dbo.tbltransaction', IncrementalLoadByIdOperator, {'id_column': 'principalid'}),
        ('accounts.dbo.tbltransaction3dsecure', IncrementalLoadByIdOperator, {'id_column': 'principalid'}),
        ('accounts.dbo.tbltransactioncapture', IncrementalLoadByIdOperator, {'id_column': 'transactioncaptureid'}),
        ('accounts.dbo.tbltransactioncharge', IncrementalLoadByIdOperator, {'id_column': 'transactioncaptureid'}),
        ('accounts.dbo.tbltransactionchargeschemefee', IncrementalLoadByIdOperator, {'id_column': 'tcschemefeeid'}),
        ('accounts.dbo.tbltransactioneditcriteria', IncrementalLoadByIdOperator, {'id_column': 'principalid'}),
        ('accounts.dbo.tbltransactionsearch', IncrementalLoadByIdOperator, {'id_column': 'transactionsearchid'}),
        ('accounts.dbo.tlkpproductcard', FullLoadOperator, {}),
        ('accounts.dbo.tlkpproductmapping', FullLoadOperator, {}),
        ('accounts.dbo.tlkptransactionstatus', FullLoadOperator, {}),
        ('accounts.dbo.tlkptransactiontype', FullLoadOperator, {}),
        ('analytics.dbo.tblbidbysalesperson', FullLoadOperator, {}),
        ('analytics.dbo.tblbidbysalesperson_history', FullLoadOperator, {}),
        ('analytics.dbo.tblbidbysalesperson_secondary_history', FullLoadOperator, {}),
        ('analytics.dbo.tblearlysettlementcommission', FullLoadOperator, {}),
        ('analytics.dbo.tblfinancialreport', IncrementalLoadByDateOperator, {'date_column': 'transactiondate'}),
        ('analytics.dbo.tblfirsttransactiondate', FullLoadOperator, {}),
        ('analytics.dbo.vw_bidbysalesperson_15thcutoff', FullLoadOperator, {}),
        ('analytics.dbo.vw_bidbysalesperson_secondary_15thcutoff', FullLoadOperator, {}),
        ('analytics.dbo.deprecated_vw_combinedrevenuereport', FullLoadOperator, {}),
        ('analytics.dbo.vw_partner_channel_15thcutoff', FullLoadOperator, {}),
        ('analytics_dev.dbo.tlkpnonsalefees', FullLoadOperator, {}),
        ('analytics_dev.dbo.vw_bid_mcc', FullLoadOperator, {}),
        ('analytics_dev.dbo.vw_masterbidlist', FullLoadOperator, {}),
        ('analytics_dev.dbo.vw_mastermidlist', FullLoadOperator, {}),
        ('cashiersunstaging.dbo.invoicelines', FullLoadOperator, {}),
        ('cashiersunstaging.dbo.invoices', FullLoadOperator, {}),
        ('core.dbo.acquirer_bin', FullLoadOperator, {}),
        ('core.dbo.acquirers', FullLoadOperator, {}),
        ('core.dbo.captured_refunds', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.captured_representments', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.captured_sales', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.card_types', FullLoadOperator, {}),
        ('core.dbo.cleared_refunds', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.cleared_representments', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.cleared_sales', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.clearing_files', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.clearing_reason_codes', FullLoadOperator, {}),
        ('core.dbo.clearing_reason_sub_codes', FullLoadOperator, {}),
        ('core.dbo.currencies', FullLoadOperator, {}),
        ('core.dbo.merchants', FullLoadOperator, {}),
        ('core.dbo.partners', FullLoadOperator, {}),
        ('core.dbo.refund_auth_responses', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.refunds', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.refundvoid_auth_responses', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.regionality_types', FullLoadOperator, {}),
        ('core.dbo.representment_auth_responses', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.representments', IncrementalLoadByIdOperator, {'id_column': 'id'}),
        ('core.dbo.sale_auth_responses', IncrementalLoadByTimestampOperator, {'ts_column': 'auth_timestamp'}),
        ('core.dbo.salechbks', IncrementalLoadByTimestampOperator, {'ts_column': 'date_updated'}),
        ('core.dbo.saledisputes', FullLoadOperator, {}),
        ('core.dbo.sales', IncrementalLoadByTimestampOperator, {'ts_column': 'sale_timestamp'}),
        ('core.dbo.settlement_batch_details', FullLoadOperator, {}),
        ('core.dbo.store_currency_details', FullLoadOperator, {}),
        ('core.dbo.stores', FullLoadOperator, {}),
        ('core.dbo.terminal_acquirers', FullLoadOperator, {}),
        ('core.dbo.terminals', FullLoadOperator, {}),
        ('core.dbo.tlkpewallet', FullLoadOperator, {}),
        ('core.dbo.trans_details', IncrementalLoadByTimestampOperator, {'ts_column': 'tran_time'}),
        ('core.dbo.transaction_class', FullLoadOperator, {}),
        ('core.dbo.transaction_types', FullLoadOperator, {})
    ]:
        Operator(
            dag=dag,
            task_id=mssql_table.replace('.', '_'),
            mssql_table=mssql_table,
            s3_bucket="{{ var.value.get('s3_bucket_name') }}",
            s3_prefix='AIRFLOW/mssql',
            redshift_schema='analytics',
            redshift_table=f"mssql__{mssql_table.replace('deprecated_', '').replace('.', '_')}",
            **kwargs
        )
