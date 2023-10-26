import logging
import re

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

logger = logging.getLogger(__name__)


def get_base_table_from_mssql_query(mssql_query: str) -> str:
    # Assume first found table is base table.
    return re.findall('[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+', mssql_query)[0]


def get_mssql_query_as_df(mssql: MsSqlHook, mssql_query: str) -> 'pd.DataFrame':
    # Extract from MSSQL.
    try:
        sql = mssql_query
        logger.info(f'Executing {sql}.')
        df = mssql.get_pandas_df(sql=sql)
    except ValueError:
        # A known issue when working with datetime2 column.
        # Refer to https://github.com/pymssql/pymssql/issues/695.
        # Here is a brute force solution.
        logger.info('Encounter datetime2 issue.')
        # Get all columns in tables. Keep order.
        cols = mssql.get_pandas_df(sql=f'select * from ({mssql_query}) as t where 1 = 0').columns
        datetime2_columns = []
        # Search for columns that causes the error.
        for col in cols:
            try:
                mssql.get_pandas_df(sql=f'select {col} from ({mssql_query}) as t')
            except ValueError:
                datetime2_columns.append(col)
        # Cast datetime2 columns as varchar when running query to avoid the error.
        # Avoid casting datetime2 as datetime as it may cause precision error.
        # For example, '2017-07-25 23:59:59.999' may be converted into '2017-07-26 00:00:00.000'.
        x = ', '.join([(f'cast({col} as varchar) as {col}' if col in datetime2_columns else col) for col in cols])
        df = mssql.get_pandas_df(sql=f'select {x} from ({mssql_query}) as t')
        # Cast columns as datetime.
        from pandas import to_datetime as pd_to_datetime
        for col in datetime2_columns:
            df[col] = pd_to_datetime(df[col])

    # Convert all column names to snake case.
    df = df.rename(columns={c: c.lower() for c in df.columns})

    logger.info(f'Found {df.shape[0]} records.')

    # Avoid casting nullable integer into float.
    sql_table = get_base_table_from_mssql_query(mssql_query=mssql_query)
    database, schema, table = sql_table.split('.')
    for column_name, data_type in mssql.get_records(sql=f"""
        select
            lower(column_name), 
            lower(data_type) 
        from 
            {database}.information_schema.columns 
        where 
            table_catalog = '{database}' 
            and table_schema = '{schema}' 
            and table_name = '{table}'
    """):
        # Refer to https://learn.microsoft.com/en-us/sql/t-sql/data-types/int-bigint-smallint-and-tinyint-transact-sql?view=sql-server-ver16 for SQL Server integer types.
        if data_type in ('bigint', 'int', 'smallint', 'tinyint'):
            df[column_name] = df[column_name].astype('Int64')
        elif data_type == 'bit':
            df[column_name] = df[column_name].replace({True: 1, False: 0}).astype('Int64')
        else:
            pass

    return df
