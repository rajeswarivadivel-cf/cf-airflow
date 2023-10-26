import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from custom_operators.mssql import get_mssql_query_as_df

logger = logging.getLogger(__name__)


def extract_mssql_data_to_s3(
        mssql_hook: MsSqlHook,
        mssql_query: str,
        s3_hook: S3Hook,
        s3_bucket: str,
        s3_prefix: str
) -> Optional[str]:
    df = get_mssql_query_as_df(mssql=mssql_hook, mssql_query=mssql_query)
    if df.empty:
        return
    with NamedTemporaryFile(mode='wb') as f:
        df.to_csv(f, index=False)
        f.flush()
        key = f'{s3_prefix}/{Path(f.name).name}.csv'
        s3_hook.load_file(filename=f.name, bucket_name=s3_bucket, key=key)
        logger.info(f'Loaded data file to {key}.')
    return key
