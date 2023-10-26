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
        s3_prefix: str = None,
        s3_key: str = None
) -> Optional[str]:
    assert (s3_prefix is None) ^ (s3_key is None)
    assert not s3_key or s3_key.endswith('.csv')
    df = get_mssql_query_as_df(mssql=mssql_hook, mssql_query=mssql_query)
    if df.empty:
        return
    with NamedTemporaryFile(mode='wb') as f:
        df.to_csv(f, index=False)
        f.flush()
        s3_key = s3_key or f'{s3_prefix}/{Path(f.name).name}.csv'
        s3_hook.load_file(filename=f.name, bucket_name=s3_bucket, key=s3_key)
        logger.info(f'Loaded data file to {s3_key}.')
    return s3_key
