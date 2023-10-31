import tempfile
from pathlib import Path
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults


class GetS3TemporaryPrefixOperator(BaseOperator):
    @apply_defaults
    def __init__(self, s3_bucket: str, s3_prefix: str, **kwargs):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket  # Not using this, for the sake of standard input only.
        self.s3_prefix = s3_prefix

    def execute(self, context: Context) -> Any:
        with tempfile.TemporaryDirectory() as tmp:
            return self.s3_prefix + '/' + Path(tmp).name
