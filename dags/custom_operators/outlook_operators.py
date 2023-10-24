import logging

from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


def get_access_token() -> str:
    http = HttpHook(http_conn_id='microsoft_token', method='POST')
    response = http.run(
        endpoint='/',
        headers={
            'Content-Type': 'application/x-www-form-urlencoded'
        },
        data={
            'client_id': Variable.get('microsoft_graph_client_id'),
            'grant_type': 'refresh_token',
            'scope': ' '.join(['.default', 'offline_access']),
            'refresh_token': Variable.get('microsft_graph_refresh_token'),
            'client_secret': Variable.get('microsoft_graph_client_secret')
        }
    )
    return response.json()['access_token']


class FetchMessagesOperator(BaseOperator):
    template_fields = ('user_id', 'mail_folder_id')

    @apply_defaults
    def __init__(
            self,
            user_id: str,
            mail_folder_id: str,
            s3_bucket: str,
            s3_prefix: str,
            aws_conn_id: str = 'aws_default',
            **kwargs):
        super().__init__(**kwargs)
        self.user_id = user_id
        self.mail_folder_id = mail_folder_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        self.http = HttpHook('GET', http_conn_id='microsoft_graph')
        self.access_token = get_access_token()
        self._fetch(
            endpoint=f"/users/{self.user_id}/mailfolders(\'{self.mail_folder_id}\')/messages?$search=\"received:{context['ds']}\"")

    def _fetch(self, endpoint: str):
        # Fetch data.
        response = self.http.run(endpoint=endpoint, headers={'Authorization': f'Bearer {self.access_token}'})
        logger.info(f'Get {response.url}')
        assert response.status_code == 200

        messages_data = response.json()['value']

        # Write to S3.
        new_messages_data = []
        for message_data in messages_data:
            key = f'{self.s3_prefix}/{message_data["subject"]}_{message_data["id"]}.txt'
            self.s3.load_string(
                string_data=message_data['body']['content'].replace('\r\n', '\n'),
                bucket_name=self.s3_bucket,
                key=key
            )
            new_messages_data.append(message_data)
            logger.info(f'{key} loaded.')

        # Fetch next page.
        if '@odata.nextLink' in response.json():
            url = response.json()['@odata.nextLink']
            assert url.startswith(self.http.base_url)
            self._fetch(endpoint=url.replace(self.http.base_url, ''))
