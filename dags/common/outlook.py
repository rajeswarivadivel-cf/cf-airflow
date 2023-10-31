import logging
from dataclasses import dataclass, astuple
from datetime import datetime

from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

MAIL_FOLDERS = {
    'AAMkAGNkOWI2MWZkLTk5MDAtNDZiNi05OWNlLTRmNmJkMTU2NTAzZQAuAAAAAAC6FYnvEioFRJLA7TjpwnSzAQAlRictDlWaR5BuqOl1YM-pAAKBNDcaAAA=': 'MasterCard Clearing Report',
    'AAMkAGNkOWI2MWZkLTk5MDAtNDZiNi05OWNlLTRmNmJkMTU2NTAzZQAuAAAAAAC6FYnvEioFRJLA7TjpwnSzAQAlRictDlWaR5BuqOl1YM - pAAAC0bTjAAA = ': 'Visa Financial Reconciliation'
}


@dataclass
class MessageMeta:
    id: str
    received_at: datetime
    subject: str
    sender_name: str
    sender_email: str
    mailbox: str
    folder: str


def get_message_meta(message_data: dict, user_id: str, mail_folder_id: str) -> MessageMeta:
    return MessageMeta(
        id=message_data['id'],
        received_at=datetime.strptime(message_data['receivedDateTime'], '%Y-%m-%dT%H:%M:%S%fZ'),
        subject=message_data['subject'],
        sender_name=message_data['sender']['emailAddress']['name'],
        sender_email=message_data['sender']['emailAddress']['address'],
        mailbox=user_id,
        folder=MAIL_FOLDERS.get(mail_folder_id, mail_folder_id)
    )


def get_access_token() -> str:
    http_hook = HttpHook(http_conn_id='microsoft_token', method='POST')
    response = http_hook.run(
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
    template_fields = ('user_id', 'mail_folder_id', 's3_bucket', 's3_prefix')

    @apply_defaults
    def __init__(
            self,
            user_id: str,
            mail_folder_id: str,
            s3_bucket: str,
            s3_prefix: str,
            **kwargs):
        super().__init__(**kwargs)
        self.user_id = user_id
        self.mail_folder_id = mail_folder_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

    def execute(self, context):
        self.s3_hook = S3Hook()
        self.http_hook = HttpHook('GET', http_conn_id='microsoft_graph')
        self.rs_hook = RedshiftSQLHook()
        self.access_token = get_access_token()
        self.message_meta = []
        self.s3_keys = []
        self._fetch(
            endpoint=f"/users/{self.user_id}/mailfolders(\'{self.mail_folder_id}\')/messages?$search=\"received:{context['ds']}\"")
        self.rs_hook.insert_rows(
            table='analytics.outlook__messages',
            rows=[astuple(i) for i in self.message_meta],
            targets=['id', 'received_at', 'subject', 'sender_name', 'sender_email', 'mailbox', 'folder']
        )
        return self.s3_keys

    def _fetch(self, endpoint: str):
        # Fetch data.
        response = self.http_hook.run(endpoint=endpoint, headers={'Authorization': f'Bearer {self.access_token}'})
        logger.info(f'Get {response.url}')
        assert response.status_code == 200

        messages_data = response.json()['value']
        message_ids = [i['id'] for i in messages_data]

        x = ', '.join([f"'{i}'" for i in message_ids])
        exist_ids = self.rs_hook.get_pandas_df(f'select id from analytics.outlook__messages where id in ({x});')[
            'id'].tolist()

        # Write to S3.
        for message_data in messages_data:
            key = f"{self.s3_prefix}/{message_data['id']}.txt"
            self.s3_keys.append(key)
            if message_data['id'] in exist_ids:
                logger.info(f"Skip {message_data['id']}, already loaded.")
            else:
                self.message_meta.append(get_message_meta(
                    message_data=message_data,
                    user_id=self.user_id,
                    mail_folder_id=self.mail_folder_id
                ))
                self.s3_hook.load_string(
                    string_data=message_data['body']['content'].replace('\r\n', '\n'),
                    bucket_name=self.s3_bucket,
                    key=key
                )
                logger.info(f"Loaded {message_data['id']}.")

        # Fetch next page.
        if '@odata.nextLink' in response.json():
            url = response.json()['@odata.nextLink']
            assert url.startswith(self.http_hook.base_url)
            self._fetch(endpoint=url.replace(self.http_hook.base_url, ''))
