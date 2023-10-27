import json
import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.providers.http.operators.http import SimpleHttpOperator

logger = logging.getLogger(__name__)


@task(task_id='make_content')
def make_content(ti: TaskInstance = None):
    content = json.loads(ti.xcom_pull(task_ids='fetch_page'))
    x = ''.join([f"""
        <tr>
            <td><p>{dag['dag_id']}</p></td>
            <td><p>{dag['description'] or ""}</p></td>
            <td><p>{dag['schedule_interval']['value'] if dag['schedule_interval'] else ''}</p></td>
            <td><p>{"✓" if dag['is_active'] else ""}</p></td>
        </tr>
    """ for dag in json.loads(ti.xcom_pull(task_ids='fetch_dags'))['dags']])
    body_value = f'''
        <table>
            <tbody>
                <tr>
                    <th><p><strong>DAG id</strong></p></th>
                    <th><p><strong>Description</strong></p></th>
                    <th><p><strong>Schedule</strong></p></th>
                    <th><p><strong>Is active</strong></p></th>
                </tr>
                {x}
            </tbody>
        </table>
    '''
    return json.dumps({
        'id': content['id'],
        'status': 'current',
        'title': content['title'],
        'body': {
            'representation': 'storage',
            'value': re.sub(r'<table(.|\n)*</table>', body_value, content['body']['storage']['value'])
        },
        'version': {
            'number': content['version']['number'] + 1
        }
    })


with DAG(
        dag_id='export_airflow_documentation_to_confluence.py',
        description='Export Airflow documentation to Confluence.',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False
) as dag:
    fetch_dags = SimpleHttpOperator(
        task_id='fetch_dags',
        http_conn_id='airflow_api',
        endpoint='dags',
        method='GET'
    )

    fetch_page = SimpleHttpOperator(
        task_id='fetch_page',
        http_conn_id='confluence_api',
        endpoint="pages/{{ var.value.get('confluence_airflow_dags_page_id') }}?body-format=storage",
        method='GET'
    )

    upload_page = SimpleHttpOperator(
        task_id='upload_page',
        http_conn_id='confluence_api',
        endpoint="pages/{{ var.value.get('confluence_airflow_dags_page_id') }}",
        method='PUT',
        data="{{ ti.xcom_pull(task_ids='make_content') }}"
    )

    [fetch_dags, fetch_page] >> make_content() >> upload_page
