import re
from datetime import datetime
from typing import List

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator


def convert_ansi_colour_to_html_colour(line: str) -> str:
    for n, color in [
        (31, 'red'),
        (32, 'green'),
        (33, 'orange')  # Yellow is not easy to read.
    ]:
        for old in re.findall(f'\x1b\[{n}m.*\x1b\[0m', line):
            new = old.replace(f'\x1b[{n}m', f'<span style="color:{color}">').replace('\x1b[0m', '</span>')
            line = line.replace(old, new)
    return line.replace('\x1b[0m', '').strip()


@task(task_id='make_html_lines')
def make_html_lines(ti: TaskInstance = None, **kwargs) -> List[str]:
    lines = ti.xcom_pull(task_ids='run_dbt').split('|')
    lines = [convert_ansi_colour_to_html_colour(line=line) for line in lines]
    results = {k: int(v) for k, v in [i.split('=') for i in re.findall('PASS.*', lines[-2])[0].split(' ')]}
    ti.xcom_push('test_passed', results['WARN'] == 0 and results['ERROR'] == 0)
    return lines


with DAG(
        dag_id='send_data_quality_report_to_analytics_team',
        description='Send data quality report ot analytics team.',
        schedule=None,
        start_date=datetime(2023, 10, 1),
        catchup=False,
        max_active_runs=1
) as dag:
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=' && '.join([
            'source ~/airflow_virtual/bin/activate',
            'cd ~/airflow_home/dags/dbt',
            "dbt test | tr '\n' '||'"  # BashOperator returns only the last output line.
        ])
    )

    send_email = EmailOperator(
        task_id='send_email',
        to="{{ var.value.get('admin_email') }}",
        subject="Redshift data check - {% if ti.xcom_pull(task_ids='make_html_content', key='test_passed') %}Pass{% else %}Fail{% endif %} (host: {{ conn.redshift_default.host }})",
        html_content="""
            <html>
                <head></head>
                <body>
                    <p>
                        {% for line in ti.xcom_pull(task_ids='make_html_content') %}
                            {{ line }}<br>
                        {% endfor %}
                    </p>
                </body>
            </html>
        """
    )

    run_dbt >> make_html_lines() >> send_email
