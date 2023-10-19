from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from include.datasets import mc_file

with DAG(
    dag_id = "read_s3_mc_file",
    schedule = [mc_file],
    start_date = datetime(2023, 9, 12),
    catchup = False

):

    @task(outlets=[mc_file])
    def read_s3_file():
        with open(mc_file.uri, "r") as f:
           print(f.read())


read_s3_file()