from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import datetime
from include.datasets import mc_file

with DAG(
    dag_id = "update_s3_mc_file",
    schedule = "@daily",
    start_date = datetime(2023, 9, 12),
    catchup = False

):

    @task(outlets=[mc_file])
    def update_s3_file():
        with open(mc_file.uri, "a+") as f:
            f.write("file updated")


update_s3_file()