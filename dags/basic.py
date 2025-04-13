
import json
from datetime import timedelta

import pendulum


from airflow.decorators import dag, task
from airflow.utils.helpers import chain

from box_airflow_provider.hooks.box import BoxHook
from box_airflow_provider.operators.box import BoxDownloadOperator
from box_airflow_provider.sensors.box import BoxSensor


@dag(
    start_date=pendulum.datetime(2025, 4, 13, 8, 0, tz="America/Chicago"),
    schedule="@daily",
    catchup=False,
)
def basic():

    wait_for_update = BoxSensor(
        task_id="wait_for_update",
        box_conn_id="box",
        path="/Grad Automation/GradCollegeTicketStatusReport.xlsx",
        newer_than='{{ dag_run.logical_date + macros.timedelta(hours=-12) }}',
        deferrable=True,
        timeout=timedelta(hours=1),
    )

    @task()
    def extract():
        box_hook = BoxHook(box_conn_id="box")
        response = box_hook.get_file_id("/Grad Automation/GradCollegeTicketStatusReport.xlsx")
        response.raise_if_error()
        return response.id

    @task()
    def last_modified():
        box_hook = BoxHook(box_conn_id="box")
        response = box_hook.get_file_modified_time("/Grad Automation/GradCollegeTicketStatusReport.xlsx")
        response.raise_if_error()
        return response.result

    down = BoxDownloadOperator(
        task_id="download",
        box_conn_id="box",
        local_path="./GradCollegeTicketStatusReport.xlsx",
        box_path="/Grad Automation/GradCollegeTicketStatusReport.xlsx",
    )

    wait_for_update >> extract() >> last_modified() >> down

basic_dag = basic()