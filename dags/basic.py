
import json

import pendulum


from airflow.decorators import dag, task

from box_airflow_provider.hooks.box import BoxHook


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def basic():

    @task()
    def extract():
        box_hook = BoxHook(box_conn_id="box")
        response = box_hook.get_file_id("/Grad Automation/GradCollegeTicketStatusReport.xlsx")
        response.raise_if_error()
        return response.id

    @task()
    def transform(id: str):
        box_hook = BoxHook(box_conn_id="box")
        box_hook.download_file("/Grad Automation/GradCollegeTicketStatusReport.xlsx", "./GradCollegeTicketStatusReport.xlsx")

    order_data = extract()
    transform(order_data)

basic_dag = basic()