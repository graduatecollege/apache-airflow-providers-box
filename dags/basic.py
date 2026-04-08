from datetime import timedelta

import pendulum

from airflow.sdk import dag, task

from box_airflow_provider.hooks.box import BoxHook
from box_airflow_provider.operators.box import BoxDownloadOperator, BoxUploadOperator
from box_airflow_provider.sensors.box import BoxSensor


@dag(
    start_date=pendulum.datetime(2025, 4, 13, 8, 0, tz="America/Chicago"),
    schedule="@hourly",
    catchup=False,
    render_template_as_native_obj=True
)
def basic():
    """
    A basic DAG to demonstrate Box operators and sensors.
    """

    # Sensor to wait for a file to be updated
    wait_for_update = BoxSensor(
        task_id="wait_for_update",
        box_conn_id="box",

        # Path to the folder or file to wait for
        path="/Grad Automation/GradCollegeTicketStatusReport.xlsx",
        # If providing a folder in `path`, use file_pattern to match files
        #file_pattern="*.xlsx",

        # Datetime to compare against the file's last modified time. This can
        # be a string that pendulum can parse, or a datetime object.
        newer_than='{{ dag_run.logical_date + macros.timedelta(hours=-1) }}',

        deferrable=True,
        timeout=timedelta(hours=1),
    )

    @task()
    def extract():
        """
        BoxHook can also be used directly to extract data from Box.
        """
        box_hook = BoxHook(box_conn_id="box")
        response = box_hook.get_file_id("/Grad Automation/GradCollegeTicketStatusReport.xlsx")
        return response

    @task()
    def last_modified():
        """
        Here we use BoxHook to get the last modified time of a file.
        """
        box_hook = BoxHook(box_conn_id="box")
        response = box_hook.get_file_modified_time("/Grad Automation/GradCollegeTicketStatusReport.xlsx")

        return response[1]

    # Download a file from Box
    down = BoxDownloadOperator(
        task_id="download",
        box_conn_id="box",
        local_path="./GradCollegeTicketStatusReport.xlsx",
        box_path="/Grad Automation/GradCollegeTicketStatusReport.xlsx",
    )

    # Upload a file to Box. This will update the file if it already exists.
    upload_to_box = BoxUploadOperator(
        task_id="upload_to_box",
        local_path="./GradCollegeTicketStatusReport.xlsx",
        box_path="/Grad Automation/Test/SummerWritingLab/Faculty_DGS_EO_Contact_Emails_{{ ds }}.csv",
        box_conn_id="box"
    )

    wait_for_update >> extract() >> last_modified() >> down >> upload_to_box

basic_dag = basic()