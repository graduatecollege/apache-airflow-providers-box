from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.context import Context

from box.hooks.box import BoxHook, BoxFileInfo


class BoxUploadOperator(BaseOperator):
    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        "local_path",
        "box_path",
    ]

    def __init__(
            self,
            *,
            local_path: str | None = None,
            box_path: str | None = None,
            box_conn_id: str = BoxHook.default_conn_name,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.box_conn_id = box_conn_id
        self.local_path = local_path
        self.box_path = box_path
        if not self.local_path:
            raise AirflowException("local_path is required")
        if not self.box_path:
            raise AirflowException("box_path is required")

    def execute(self, context: Context) -> BoxFileInfo:
        hook = BoxHook(box_conn_id=self.box_conn_id)

        response = hook.upload_file(self.local_path, self.box_path)

        if not response:
            raise AirflowException("Upload failed: " + response.error_message)

        return response.result
