from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from box_airflow_provider.hooks.box import BoxHook, BoxFileInfo


class BoxUploadOperator(BaseOperator):
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

        return response


class BoxDownloadOperator(BoxUploadOperator):

    def execute(self, context: Context) -> BoxFileInfo:
        hook = BoxHook(box_conn_id=self.box_conn_id)

        response = hook.download_file(self.box_path, self.local_path)

        return response
