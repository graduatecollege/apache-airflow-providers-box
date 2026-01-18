from datetime import datetime, timedelta
from typing import Callable, Any

import pendulum
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context
from airflow.utils.timezone import parse, convert_to_utc

from box_airflow_provider.hooks.box import BoxHook
from box_airflow_provider.triggers.box import BoxTrigger


class BoxSensor(BaseSensorOperator):
    """
    Waits for a file or folder to be available in Box.

    :param path: Box file or folder path
    :param file_pattern: The pattern that will be used to match the file (fnmatch format)
    :param box_conn_id: The connection to run the sensor against
    :param newer_than: DateTime for which the file or file path should be newer than, comparison is inclusive
    :param deferrable: If waiting for completion, whether to defer the task until done, default is ``False``.
    """

    template_fields = [
        "path",
        "newer_than",
    ]

    def __init__(
            self,
            *,
            box_conn_id: str = BoxHook.default_conn_name,
            path: str,
            file_pattern: str = "",
            newer_than: datetime | str | None = None,
            hook: BoxHook | None = None,
            python_callable: Callable | None = None,
            op_args: list | None = None,
            op_kwargs: dict[str, any] | None = None,
            deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_pattern = file_pattern
        self.hook: BoxHook | None = hook
        self.box_conn_id = box_conn_id
        self.newer_than: datetime | str | None = newer_than
        self.python_callable: Callable | None = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.deferrable = deferrable

    def poke(self, context: Context) -> PokeReturnValue | bool:
        if self.hook is None:
            self.hook = BoxHook(self.box_conn_id)
        self.log.debug("Poking for %s, with pattern %s", self.path, self.file_pattern)
        files_found = []

        if self.file_pattern:
            files_from_pattern = self.hook.get_files_by_pattern(self.path, self.file_pattern)
            if files_from_pattern:
                actual_files_to_check = files_from_pattern
            else:
                return False
        else:
            actual_files_to_check = [self.hook.get_file_info(self.path)]

        for file_info in actual_files_to_check:
            if not file_info:
                continue

            mod_time = file_info.modified_at

            if self.newer_than and self.newer_than != "None":
                if isinstance(self.newer_than, str):
                    _newer_than = parse(self.newer_than)
                elif isinstance(self.newer_than, datetime):
                    _newer_than = pendulum.instance(self.newer_than)
                else:
                    raise ValueError(f"Invalid newer_than value: {self.newer_than}")

                if isinstance(mod_time, str):
                    _mod_time = parse(mod_time)
                else:
                    _mod_time = pendulum.instance(mod_time)


                if _newer_than <= _mod_time:
                    files_found.append(file_info)
                    self.log.info(
                        "File %s has modification time: '%s', which is newer than: '%s'",
                        file_info.path,
                        str(_mod_time),
                        str(_newer_than),
                    )
                else:
                    self.log.info(
                        "File %s has modification time: '%s', which is older than: '%s'",
                        file_info.path,
                        str(_mod_time),
                        str(_newer_than),
                    )
            else:
                files_found.append(file_info)

        if not len(files_found):
            return False

        if self.python_callable is not None:
            if self.op_kwargs:
                self.op_kwargs["files_found"] = files_found
            callable_return = self.python_callable(*self.op_args, **self.op_kwargs)
            return PokeReturnValue(
                is_done=True,
                xcom_value={"files_found": files_found, "decorator_return_value": callable_return},
            )
        return True

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=BoxTrigger(
                    path=self.path,
                    file_pattern=self.file_pattern,
                    box_conn_id=self.box_conn_id,
                    poke_interval=self.poke_interval,
                    newer_than=self.newer_than,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event["status"] == "success":
            self.log.info(event["message"])
            return event["files_sensed"]
        else:
            raise AirflowException(event["message"])
