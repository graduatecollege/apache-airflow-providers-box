# ASF Adapted from https://github.com/apache/airflow/blob/providers-sftp/4.11.1/airflow/providers/sftp/triggers/sftp.py

import asyncio
import traceback
from dataclasses import asdict
from datetime import datetime
from typing import Any, AsyncIterator

import pendulum
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.timezone import parse

from box_airflow_provider.hooks.box import BoxHook, BoxFileInfo
from box_airflow_provider.models import BoxTriggerEventData


class BoxTrigger(BaseTrigger):
    def __init__(
            self,
            path: str,
            file_pattern: str = "",
            box_conn_id: str = BoxHook.default_conn_name,
            newer_than: datetime | str | None = None,
            poke_interval: float = 60,
            hook: BoxHook | None = None,
            files_sensed: list[tuple[str, str]] | None = None,
            status: str = "",
            message: str = "",
    ) -> None:
        super().__init__()
        self.path = path
        self.file_pattern = file_pattern
        self.box_conn_id = box_conn_id
        self.newer_than = newer_than
        self.poke_interval = poke_interval
        self.hook = hook
        self.files_sensed = files_sensed
        self.status = status
        self.message = message

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BoxTrigger arguments and classpath."""
        return (
            "box_airflow_provider.triggers.box.BoxTrigger",
            {
                "path": self.path,
                "file_pattern": self.file_pattern,
                "box_conn_id": self.box_conn_id,
                "newer_than": self.newer_than,
                "poke_interval": self.poke_interval,
            },
        )

    def _create_event_data(self, status: str, message: str, files_sensed: list[tuple[str, str]] | None = None) -> BoxTriggerEventData:
        """
        Utility function to create a BoxTriggerEventData object using instance variables.

        :param status: Status of the event (e.g., "success", "error").
        :param message: Message describing the event.
        :param files_sensed: List of files sensed, if any.
        :return: A BoxTriggerEventData object.
        """
        return BoxTriggerEventData(
            status=status,
            message=message,
            newer_than=self.newer_than,
            files_sensed=files_sensed,
            path=self.path,
            file_pattern=self.file_pattern,
        )

    def yield_even_data(self, status: str, message: str, files_sensed: list[tuple[str, str]] | None = None) -> TriggerEvent:
        return TriggerEvent(asdict(self._create_event_data(status, message, files_sensed)))

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Make a series of asynchronous calls to Box API. It yields a TriggerEvent.

        - If file matching file pattern exists at the specified path, return it.
        - If file pattern was not provided, it looks directly into the specific path provided.
        - If newer_than datetime was provided, it checks the file's last modified time.
        """
        hook = self.hook or BoxHook(self.box_conn_id)
        if isinstance(self.newer_than, str):
            self.newer_than = parse(self.newer_than)
        _newer_than = pendulum.instance(self.newer_than) if self.newer_than else None

        while True:
            try:
                if self.file_pattern:
                    files_result: list[BoxFileInfo] = await asyncio.to_thread(
                        hook.get_files_by_pattern, self.path, self.file_pattern
                    )
                    if not files_result or len(files_result) == 0:
                        await asyncio.sleep(self.poke_interval)
                        continue

                    files_sensed = []
                    for file_info in files_result:
                        if _newer_than:
                            mod_time = pendulum.instance(file_info.modified_at)
                            if _newer_than <= mod_time:
                                files_sensed.append((file_info.name, file_info.object_id))
                        else:
                            files_sensed.append((file_info.name, file_info.object_id))

                    if files_sensed:
                        event_data = self._create_event_data(
                            status="success",
                            message=f"Sensed {len(files_sensed)} files: {files_sensed}",
                            files_sensed=files_sensed,
                        )
                        yield self.yield_even_data(event_data.status, event_data.message, event_data.files_sensed)
                        return
                else:
                    # Catch FileNotFoundError and sleep, other errors should propagate
                    try:
                        file_info_result: BoxFileInfo = await asyncio.to_thread(
                            hook.get_file_info, self.path
                        )
                    except FileNotFoundError:
                        await asyncio.sleep(self.poke_interval)
                        continue

                    if not file_info_result:
                        await asyncio.sleep(self.poke_interval)
                        continue

                    file_info = file_info_result
                    if _newer_than:
                        mod_time = pendulum.instance(file_info.modified_at)
                        if _newer_than <= mod_time:
                            event_data = self._create_event_data(
                                status="success",
                                message=f"Sensed file: {self.path}",
                                files_sensed=[(file_info.name, file_info.object_id)],
                            )
                            yield self.yield_even_data(event_data.status, event_data.message, event_data.files_sensed)
                            return
                    else:
                        event_data = self._create_event_data(
                            status="success",
                            message=f"Sensed file: {self.path}",
                            files_sensed=[(file_info.name, file_info.object_id)],
                        )
                        yield self.yield_even_data(event_data.status, event_data.message, event_data.files_sensed)
                        return

                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                event_data = self._create_event_data(
                    status="error",
                    message=traceback.format_exc(),
                    files_sensed=None,
                )
                yield self.yield_even_data(event_data.status, event_data.message, event_data.files_sensed)
                return
