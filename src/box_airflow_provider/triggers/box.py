# ASF Adapted from https://github.com/apache/airflow/blob/providers-sftp/4.11.1/airflow/providers/sftp/triggers/sftp.py

import asyncio
from datetime import datetime
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.timezone import convert_to_utc, parse

from box_airflow_provider.hooks.box import BoxHook


class BoxTrigger(BaseTrigger):
    def __init__(
        self,
        path: str,
        file_pattern: str = "",
        box_conn_id: str = BoxHook.default_conn_name,
        newer_than: datetime | str | None = None,
        poke_interval: float = 60,
    ) -> None:
        super().__init__()
        self.path = path
        self.file_pattern = file_pattern
        self.box_conn_id = box_conn_id
        self.newer_than = newer_than
        self.poke_interval = poke_interval

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

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Make a series of asynchronous calls to Box API. It yields a TriggerEvent.

        - If file matching file pattern exists at the specified path, return it.
        - If file pattern was not provided, it looks directly into the specific path provided.
        - If newer_than datetime was provided, it checks the file's last modified time.
        """
        hook = BoxHook(self.box_conn_id)
        if isinstance(self.newer_than, str):
            self.newer_than = parse(self.newer_than)
        _newer_than = convert_to_utc(self.newer_than) if self.newer_than else None

        while True:
            try:
                if self.file_pattern:
                    files_result = hook.get_files_by_pattern(self.path, self.file_pattern)
                    if not files_result.success:
                        await asyncio.sleep(self.poke_interval)
                        continue

                    files_sensed = []
                    for file_info in files_result.result:
                        if _newer_than:
                            mod_time = convert_to_utc(parse(file_info.modified_at))
                            if _newer_than <= mod_time:
                                files_sensed.append(file_info.name)
                        else:
                            files_sensed.append(file_info.name)

                    if files_sensed:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"Sensed {len(files_sensed)} files: {files_sensed}",
                            }
                        )
                        return
                else:
                    file_info_result = hook.get_file_info(self.path)
                    if not file_info_result.success:
                        await asyncio.sleep(self.poke_interval)
                        continue

                    file_info = file_info_result.result
                    if _newer_than:
                        mod_time = convert_to_utc(parse(file_info.modified_at))
                        if _newer_than <= mod_time:
                            yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                            return
                    else:
                        yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                        return

                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
                return
