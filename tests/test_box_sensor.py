from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from box_airflow_provider.sensors.box import BoxSensor


def _file_info(*, path: str, name: str = "f", object_id: str = "1", modified_at=None):
    return SimpleNamespace(path=path, name=name, object_id=object_id, modified_at=modified_at)


def test_poke_with_pattern_returns_false_when_no_match():
    fake_hook = MagicMock()
    fake_hook.get_files_by_pattern.return_value = []
    sensor = BoxSensor(task_id="t", path="/data", file_pattern="*.csv", deferrable=False, hook=fake_hook)

    assert sensor.poke(context={}) is False


def test_poke_with_pattern_filters_newer_than_and_callable_returns_poke_return_value():
    newer_than = "2025-01-01T00:00:00Z"
    f_old = _file_info(path="/data/a.csv", object_id="a", modified_at="2024-12-31T23:59:59Z")
    f_new = _file_info(path="/data/b.csv", object_id="b", modified_at="2025-01-01T00:00:00Z")

    py_callable = MagicMock(return_value={"ok": True})
    fake_hook = MagicMock()
    fake_hook.get_files_by_pattern.return_value = [f_old, f_new]

    sensor = BoxSensor(
        task_id="t",
        path="/data",
        file_pattern="*.csv",
        newer_than=newer_than,
        hook=fake_hook,
        python_callable=py_callable,
        op_kwargs={"x": 1},
        deferrable=False,
    )

    result = sensor.poke(context={})

    assert result.is_done is True
    assert "files_found" in result.xcom_value
    assert [f.object_id for f in result.xcom_value["files_found"]] == ["b"]
    assert result.xcom_value["decorator_return_value"] == {"ok": True}
    py_callable.assert_called_once()


def test_poke_without_pattern_returns_true_when_file_exists():
    fake_hook = MagicMock()
    fake_hook.get_file_info.return_value = _file_info(
        path="/data/x.txt", modified_at=pendulum.datetime(2025, 1, 1, tz="UTC")
    )

    sensor = BoxSensor(task_id="t", path="/data/x.txt", deferrable=False, hook=fake_hook)
    assert sensor.poke(context={}) is True


def test_poke_invalid_newer_than_type_raises():
    fake_hook = MagicMock()
    fake_hook.get_file_info.return_value = _file_info(path="/x", modified_at=datetime.now(timezone.utc))

    sensor = BoxSensor(task_id="t", path="/x", newer_than=123, deferrable=False,
                       hook=fake_hook)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Invalid newer_than value"):
        sensor.poke(context={})


def test_execute_non_deferrable_calls_base_execute():
    sensor = BoxSensor(task_id="t", path="/x", deferrable=False)
    with patch("airflow.sensors.base.BaseSensorOperator.execute") as base_execute:
        sensor.execute(context={})
    base_execute.assert_called_once()


def test_execute_defers_when_not_ready():
    sensor = BoxSensor(task_id="t", path="/x", deferrable=True, poke_interval=5)
    sensor.poke = MagicMock(return_value=False)
    sensor.defer = MagicMock()

    sensor.execute(context={})

    sensor.defer.assert_called_once()
    _, kwargs = sensor.defer.call_args
    assert kwargs["method_name"] == "execute_complete"
    trigger = kwargs["trigger"]
    assert trigger.path == "/x"
    assert trigger.poke_interval == 5


def test_execute_complete_success_returns_files_sensed():
    sensor = BoxSensor(task_id="t", path="/x", deferrable=True)
    event = {"status": "success", "message": "ok", "files_sensed": [("a", "1")]}
    assert sensor.execute_complete(context={}, event=event) == [("a", "1")]


def test_execute_complete_error_raises():
    from airflow.exceptions import AirflowException

    sensor = BoxSensor(task_id="t", path="/x", deferrable=True)
    event = {"status": "error", "message": "bad", "files_sensed": None}
    with pytest.raises(AirflowException, match="bad"):
        sensor.execute_complete(context={}, event=event)
