from __future__ import annotations

import asyncio
from asyncio import Task
from datetime import datetime, timezone

import pendulum
import pytest

from box_airflow_provider.hooks.box import BoxHook
from box_airflow_provider.triggers.box import BoxTrigger

from tests.async_test_utils import patched_asyncio_for_tests


def _hook_for(box_fake) -> BoxHook:
    return BoxHook(client_factory=box_fake.client)


async def _assert_trigger_sleeps(trigger: BoxTrigger, controller) -> None:
    gen = trigger.run()
    next_event_task: Task = asyncio.create_task(gen.__anext__())
    await controller.wait_for_sleep_calls(1)
    assert next_event_task.done() is False
    next_event_task.cancel()
    controller.release_all_sleeps()
    with pytest.raises(asyncio.CancelledError):
        await next_event_task


@pytest.mark.asyncio
async def test_run_with_pattern_no_files_continues_to_sleep(box_fake):
    box_fake.seed_folder("/data")

    trigger = BoxTrigger(path="/data", file_pattern="*.csv", poke_interval=0.01, hook=_hook_for(box_fake))

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        await _assert_trigger_sleeps(trigger, controller)


@pytest.mark.asyncio
async def test_run_with_pattern_files_dont_match_continues_to_sleep(box_fake):
    box_fake.seed_file("/data/x.txt", modified_at=pendulum.datetime(2025, 1, 1, tz="UTC"))

    trigger = BoxTrigger(path="/data", file_pattern="*.csv", poke_interval=0.01, hook=_hook_for(box_fake))

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        await _assert_trigger_sleeps(trigger, controller)


@pytest.mark.asyncio
async def test_run_with_pattern_matching_files_but_older_continues_to_sleep(box_fake):
    newer_than = pendulum.datetime(2025, 1, 1, tz="UTC")
    box_fake.seed_file("/data/f.csv", modified_at=pendulum.datetime(2024, 12, 31, 23, 59, 59, tz="UTC"))

    trigger = BoxTrigger(
        path="/data",
        file_pattern="*.csv",
        newer_than=newer_than,
        poke_interval=0.01,
        hook=_hook_for(box_fake),
    )

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        await _assert_trigger_sleeps(trigger, controller)


@pytest.mark.asyncio
async def test_run_with_pattern_matching_files_and_newer_triggers(box_fake):
    newer_than = pendulum.datetime(2025, 1, 1, tz="UTC")
    file_id = box_fake.seed_file("/data/f.csv", modified_at=pendulum.datetime(2025, 1, 1, 0, 0, 0, tz="UTC"))

    trigger = BoxTrigger(
        path="/data",
        file_pattern="*.csv",
        newer_than=newer_than,
        poke_interval=0.01,
        hook=_hook_for(box_fake),
    )

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        gen = trigger.run()
        event = await asyncio.wait_for(gen.__anext__(), timeout=1)

    assert controller.sleep_calls == 0
    assert event.payload["status"] == "success"
    assert event.payload["files_sensed"] == [("f.csv", file_id)]


@pytest.mark.asyncio
async def test_run_folder_does_not_exist_triggers_error(box_fake):
    trigger = BoxTrigger(path="/missing", file_pattern="*.csv", poke_interval=0.01, hook=_hook_for(box_fake))

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        gen = trigger.run()
        event = await asyncio.wait_for(gen.__anext__(), timeout=1)

    assert controller.sleep_calls == 0
    assert event.payload["status"] == "error"
    assert "FileNotFoundError" in event.payload["message"]


@pytest.mark.asyncio
async def test_run_initially_older_then_updated_triggers(box_fake):
    newer_than = datetime(2025, 1, 1, tzinfo=timezone.utc)
    file_id = box_fake.seed_file(
        "/data/f.csv",
        modified_at=datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    )

    trigger = BoxTrigger(
        path="/data",
        file_pattern="*.csv",
        newer_than=newer_than,
        poke_interval=0.01,
        hook=_hook_for(box_fake),
    )

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        gen = trigger.run()
        next_event_task = asyncio.create_task(gen.__anext__())

        await controller.wait_for_sleep_calls(1)
        assert next_event_task.done() is False

        box_fake.set_file_modified_at(file_id, datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

        controller.release_next_sleep()
        event = await asyncio.wait_for(next_event_task, timeout=1)

    assert event.payload["status"] == "success"
    assert event.payload["files_sensed"] == [("f.csv", file_id)]


@pytest.mark.asyncio
async def test_run_without_pattern_file_not_found_continues_to_sleep(box_fake):
    trigger = BoxTrigger(path="/data/x.txt", file_pattern="", poke_interval=0.01, hook=_hook_for(box_fake))

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        await _assert_trigger_sleeps(trigger, controller)


@pytest.mark.asyncio
async def test_run_without_pattern_file_exists_and_newer_triggers(box_fake):
    file_id = box_fake.seed_file("/data/x.txt", modified_at=pendulum.datetime(2025, 1, 1, tz="UTC"))

    trigger = BoxTrigger(
        path="/data/x.txt",
        newer_than="2025-01-01T00:00:00Z",
        poke_interval=0.01,
        hook=_hook_for(box_fake),
    )

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )
    with cm():
        gen = trigger.run()
        event = await asyncio.wait_for(gen.__anext__(), timeout=1)

    assert controller.sleep_calls == 0
    assert event.payload["status"] == "success"
    assert event.payload["files_sensed"] == [("x.txt", file_id)]
