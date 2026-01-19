from __future__ import annotations

import asyncio

import pytest

from tests.async_test_utils import patched_asyncio_for_tests


@pytest.mark.asyncio
async def test_patched_asyncio_for_tests_sleep_blocks_until_released_and_counts_calls():
    cm, controller = patched_asyncio_for_tests()

    with cm():
        sleep_task = asyncio.create_task(asyncio.sleep(0.01))

        await controller.wait_for_sleep_calls(1)
        assert controller.sleep_calls == 1
        assert sleep_task.done() is False

        controller.release_next_sleep()
        await sleep_task


@pytest.mark.asyncio
async def test_patched_asyncio_for_tests_to_thread_runs_inline():
    cm, _ = patched_asyncio_for_tests()

    with cm():
        result = await asyncio.to_thread(lambda x: x + 1, 41)
    assert result == 42
