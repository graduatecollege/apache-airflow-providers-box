from __future__ import annotations

from contextlib import ExitStack, contextmanager, _GeneratorContextManager
from dataclasses import dataclass, field
from typing import Any, Callable
import asyncio
from unittest.mock import patch


@dataclass
class SleepController:
    """Controller used by tests to coordinate patched `asyncio.sleep`.

    The patched sleep will block until the test releases it.
    """

    sleep_calls: int = 0
    _waiters: list[asyncio.Future[None]] = field(default_factory=list)
    _calls_changed: asyncio.Event = field(default_factory=asyncio.Event)

    async def wait_for_sleep_calls(self, at_least: int = 1) -> None:
        while self.sleep_calls < at_least:
            self._calls_changed.clear()
            await self._calls_changed.wait()

    def release_next_sleep(self) -> None:
        if not self._waiters:
            return
        fut = self._waiters.pop(0)
        if not fut.done():
            fut.set_result(None)

    def release_all_sleeps(self) -> None:
        while self._waiters:
            self.release_next_sleep()


def patched_asyncio_for_tests(
    *,
    sleep_patch_target: str = "asyncio.sleep",
    to_thread_patch_target: str = "asyncio.to_thread",
) -> tuple[Callable[..., _GeneratorContextManager[Any, None, None]], SleepController]:
    """
    Return a contextmanager that patches `asyncio.sleep`/`asyncio.to_thread`
    and a sleep counter.

    The returned context manager can be used in a `with` block. While active:
    - `asyncio.sleep` is replaced with an awaitable that blocks until the test releases it via
      the returned `SleepController`.
    - `asyncio.to_thread` is replaced with an awaitable that calls the function synchronously.

    `sleep_patch_target` / `to_thread_patch_target` allow patching the module-under-test, e.g.
    `"box_airflow_provider.triggers.box.asyncio.sleep"`.
    """

    controller = SleepController()

    async def _patched_sleep(delay: float | int = 0, result: Any = None) -> Any:  # noqa: ARG001
        controller.sleep_calls += 1
        controller._calls_changed.set()

        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        controller._waiters.append(waiter)
        await waiter
        return result

    async def _patched_to_thread(func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    @contextmanager
    def _cm():
        with ExitStack() as stack:
            stack.enter_context(patch(sleep_patch_target, new=_patched_sleep))
            stack.enter_context(patch(to_thread_patch_target, new=_patched_to_thread))
            yield

    return _cm, controller

