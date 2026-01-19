# Repository agents guide

This repository contains an Apache Airflow provider for Box (`src/box_airflow_provider`) plus a small test suite (`tests/`).

## Development setup

- Python: see `pyproject.toml`.
- Dependency/tooling manager: `pdm`.

Common commands:

```bash
pdm install
pdm run test
```

## Project layout

- Provider code: `src/box_airflow_provider/`
  - Hooks: `src/box_airflow_provider/hooks/`
  - Operators: `src/box_airflow_provider/operators/`
  - Sensors: `src/box_airflow_provider/sensors/`
  - Triggers: `src/box_airflow_provider/triggers/`
- Tests: `tests/`
  - Shared fake Box backend: `tests/conftest.py` (`FakeBoxEnvironment`, `box_env` fixture)
  - Shared async test helpers: `tests/async_test_utils.py`

## Testing guidelines

- Prefer `FakeBoxEnvironment` over mocks for Box interactions.
  - Example pattern: create `BoxHook(client_factory=box_env.client)` and seed state via `box_env.seed_folder(...)` / `box_env.seed_file(...)`.
- For async trigger/sensor tests, prefer `pytest-asyncio`:
  - Use `@pytest.mark.asyncio` and `async def` tests.
- When testing polling behavior, avoid real sleeps and real threads.
  - Use `patched_asyncio_for_tests()` from `tests/async_test_utils.py` to patch `asyncio.sleep` and `asyncio.to_thread`.

Example pattern:

```python
import asyncio

import pytest

from tests.async_test_utils import patched_asyncio_for_tests


@pytest.mark.asyncio
async def test_polling_loop_is_deterministic():
    async def run_something():
        ...

    cm, controller = patched_asyncio_for_tests(
        sleep_patch_target="box_airflow_provider.triggers.box.asyncio.sleep",
        to_thread_patch_target="box_airflow_provider.triggers.box.asyncio.to_thread",
    )

    with cm():
        # start code-under-test (it will eventually `await asyncio.sleep(...)`)
        task = asyncio.create_task(run_something())

        # wait until the code hits its first sleep
        await controller.wait_for_sleep_calls(1)

        # advance exactly one polling iteration
        controller.release_next_sleep()

        # or, to unblock all pending sleeps at once:
        # controller.release_all_sleeps()

        await task
```

## Code style & changes

- Follow the existing style and patterns in the touched module (imports, naming, formatting).
- Keep changes minimal and focused.
- Add/adjust tests for behavior changes.
- Do not weaken tests to make them pass.

## Notes

- Don't worry about checking git status or git diff for other changes, they will be
  checked by the developer.
