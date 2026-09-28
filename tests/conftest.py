"""pytest 公共夹具。

每个用例通过 QueueHarness 在专用事件循环中操作队列；harness 在同一个循环里
完成 start / 业务操作 / stop，杜绝跨已关闭循环访问任务对象的问题。
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

import pytest

from jobqueue import handlers
from jobqueue.models import JobStatus, RetryPolicy
from jobqueue.queue import JobQueue
from jobqueue.storage import JobStore


@pytest.fixture(autouse=True)
def _reset_handler_calls() -> None:
    handlers.reset_calls()
    yield
    handlers.reset_calls()


class QueueHarness:
    """同步外观：内部持有一个事件循环与队列，所有协程在该循环内执行。"""

    def __init__(self, **kwargs: Any) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.store: JobStore = kwargs.pop("store", None) or JobStore(":memory:")
        self.queue = JobQueue(self.store, handlers.default_registry(), **kwargs)
        self.loop.run_until_complete(self.queue.start())

    def run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def submit(self, *args: Any, **kwargs: Any) -> Any:
        return self.loop.run_until_complete(self.queue.submit(*args, **kwargs))

    def get(self, job_id: str) -> Any:
        return self.loop.run_until_complete(self.queue.get_status(job_id))

    def wait_for(
        self,
        job_id: str,
        targets: set[JobStatus],
        *,
        timeout: float = 5.0,
        interval: float = 0.005,
    ) -> Any:
        async def _poll() -> Any:
            deadline = time.time() + timeout
            job = None
            while time.time() < deadline:
                job = await self.queue.get_status(job_id)
                if job.status in targets:
                    return job
                await asyncio.sleep(interval)
            current = job.status.value if job else "<no job>"
            raise AssertionError(f"等待任务 {job_id} 进入 {targets} 超时，当前={current}")

        return self.loop.run_until_complete(_poll())

    def close(self) -> None:
        if not self.loop.is_closed() and self.queue._accepting:  # noqa: SLF001
            self.loop.run_until_complete(self.queue.stop())
        if not self.loop.is_closed():
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()


@pytest.fixture
def harness_factory() -> Callable[..., QueueHarness]:
    created: list[QueueHarness] = []

    def _make(
        *,
        max_workers: int = 4,
        poll_interval: float = 0.005,
        shutdown_timeout: float = 5.0,
        store: JobStore | None = None,
    ) -> QueueHarness:
        h = QueueHarness(
            max_workers=max_workers,
            poll_interval=poll_interval,
            shutdown_timeout=shutdown_timeout,
            store=store,
        )
        created.append(h)
        return h

    yield _make

    for h in created:
        h.close()


@pytest.fixture
def fast_policy() -> Callable[..., RetryPolicy]:
    def _make(
        max_attempts: int = 3,
        attempt_timeout: float = 5.0,
        backoff_base: float = 0.01,
        backoff_factor: float = 2.0,
        backoff_max: float = 0.05,
    ) -> RetryPolicy:
        return RetryPolicy(
            max_attempts=max_attempts,
            attempt_timeout=attempt_timeout,
            backoff_base=backoff_base,
            backoff_factor=backoff_factor,
            backoff_max=backoff_max,
        )

    return _make
