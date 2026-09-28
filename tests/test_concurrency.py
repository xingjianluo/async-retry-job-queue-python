"""并发抢占：同一任务不会被两个执行者同时拿到；并发上限严格生效。"""
from __future__ import annotations

import asyncio
import logging
import time

from jobqueue.models import JobStatus, RetryPolicy

logger = logging.getLogger("test.concurrency")


def test_claim_is_atomic(harness_factory):
    """30 个协程同时 claim 30 个到期任务，每个任务恰好被抢占一次。"""
    h = harness_factory(max_workers=1)
    # 停掉内置 dispatcher，避免它参与抢占。
    h.queue._dispatcher.cancel()  # noqa: SLF001
    time.sleep(0.02)
    for i in range(30):
        h.submit(f"bulk-{i}", "succeed", {}, policy=RetryPolicy())

    async def _claim() -> str | None:
        async with h.queue._db_lock:  # noqa: SLF001
            job = await asyncio.to_thread(h.store.claim_due_job, time.time())
        return job.id if job else None

    async def scenario() -> tuple[int, int]:
        results = await asyncio.gather(*[_claim() for _ in range(30)])
        claimed = [r for r in results if r is not None]
        return len(claimed), len(set(claimed))

    count, unique = h.run(scenario())
    logger.info(
        "抢占原子性判定：30 个并发 claim 对 30 个到期任务 -> 抢占成功=%d，去重后=%d（必须相等）",
        count, unique,
    )
    assert count == 30 and unique == 30


def test_max_workers_limit(harness_factory, fast_policy):
    """max_workers=2 时同时处于执行期的任务数绝不超过 2；放行后全部成功。"""
    h = harness_factory(max_workers=2)

    active = 0
    max_seen = 0
    gate = asyncio.Event()
    reached_two = asyncio.Event()

    async def gated(payload: dict) -> str:  # noqa: ANN001
        nonlocal active, max_seen
        active += 1
        max_seen = max(max_seen, active)
        if active >= 2:
            reached_two.set()
        try:
            await asyncio.wait_for(gate.wait(), timeout=5)
        finally:
            active -= 1
        return "done"

    h.queue.registry.register("gated", gated)
    job_ids = []
    for i in range(6):
        job, _ = h.submit(f"gated-{i}", "gated", {}, policy=fast_policy(attempt_timeout=6))
        job_ids.append(job.id)

    async def observe() -> None:
        await asyncio.wait_for(reached_two.wait(), timeout=3)
        await asyncio.sleep(0.05)  # 给 dispatcher 留出「违规多调度」的窗口

    h.run(observe())
    logger.info(
        "并发上限判定：max_workers=2，6 个门控长任务的观测最大并发=%d（必须恰好为 2）",
        max_seen,
    )
    assert max_seen == 2, f"并发峰值不得超过 worker 上限，实际={max_seen}"

    async def release() -> None:
        gate.set()

    h.run(release())
    for job_id in job_ids:
        job = h.wait_for(job_id, {JobStatus.SUCCEEDED})
        assert job.result == "done"
