"""重复提交：同一业务键只能产生一个任务，返回同一标识与当前状态。"""
from __future__ import annotations

import asyncio
import logging

from jobqueue.models import JobStatus

logger = logging.getLogger("test.idempotency")


def test_duplicate_submit_returns_same_job(harness_factory, fast_policy):
    h = harness_factory()
    job1, created1 = h.submit("order-1001", "succeed", {"n": 1})
    # 任务尚未被调度时立即重复提交（不同 payload 也不能产生新任务）
    job2, created2 = h.submit("order-1001", "succeed", {"n": 999})
    logger.info(
        "幂等判定：business_key=order-1001 首次 job_id=%s created=%s；重复 job_id=%s created=%s",
        job1.id, created1, job2.id, created2,
    )
    assert created1 is True and created2 is False
    assert job1.id == job2.id, "重复提交必须返回同一个任务标识"

    done = h.wait_for(job1.id, {JobStatus.SUCCEEDED})
    assert done.result == {"ok": True, "echo": {"n": 1}}, "只应执行首次提交的载荷"

    # 成功后再重复提交：同一标识、同一终态、不再次执行
    job3, created3 = h.submit("order-1001", "succeed", {"n": 1})
    logger.info(
        "终态后重复提交判定：job_id=%s created=%s status=%s retries/attempts=%d",
        job3.id, created3, job3.status.value, job3.attempts,
    )
    assert created3 is False and job3.id == job1.id
    assert job3.status is JobStatus.SUCCEEDED and job3.attempts == 1

    other, created_other = h.submit("order-1002", "succeed", {})
    assert created_other is True and other.id != job1.id


def test_concurrent_duplicate_submits_collapse_to_one(harness_factory):
    """20 个并发提交同一业务键：UNIQUE 约束兜底，只落一个任务、只执行一次。"""
    h = harness_factory()

    async def hammer() -> list[tuple[str, bool]]:
        results = await asyncio.gather(*[
            h.queue.submit("hot-key", "succeed", {"i": i}) for i in range(20)
        ])
        return [(job.id, created) for job, created in results]

    outcomes = h.run(hammer())
    ids = {job_id for job_id, _ in outcomes}
    created_flags = [created for _, created in outcomes]
    logger.info(
        "并发抢占判定：20 个并发提交 business_key=hot-key 去重后任务数=%d，新建次数=%d，job_id=%s",
        len(ids), sum(created_flags), next(iter(ids)),
    )
    assert len(ids) == 1
    assert sum(created_flags) == 1

    job = h.wait_for(next(iter(ids)), {JobStatus.SUCCEEDED})
    assert job.attempts == 1
