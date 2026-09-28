"""优雅关停与重启恢复：

* 关停中拒绝新提交；
* 在途任务被收尾为 RETRY_WAIT（run_after_shutdown=1，追加中断历史），
  退避等待中的任务保持 RETRY_WAIT，均不丢失、不错乱；
* 进程被 kill -9 残留的 RUNNING，重启后 recover_on_startup 回收继续；
* 回收任务最终可以成功，attempts 单调递增。
"""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
import time

from jobqueue import handlers
from jobqueue.models import JobStatus, RetryPolicy
from jobqueue.queue import JobQueue, QueueShuttingDown
from jobqueue.storage import JobStore

logger = logging.getLogger("test.shutdown")


def _temp_db_path() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)  # JobStore 自行创建文件
    return path


def _cleanup_db(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        if os.path.exists(p):
            os.unlink(p)


def test_shutdown_drains_running_and_requeues_waiting(harness_factory, fast_policy):
    h = harness_factory(shutdown_timeout=5)
    running_job, _ = h.submit(
        "shut-running", "slow", {"seconds": 30},
        policy=RetryPolicy(max_attempts=3, attempt_timeout=60),
    )
    waiting_job, _ = h.submit(
        "shut-waiting", "fail_then_succeed", {"key": "shut-w", "fail_times": 99},
        policy=RetryPolicy(max_attempts=5, backoff_base=100, backoff_max=100),
    )
    # 等一个进 RUNNING、一个退避到 RETRY_WAIT
    deadline = time.time() + 2
    while time.time() < deadline:
        r = h.get(running_job.id)
        w = h.get(waiting_job.id)
        if r.status is JobStatus.RUNNING and w.status is JobStatus.RETRY_WAIT:
            break
        time.sleep(0.01)
    assert h.get(running_job.id).status is JobStatus.RUNNING
    assert h.get(waiting_job.id).status is JobStatus.RETRY_WAIT

    t0 = time.time()
    h.loop.run_until_complete(h.queue.stop())
    elapsed = time.time() - t0

    r = h.store.get_job(running_job.id)
    w = h.store.get_job(waiting_job.id)
    logger.info(
        "关停收尾判定：耗时=%.3fs；在途 job_id=%s -> status=%s attempts=%d run_after_shutdown=%s"
        " 历史末态=%s；等待 job_id=%s -> status=%s",
        elapsed, r.id, r.status.value, r.attempts, r.run_after_shutdown,
        r.history[-1].outcome.value, w.id, w.status.value,
    )
    assert elapsed < 2, "关停不应等待长任务自然结束"
    assert r.status is JobStatus.RETRY_WAIT and r.run_after_shutdown
    assert r.attempts == 1 and r.history[-1].outcome.value == "SHUTDOWN_INTERRUPTED"
    assert w.status is JobStatus.RETRY_WAIT  # 等待态任务原样保留，不丢失
    h.queue._accepting = False  # 阻止 fixture 再次 stop  # noqa: SLF001


def test_submit_rejected_during_shutdown(harness_factory):
    h = harness_factory()
    job, _ = h.submit("shut-block", "slow", {"seconds": 5},
                      policy=RetryPolicy(max_attempts=1, attempt_timeout=30))

    async def wait_running() -> None:
        deadline = time.time() + 2
        while time.time() < deadline:
            if (await h.queue.get_status(job.id)).status is JobStatus.RUNNING:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("任务未进入 RUNNING")

    h.run(wait_running())

    async def scenario() -> None:
        stop_task = asyncio.ensure_future(h.queue.stop())
        await asyncio.sleep(0)  # 让关停流程启动
        try:
            await h.queue.submit("late-key", "succeed", {})
            raise AssertionError("关停期间提交必须被拒绝")
        except QueueShuttingDown as exc:
            logger.info("关停期提交判定：business_key=late-key 被拒绝，原因=%s", exc)
        await asyncio.wait_for(stop_task, timeout=3)

    h.run(scenario())
    # 被拒的任务绝不能悄悄落库
    from jobqueue.models import JobNotFoundError
    try:
        h.store.get_job_by_business_key("late-key")
        raise AssertionError("被拒任务不应落库")
    except JobNotFoundError:
        pass
    h.queue._accepting = False  # noqa: SLF001


def test_pending_survives_restart():
    """文件 DB + 子进程模拟 kill -9：残留 RUNNING 在重启后被恢复并继续执行。

    分三个进程阶段，最贴近真实部署：
      阶段 A（子进程）：启动队列、提交慢任务、置 RUNNING 后 os._exit 硬退出；
      阶段 B（本进程）：打开同一 DB，确认现场确为 RUNNING 残留；
      阶段 C（本进程）：新队列启动应 recover 并最终执行成功，attempts 单调递增。
    """
    path = _temp_db_path()
    try:
        code = f"""
import asyncio, os, sys, time
sys.path.insert(0, {os.getcwd()!r})
from jobqueue.storage import JobStore
from jobqueue.queue import JobQueue
from jobqueue import handlers
from jobqueue.models import RetryPolicy

async def main():
    store = JobStore({path!r})
    q = JobQueue(store, handlers.default_registry(), max_workers=1, poll_interval=0.005)
    await q.start()
    job, _ = await q.submit("restart-running", "slow", {{}},
                            policy=RetryPolicy(max_attempts=3, attempt_timeout=60))
    await asyncio.sleep(0.05)
    # 硬置 RUNNING 后立即 os._exit：不执行任何收尾，等价于 kill -9
    store._conn.execute(
        "UPDATE jobs SET status='RUNNING', attempts=1, updated_at=?", (time.time(),))
    store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert store.get_job(job.id).status.value == "RUNNING"
    sys.stdout.write(job.id)
    sys.stdout.flush()
    os._exit(0)

asyncio.run(main())
"""
        import subprocess
        import sys

        proc = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=15,
        )
        assert proc.returncode == 0, proc.stderr
        run_job_id = proc.stdout.strip()
        assert run_job_id, proc.stderr

        # 阶段 B：进程已死，DB 里必须仍是 RUNNING
        dead_store = JobStore(path)
        leftover = dead_store.get_job(run_job_id)
        assert leftover.status is JobStatus.RUNNING and leftover.attempts == 1
        dead_store.close()

        # 阶段 C：重启恢复
        store2 = JobStore(path)
        reg = handlers.default_registry()

        async def quick(payload: dict) -> dict:  # noqa: ANN001
            return {"recovered": True}

        reg.register("slow", quick)
        import asyncio as _aio
        q2 = JobQueue(store2, reg, max_workers=2, poll_interval=0.005)
        loop2 = _aio.new_event_loop()
        loop2.run_until_complete(q2.start())  # 内部执行 recover_on_startup

        recovered_job = store2.get_job(run_job_id)
        logger.info("启动恢复判定：残留任务恢复后 status=%s attempts=%d 历史=%s",
                    recovered_job.status.value, recovered_job.attempts,
                    [a.outcome.value for a in recovered_job.history])
        # 可能已被 dispatcher 抢占为 RUNNING，但必须带上中断历史，证明
        # 经过了 recover_on_startup 的回收，而不是把旧 RUNNING 当新任务。
        assert any(a.outcome.value == "SHUTDOWN_INTERRUPTED"
                   for a in recovered_job.history)

        async def await_terminal(job_id: str):
            deadline = time.time() + 3
            while time.time() < deadline:
                job = await q2.get_status(job_id)
                if job.status in (JobStatus.SUCCEEDED, JobStatus.DEAD):
                    return job
                await asyncio.sleep(0.01)
            raise AssertionError("重启后任务未终结")

        job = loop2.run_until_complete(await_terminal(run_job_id))
        logger.info(
            "重启恢复判定：残留 job_id=%s -> status=%s attempts=%d（第 1 次尝试=关停中断），"
            "历史=%s，result=%s",
            job.id, job.status.value, job.attempts,
            [a.outcome.value for a in job.history], job.result,
        )
        assert job.status is JobStatus.SUCCEEDED
        assert job.attempts == 2, "中断的第 1 次尝试保留计数，恢复后为第 2 次"
        assert job.result == {"recovered": True}
        assert [a.outcome.value for a in job.history] == [
            "SHUTDOWN_INTERRUPTED", "SUCCESS",
        ]
        loop2.run_until_complete(q2.stop())
        loop2.close()
        store2.close()
    finally:
        _cleanup_db(path)
