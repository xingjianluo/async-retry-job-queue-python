"""优雅关停：停止接收新任务、在跑任务收尾、未完成任务不丢失/不乱状态。"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from jobqueue.models import JobStatus, QueueClosedError
from tests.conftest import testlog


def test_shutdown_rejects_new_submissions(make_queue):
    q = make_queue(concurrency=2)
    q.shutdown_blocking(timeout=3)
    with pytest.raises(QueueClosedError):
        q.submit("echo", {}, business_key="after-close")
    testlog.info(
        "closed submit job_id=- attempts=- status=- "
        "verdict=queueclosed_raises_explicit_error"
    )


def test_shutdown_drains_inflight_and_terminal_states_stick(make_queue, db_path):
    """在跑的任务被收尾到终态；未被抢占的任务保持 PENDING，重启后继续执行。"""
    from jobqueue.queue import JobQueue
    from jobqueue.storage import JobStore
    from tests.conftest import _register_all

    q = make_queue(concurrency=2)
    ids = []
    for i in range(4):
        j, _ = q.submit(
            "slow_handler", {"seconds": 0.3}, business_key=f"drain-{i}",
            timeout_seconds=5.0,
        )
        ids.append(j.id)
    time.sleep(0.15)  # 前两个进入 RUNNING，后两个仍 PENDING
    t0 = time.time()
    q.shutdown_blocking(timeout=8)
    elapsed = time.time() - t0

    after = [q.get_job(jid) for jid in ids]
    running_now = [f for f in after if f.status is JobStatus.SUCCEEDED]
    pending_now = [f for f in after if f.status is JobStatus.PENDING]
    testlog.info(
        "drain shutdown job_ids=%s statuses=%s elapsed=%.2fs "
        "verdict=inflight_finished_unclaimed_left_pending",
        ids, [f.status.value for f in after], elapsed,
    )
    assert len(running_now) == 2
    assert len(pending_now) == 2  # 停机不启动新抢占

    # 重启：未完成的任务从 SQLite 恢复并跑完，状态/结果不乱
    store2 = JobStore(db_path)
    q2 = JobQueue(store2, max_concurrency=2, default_timeout_seconds=10,
                  poll_interval=0.01)
    _register_all(q2)
    q2.start()
    finals = [q2.wait_for_terminal(jid, timeout=10) for jid in ids]
    assert all(f.status is JobStatus.SUCCEEDED for f in finals)
    testlog.info(
        "post-restart completion job_ids=%s statuses=%s "
        "verdict=all_completed_after_restart_no_loss",
        ids, [f.status.value for f in finals],
    )
    q2.shutdown_blocking(timeout=5)
    q2.close_store()


def test_shutdown_interrupts_stuck_job_without_loss(make_queue, db_path):
    """卡住的任务在关停时被收尾为 RETRY_WAIT（attempts 回退，不判失败），
    重启队列后继续执行并成功 —— 任务不丢失、状态不错乱。"""
    from jobqueue.queue import JobQueue
    from jobqueue.storage import JobStore
    from tests.conftest import _register_all

    q = make_queue(concurrency=2, default_timeout_seconds=60)
    marker = Path(db_path).with_name("sleep_once.marker")
    if marker.exists():
        marker.unlink()
    job, _ = q.submit(
        "sleep_once",
        {"marker_file": str(marker), "sleep": 60},
        business_key="restart-me",
        timeout_seconds=60,
    )
    time.sleep(0.4)  # 等子进程进入长睡
    mid = q.get_job(job.id)
    assert mid.status is JobStatus.RUNNING and mid.attempts == 1

    t0 = time.time()
    q.shutdown_blocking(timeout=5)  # 不强求等子进程睡完，应较快收尾
    elapsed = time.time() - t0
    assert elapsed < 8

    # 用同一个库文件「重启进程」：未完成任务仍在，状态为 RETRY_WAIT
    store2 = JobStore(db_path)
    q2 = JobQueue(store2, max_concurrency=2, default_timeout_seconds=10,
                  poll_interval=0.01)
    _register_all(q2)
    resumed = q2.get_job(job.id)
    testlog.info(
        "after shutdown job_id=%s status=%s attempts=%d "
        "verdict=interrupted_requeued_not_dead_not_lost",
        resumed.id, resumed.status.value, resumed.attempts,
    )
    assert resumed.status is JobStatus.RETRY_WAIT
    assert resumed.attempts == 0  # 被中断的尝试不计入重试预算

    q2.start()
    final = q2.wait_for_terminal(job.id, timeout=10)
    testlog.info(
        "after restart job_id=%s status=%s attempts=%d result=%s "
        "verdict=resumed_and_completed",
        final.id, final.status.value, final.attempts, final.result,
    )
    assert final.status is JobStatus.SUCCEEDED
    assert final.attempts == 1
    assert final.result == "resumed after restart"
    q2.shutdown_blocking(timeout=5)
    q2.close_store()


def test_pending_jobs_survive_shutdown_and_run_next_time(make_queue, db_path):
    """关停时还在 PENDING 的任务只落库未执行，重启后照常执行。"""
    from jobqueue.queue import JobQueue
    from jobqueue.storage import JobStore
    from tests.conftest import _register_all

    q = make_queue(concurrency=1)
    q.submit("slow_handler", {"seconds": 1.0}, business_key="blocker")
    time.sleep(0.2)
    pending, _ = q.submit("echo", {"k": "v"}, business_key="never-started")
    time.sleep(0.1)
    assert q.get_job(pending.id).status in (
        JobStatus.PENDING, JobStatus.RUNNING
    )
    q.shutdown_blocking(timeout=8)

    store2 = JobStore(db_path)
    q2 = JobQueue(store2, max_concurrency=2, default_timeout_seconds=10,
                  poll_interval=0.01)
    _register_all(q2)
    q2.start()
    final = q2.wait_for_terminal(pending.id, timeout=10)
    testlog.info(
        "pending survive job_id=%s status=%s result=%s "
        "verdict=pending_persisted_and_executed_after_restart",
        final.id, final.status.value, final.result,
    )
    assert final.status is JobStatus.SUCCEEDED
    assert final.result == {"echo": {"k": "v"}}
    q2.shutdown_blocking(timeout=5)
    q2.close_store()
