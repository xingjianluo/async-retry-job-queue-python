"""重复提交幂等性：同一 business_key 只能产生一个实际执行的任务。

重复提交必须：返回同一任务标识 + 当前状态；既不重复执行，也不静默丢弃
（返回 202/created=False，而不是报错或忽略）。
"""

from __future__ import annotations

import threading
from pathlib import Path

from jobqueue.models import JobStatus

from tests.conftest import testlog


def _read_counter(path: Path) -> int:
    try:
        return int(path.read_text().strip() or "0")
    except (OSError, ValueError):
        return 0


def test_duplicate_submit_returns_same_id_and_runs_once(make_queue, tmp_path):
    q = make_queue(concurrency=2)
    counter = tmp_path / "counter.txt"

    j1, created1 = q.submit(
        "counting_handler", {"counter_file": str(counter)}, business_key="biz-1"
    )
    # 等第一次执行进入后再重复提交，覆盖「运行中重复提交」窗口
    final = q.wait_for_terminal(j1.id, timeout=5)
    j2, created2 = q.submit(
        "counting_handler", {"counter_file": str(counter)}, business_key="biz-1"
    )

    testlog.info(
        "idempotency check job_id=%s first_created=%s duplicate_created=%s "
        "attempts=%d final_status=%s verdict=duplicate_must_share_id",
        j1.id, created1, created2, final.attempts, final.status.value,
    )

    assert created1 is True
    assert created2 is False                 # 不是静默丢弃：明确返回既有任务
    assert j1.id == j2.id                    # 同一任务标识
    assert j2.status is JobStatus.SUCCEEDED  # 重复提交读到的是当前状态
    assert final.result == 1
    assert _read_counter(counter) == 1       # 实际只执行了一次


def test_duplicate_submit_while_pending_shares_state(make_queue, tmp_path):
    # 先占满并发槽位，使目标任务停留在 PENDING 时重复提交
    q = make_queue(concurrency=1)
    q.submit("slow_handler", {"seconds": 1.0}, business_key="blocker")
    import time

    time.sleep(0.2)  # 等 blocker 抢到唯一槽位
    j1, c1 = q.submit("echo", {"x": 1}, business_key="biz-pending")
    j2, c2 = q.submit("echo", {"x": 2}, business_key="biz-pending")

    testlog.info(
        "pending-window duplicate job_id=%s status=%s created_flags=(%s,%s) "
        "verdict=pending_duplicate_visible",
        j1.id, j2.status.value, c1, c2,
    )
    assert c1 is True and c2 is False and j1.id == j2.id
    assert j2.status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCEEDED)

    final = q.wait_for_terminal(j1.id, timeout=5)
    # 以第一次提交的 payload 执行，结果只一份
    assert final.status is JobStatus.SUCCEEDED
    assert final.result == {"echo": {"x": 1}}


def test_concurrent_submit_same_business_key_single_job(make_queue, tmp_path):
    q = make_queue(concurrency=4)
    counter = tmp_path / "counter2.txt"
    results: list[tuple[str, bool]] = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()
        job, created = q.submit(
            "counting_handler",
            {"counter_file": str(counter)},
            business_key="biz-race",
        )
        results.append((job.id, created))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    job_id = results[0][0]
    final = q.wait_for_terminal(job_id, timeout=5)
    created_count = sum(1 for _, c in results if c)

    testlog.info(
        "concurrent submit race job_id=%s submitters=%d created=%d attempts=%d "
        "status=%s verdict=exactly_one_created",
        job_id, len(results), created_count, final.attempts, final.status.value,
    )

    assert len({jid for jid, _ in results}) == 1  # 8 次提交拿到同一个 id
    assert created_count == 1                      # 数据库层只有一次插入
    assert final.status is JobStatus.SUCCEEDED
    assert _read_counter(counter) == 1
