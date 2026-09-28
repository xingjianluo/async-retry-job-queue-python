"""并发执行：硬上限不被突破；卡住/超时任务不能拖垮其他任务。"""

from __future__ import annotations

import time
from pathlib import Path

from jobqueue.models import JobStatus
from tests.conftest import testlog
from tests.testhandlers import read_peak


def test_concurrency_never_exceeds_limit(make_queue, tmp_path):
    limit = 2
    q = make_queue(concurrency=limit)
    state = tmp_path / "conc.txt"
    state.write_text("0,0")

    ids = []
    for i in range(8):
        job, _ = q.submit(
            "concurrency_probe",
            {"state_file": str(state), "hold": 0.2},
            business_key=f"c-{i}",
            timeout_seconds=5.0,
        )
        ids.append(job.id)

    finals = [q.wait_for_terminal(jid, timeout=10) for jid in ids]
    peak = read_peak(str(state))

    testlog.info(
        "concurrency limit jobs=%d limit=%d observed_peak=%d all_status=%s "
        "verdict=peak_within_limit",
        len(ids), limit, peak, sorted({f.status.value for f in finals}),
    )
    assert all(f.status is JobStatus.SUCCEEDED for f in finals)
    assert peak <= limit, f"并发峰值 {peak} 超过上限 {limit}"
    assert peak == limit, "任务量足够时应打满过上限（验证上限真的在限流）"


def test_stuck_job_does_not_block_others(make_queue, tmp_path):
    # 一个槽位被超时任务占住，其余任务必须照常完成
    q = make_queue(concurrency=2, default_timeout_seconds=0.5)
    stuck, _ = q.submit(
        "slow_handler", {"seconds": 30}, business_key="stuck", timeout_seconds=0.5
    )
    time.sleep(0.1)  # 确保 stuck 先占槽
    ok_ids = []
    for i in range(3):
        j, _ = q.submit("echo", {"i": i}, business_key=f"ok-{i}")
        ok_ids.append(j.id)

    t0 = time.time()
    ok_finals = [q.wait_for_terminal(jid, timeout=10) for jid in ok_ids]
    elapsed = time.time() - t0

    testlog.info(
        "isolation test stuck_job_id=%s other_jobs_succeeded=%d elapsed=%.2fs "
        "verdict=others_unaffected_by_stuck_job",
        stuck.id, sum(f.status is JobStatus.SUCCEEDED for f in ok_finals), elapsed,
    )
    assert all(f.status is JobStatus.SUCCEEDED for f in ok_finals)
    assert [f.result for f in ok_finals] == [
        {"echo": {"i": i}} for i in range(3)
    ]
    # 30 秒的睡眠被 0.5 秒超时强杀，其它任务没被拖到 30 秒
    assert elapsed < 10

    stuck_final = q.wait_for_terminal(stuck.id, timeout=10)
    # max_retries 默认 3：最终进死信，原因是「超时」类耗尽，可与普通失败区分
    testlog.info(
        "stuck final job_id=%s status=%s attempts=%d failure_reason=%s "
        "verdict=timeout_class_distinguishable",
        stuck_final.id, stuck_final.status.value, stuck_final.attempts,
        stuck_final.failure_reason,
    )
    from jobqueue.models import FailureReason

    assert stuck_final.status is JobStatus.DEAD
    assert stuck_final.failure_reason is FailureReason.TIMEOUT_RETRIES_EXHAUSTED
