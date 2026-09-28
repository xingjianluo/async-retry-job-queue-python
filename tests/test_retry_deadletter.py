"""重试退避与死信：重试耗尽 / 不可重试 / 超时耗尽，失败原因必须可区分。"""

from __future__ import annotations

from pathlib import Path

from jobqueue.backoff import ExponentialBackoff, FixedBackoff
from jobqueue.models import FailureReason, JobStatus
from tests.conftest import testlog


def test_retry_then_success_attempts_match(make_queue, tmp_path):
    q = make_queue(concurrency=1)
    state = tmp_path / "flaky.cnt"
    job, _ = q.submit(
        "flaky",
        {"state_file": str(state), "fail_times": 2},
        business_key="flaky-1",
        max_retries=3,
    )
    final = q.wait_for_terminal(job.id, timeout=8)
    testlog.info(
        "retry-success job_id=%s attempts=%d max_retries=%d status=%s "
        "verdict=retry_budget_allowed_recovery",
        final.id, final.attempts, final.max_retries, final.status.value,
    )
    assert final.status is JobStatus.SUCCEEDED
    assert final.attempts == 3  # 首次 + 2 次重试
    assert final.failure_reason is None


def test_retries_exhausted_dead_with_reason(make_queue):
    q = make_queue(concurrency=2)
    job, _ = q.submit(
        "always_fail", {}, business_key="dead-1", max_retries=2
    )
    final = q.wait_for_terminal(job.id, timeout=8)
    testlog.info(
        "retry-exhausted job_id=%s attempts=%d retries_used=%d status=%s "
        "failure_reason=%s last_error=%s verdict=dead_after_last_retry",
        final.id, final.attempts, final.attempts - 1, final.status.value,
        final.failure_reason, final.last_error,
    )
    assert final.status is JobStatus.DEAD
    assert final.attempts == 3  # 1 次首发 + 2 次重试
    assert final.failure_reason is FailureReason.RETRIES_EXHAUSTED
    assert final.last_error and "always fails" in final.last_error


def test_non_retryable_goes_dead_immediately(make_queue):
    q = make_queue(concurrency=2)
    job, _ = q.submit(
        "always_fatal", {}, business_key="dead-fatal", max_retries=5
    )
    final = q.wait_for_terminal(job.id, timeout=8)
    testlog.info(
        "non-retryable job_id=%s attempts=%d status=%s failure_reason=%s "
        "last_error=%s verdict=dead_without_retry",
        final.id, final.attempts, final.status.value, final.failure_reason,
        final.last_error,
    )
    assert final.status is JobStatus.DEAD
    assert final.attempts == 1  # 不重试
    assert final.failure_reason is FailureReason.NON_RETRYABLE
    assert final.last_error and "invalid" in final.last_error


def test_timeout_exhausted_has_distinct_reason(make_queue):
    q = make_queue(concurrency=2, default_timeout_seconds=0.3)
    job, _ = q.submit(
        "slow_handler", {"seconds": 30}, business_key="dead-timeout",
        max_retries=1, timeout_seconds=0.3,
    )
    final = q.wait_for_terminal(job.id, timeout=8)
    testlog.info(
        "timeout-exhausted job_id=%s attempts=%d status=%s failure_reason=%s "
        "last_error=%s verdict=dead_timeout_distinct_from_error",
        final.id, final.attempts, final.status.value, final.failure_reason,
        final.last_error,
    )
    assert final.status is JobStatus.DEAD
    assert final.attempts == 2
    assert final.failure_reason is FailureReason.TIMEOUT_RETRIES_EXHAUSTED
    assert final.last_error and "timeout" in final.last_error.lower()


def test_backoff_delays_retries():
    # 退避策略本身的确定性：FixedBackoff 固定延迟；指数退避有抖动上限
    assert FixedBackoff(0.5).delay_for(1) == 0.5
    assert FixedBackoff(0.5).delay_for(3) == 0.5
    import random

    bo = ExponentialBackoff(base=1.0, factor=2.0, cap=10.0,
                            rng=random.Random(42))
    d1, d2, d3 = bo.delay_for(1), bo.delay_for(2), bo.delay_for(3)
    testlog.info(
        "backoff job_id=- attempts=- retry_delays=(%.3f,%.3f,%.3f) "
        "verdict=jittered_exponential_within_cap", d1, d2, d3,
    )
    assert 0 <= d1 <= 1.0 and 0 <= d2 <= 2.0 and 0 <= d3 <= 4.0


def test_terminal_state_is_monotonic(make_queue):
    """终态之后重复提交同一键，看到的状态仍是终态，不会被重新激活。"""
    q = make_queue(concurrency=2)
    job, _ = q.submit("always_fatal", {}, business_key="mono", max_retries=0)
    final = q.wait_for_terminal(job.id, timeout=8)
    assert final.status is JobStatus.DEAD
    again, created = q.submit("always_fatal", {}, business_key="mono")
    testlog.info(
        "post-terminal duplicate job_id=%s status=%s created=%s "
        "verdict=terminal_state_not_reactivated",
        again.id, again.status.value, created,
    )
    assert created is False
    assert again.status is JobStatus.DEAD
    assert again.id == final.id
