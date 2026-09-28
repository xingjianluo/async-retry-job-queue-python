"""重试与死信：退避后成功、重试耗尽、不可重试错误、超时隔离。

断言点：最终状态、attempts、可区分的 failure_reason，以及单任务卡住/持续
失败不会拖垮其他任务。日志打印任务标识、重试次数和判定依据。
"""
from __future__ import annotations

import logging
import time

from jobqueue.models import FailureReason, JobStatus

logger = logging.getLogger("test.retry_deadletter")


def test_retries_then_success(harness_factory, fast_policy):
    """前两次可重试失败 + 退避，第三次成功；attempts=3，状态 SUCCEEDED。"""
    h = harness_factory()
    job, _ = h.submit(
        "flaky-1", "fail_then_succeed", {"key": "flaky-1", "fail_times": 2},
        policy=fast_policy(max_attempts=4),
    )
    done = h.wait_for(job.id, {JobStatus.SUCCEEDED})
    logger.info(
        "退避重试判定：job_id=%s 最终状态=%s 重试次数(attempts)=%d/4 判定依据=前两次抛可重试"
        "RuntimeError、第三次正常返回",
        done.id, done.status.value, done.attempts,
    )
    assert done.attempts == 3
    assert done.result == {"ok": True, "calls": 3}
    outcomes = [a.outcome.value for a in done.history]
    assert outcomes == ["RETRYABLE_ERROR", "RETRYABLE_ERROR", "SUCCESS"]


def test_retries_exhausted_dead(harness_factory, fast_policy):
    """持续可重试失败：耗尽次数后进 DEAD，原因 RETRIES_EXHAUSTED。"""
    h = harness_factory()
    job, _ = h.submit(
        "poison-1", "always_retryable", policy=fast_policy(max_attempts=3)
    )
    dead = h.wait_for(job.id, {JobStatus.DEAD})
    logger.info(
        "重试耗尽判定：job_id=%s 最终状态=%s 重试次数=%d/%d failure_reason=%s 判定依据=每次均为"
        "RETRYABLE_ERROR 且 attempt 达到上限；last_error=%s",
        dead.id, dead.status.value, dead.attempts, dead.max_attempts,
        dead.failure_reason.value, dead.last_error_message,
    )
    assert dead.failure_reason is FailureReason.RETRIES_EXHAUSTED
    assert dead.attempts == 3
    assert dead.last_error_type == "RuntimeError"
    assert "RETRIES_EXHAUSTED" in dead.last_error_message
    assert [a.outcome.value for a in dead.history] == ["RETRYABLE_ERROR"] * 3


def test_non_retryable_dead(harness_factory, fast_policy):
    """不可重试错误：首次即进 DEAD，原因 NON_RETRYABLE_ERROR，不再重试。"""
    h = harness_factory()
    job, _ = h.submit(
        "fatal-1", "always_non_retryable", policy=fast_policy(max_attempts=5)
    )
    dead = h.wait_for(job.id, {JobStatus.DEAD})
    logger.info(
        "不可重试判定：job_id=%s 最终状态=%s 重试次数=%d/5 failure_reason=%s 判定依据=首次尝试"
        "抛出 NonRetryable，直接死信；last_error=%s",
        dead.id, dead.status.value, dead.attempts, dead.failure_reason.value,
        dead.last_error_message,
    )
    assert dead.failure_reason is FailureReason.NON_RETRYABLE_ERROR
    assert dead.attempts == 1, "不可重试错误不得重试"
    assert dead.last_error_type == "NonRetryable"
    assert "NON_RETRYABLE_ERROR" in dead.last_error_message


def test_failure_reasons_are_distinguishable(harness_factory, fast_policy):
    """三类死信原因必须可区分：耗尽 vs 不可重试 vs 超时耗尽。"""
    h = harness_factory()
    j_retry, _ = h.submit("d-retry", "always_retryable",
                          policy=fast_policy(max_attempts=1))
    j_fatal, _ = h.submit("d-fatal", "always_non_retryable",
                          policy=fast_policy(max_attempts=3))
    j_slow, _ = h.submit("d-slow", "slow", {"seconds": 10},
                         policy=fast_policy(max_attempts=1, attempt_timeout=0.1))
    r1 = h.wait_for(j_retry.id, {JobStatus.DEAD})
    r2 = h.wait_for(j_fatal.id, {JobStatus.DEAD})
    r3 = h.wait_for(j_slow.id, {JobStatus.DEAD})
    reasons = {r1.failure_reason, r2.failure_reason, r3.failure_reason}
    logger.info(
        "死信原因可区分判定：job_ids=(%s,%s,%s) reasons=(%s,%s,%s)",
        r1.id, r2.id, r3.id,
        r1.failure_reason.value, r2.failure_reason.value, r3.failure_reason.value,
    )
    assert reasons == {
        FailureReason.RETRIES_EXHAUSTED,
        FailureReason.NON_RETRYABLE_ERROR,
        FailureReason.TIMEOUT_EXHAUSTED,
    }


def test_timeout_job_does_not_block_others(harness_factory, fast_policy):
    """一个持续超时（最终死信）的任务不能拖垮其他正常任务。"""
    h = harness_factory(max_workers=2)
    stuck, _ = h.submit(
        "stuck-1", "slow", {"seconds": 30},
        policy=fast_policy(max_attempts=2, attempt_timeout=0.15, backoff_base=0.01,
                           backoff_max=0.02),
    )
    healthy_ids = []
    for i in range(4):
        job, _ = h.submit(f"healthy-{i}", "succeed", {"i": i},
                          policy=fast_policy())
        healthy_ids.append(job.id)

    t0 = time.time()
    for job_id in healthy_ids:
        ok = h.wait_for(job_id, {JobStatus.SUCCEEDED}, timeout=4)
        assert ok.result["echo"]["i"] in range(4)
    healthy_elapsed = time.time() - t0
    logger.info(
        "隔离性判定：卡住任务 job_id=%s 存在期间，4 个健康任务全部成功，耗时=%.3fs（应远小于"
        "卡住任务的 30s 睡眠）",
        stuck.id, healthy_elapsed,
    )
    assert healthy_elapsed < 3, "健康任务被卡住任务拖慢"

    dead = h.wait_for(stuck.id, {JobStatus.DEAD}, timeout=5)
    assert dead.failure_reason is FailureReason.TIMEOUT_EXHAUSTED
    assert dead.attempts == 2
