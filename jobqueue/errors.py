"""错误分类：判定一次执行结果属于可重试 / 不可重试 / 超时。

判定依据（可解释）：
1. 任务协程内部抛出 ``NonRetryable``（或其子类）-> 不可重试，直接死信。
2. 任务执行超过单次尝试时限，被 ``asyncio.wait_for`` 取消并聚合为
   ``JobTimeout`` -> 超时，按可重试处理，耗尽后失败原因记为 TIMEOUT_EXHAUSTED。
3. 关停信号到达、任务协程被取消 -> SHUTDOWN_INTERRUPTED，按可重试处理，
   进程重启后继续，不计入业务失败的死信判定（仅消耗一次尝试）。
4. 其他任何异常 -> 普通可重试错误，耗尽后失败原因记为 RETRIES_EXHAUSTED。
"""
from __future__ import annotations

import asyncio

from .models import AttemptOutcome


class NonRetryable(Exception):
    """业务任务抛出即表示不可重试，直接进入死信。

    用法::

        raise NonRetryable("参数非法：缺少字段 x")
    """


class JobTimeout(Exception):
    """单次尝试超时（队列内部把 asyncio.TimeoutError 归一化为此类型）。"""


def classify_exception(exc: BaseException) -> AttemptOutcome:
    """把执行期异常映射为尝试结果。"""
    if isinstance(exc, NonRetryable):
        return AttemptOutcome.NON_RETRYABLE_ERROR
    if isinstance(exc, JobTimeout):
        return AttemptOutcome.TIMEOUT
    if isinstance(exc, asyncio.CancelledError):
        return AttemptOutcome.SHUTDOWN_INTERRUPTED
    return AttemptOutcome.RETRYABLE_ERROR


def describe_failure(outcome: AttemptOutcome, attempts: int, max_attempts: int) -> str:
    """生成可解释的判定依据文本（写入 last_error_message / 日志）。"""
    if outcome is AttemptOutcome.NON_RETRYABLE_ERROR:
        return (
            f"判定=不可重试错误(NON_RETRYABLE_ERROR)：任务显式抛出 NonRetryable，"
            f"不再重试；尝试次数={attempts}/{max_attempts}。"
        )
    if outcome is AttemptOutcome.TIMEOUT:
        return (
            f"判定=超时耗尽(TIMEOUT_EXHAUSTED)：第 {attempts}/{max_attempts} 次尝试仍超过"
            f"单次时限，重试次数已用尽。"
        )
    return (
        f"判定=重试耗尽(RETRIES_EXHAUSTED)：已尝试 {attempts}/{max_attempts} 次，"
        f"仍为可重试错误，重试次数已用尽。"
    )
