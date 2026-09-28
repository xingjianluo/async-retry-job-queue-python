"""任务队列的领域模型与状态/原因枚举。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """任务状态机：单调流转（只能向后，不允许回退）。

    PENDING -> RETRY_WAIT -> RUNNING(第 2+ 次尝试)
    PENDING -> RUNNING(首次)
    RUNNING -> SUCCEEDED / DEAD
    RUNNING -> RETRY_WAIT (失败但仍可重试)
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    DEAD = "DEAD"


class FailureReason(str, enum.Enum):
    """进入死信的可区分失败原因。"""

    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"  # 可重试错误，但重试次数已耗尽
    NON_RETRYABLE_ERROR = "NON_RETRYABLE_ERROR"  # 业务声明的不可重试错误
    TIMEOUT_EXHAUSTED = "TIMEOUT_EXHAUSTED"  # 持续超时，重试次数已耗尽


class AttemptOutcome(str, enum.Enum):
    SUCCESS = "SUCCESS"
    RETRYABLE_ERROR = "RETRYABLE_ERROR"
    NON_RETRYABLE_ERROR = "NON_RETRYABLE_ERROR"
    TIMEOUT = "TIMEOUT"
    SHUTDOWN_INTERRUPTED = "SHUTDOWN_INTERRUPTED"


class JobNotFoundError(Exception):
    """按 job_id / business_key 查询不到任务。"""


@dataclass(slots=True)
class AttemptRecord:
    attempt_no: int
    outcome: AttemptOutcome
    error_type: str | None = None
    error_message: str | None = None
    started_at: float | None = None
    finished_at: float | None = None


@dataclass(slots=True)
class Job:
    id: str
    business_key: str
    job_type: str
    payload: dict[str, str] = field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    attempt_timeout: float = 10.0
    retry_backoff_base: float = 0.05
    retry_backoff_factor: float = 2.0
    retry_backoff_max: float = 5.0
    failure_reason: FailureReason | None = None
    last_error_type: str | None = None
    last_error_message: str | None = None
    result: Any | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    not_before: float = 0.0
    run_after_shutdown: bool = False
    history: list[AttemptRecord] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """重试与退避配置；可在提交任务时逐任务覆盖。"""

    max_attempts: int = 3
    attempt_timeout: float = 10.0
    backoff_base: float = 0.05
    backoff_factor: float = 2.0
    backoff_max: float = 5.0
