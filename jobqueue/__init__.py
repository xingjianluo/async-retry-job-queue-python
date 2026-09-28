"""本地异步任务队列：幂等提交、退避重试、死信、并发上限与优雅关停。

仅依赖标准库（sqlite3 / multiprocessing / threading）与 FastAPI，
不需要任何外部消息中间件或额外部署的服务。
"""

from __future__ import annotations

from .backoff import Backoff, ExponentialBackoff, FixedBackoff, NoBackoff
from .models import (
    FailureReason,
    Job,
    JobQueueError,
    JobStatus,
    NonRetryableError,
    QueueClosedError,
)
from .queue import JobQueue
from .storage import JobStore

__all__ = [
    "Backoff",
    "ExponentialBackoff",
    "FailureReason",
    "FixedBackoff",
    "Job",
    "JobQueue",
    "JobQueueError",
    "JobStatus",
    "JobStore",
    "NoBackoff",
    "NonRetryableError",
    "QueueClosedError",
]
