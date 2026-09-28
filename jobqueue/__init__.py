"""本地后台异步任务队列：幂等提交、退避重试、死信、并发上限与优雅关停。"""
from .errors import NonRetryable
from .models import AttemptOutcome, FailureReason, Job, JobStatus, RetryPolicy
from .queue import JobQueue
from .registry import HandlerRegistry
from .storage import JobStore

__all__ = [
    "AttemptOutcome",
    "FailureReason",
    "HandlerRegistry",
    "Job",
    "JobQueue",
    "JobStatus",
    "JobStore",
    "NonRetryable",
    "RetryPolicy",
]
