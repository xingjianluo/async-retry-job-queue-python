r"""任务领域模型：状态机、错误分类与任务记录。

状态机（单调流转，见 README）::

    PENDING -> RUNNING -> SUCCEEDED
                        \-> RETRY_WAIT -> RUNNING ...   （最多 max_retries 次重试）
                        \-> DEAD
    任意非终态 --------> RETRY_WAIT                      （停机中断，不计重试）
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """任务状态。值即落库字符串，保证可读、可比较。"""

    PENDING = "PENDING"                # 已提交，等待调度
    RUNNING = "RUNNING"                # 已被某个 worker 抢占，正在执行
    RETRY_WAIT = "RETRY_WAIT"          # 失败后退避等待，到期重新调度
    SUCCEEDED = "SUCCEEDED"            # 成功（终态）
    DEAD = "DEAD"                      # 死信（终态）


class FailureReason(str, enum.Enum):
    """死信/失败的可区分原因。"""

    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"   # 可重试错误，但重试次数耗尽
    NON_RETRYABLE = "NON_RETRYABLE"           # 业务显式抛出不可重试错误
    TIMEOUT_RETRIES_EXHAUSTED = "TIMEOUT_RETRIES_EXHAUSTED"  # 超时重试耗尽


# 非终态：还可以继续被调度
NON_TERMINAL = frozenset({JobStatus.PENDING, JobStatus.RUNNING, JobStatus.RETRY_WAIT})
# 终态：状态机终点
TERMINAL = frozenset({JobStatus.SUCCEEDED, JobStatus.DEAD})

# 合法的状态流转边（单调，不允许回退到更早阶段；停机中断除外，见 RESET_EDGES）
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.RETRY_WAIT, JobStatus.DEAD}
    ),
    JobStatus.RETRY_WAIT: frozenset({JobStatus.RUNNING}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.DEAD: frozenset(),
}
# 优雅停机时允许把 RUNNING 重置为 RETRY_WAIT（恢复性流转，不改变业务阶段序号）
RESET_ON_SHUTDOWN = (JobStatus.RUNNING, JobStatus.RETRY_WAIT)


class JobQueueError(Exception):
    """任务队列领域错误基类。"""


class QueueClosedError(JobQueueError):
    """队列已进入关停流程，拒绝接收新任务。"""


class NonRetryableError(JobQueueError):
    """业务处理器抛出该错误表示任务不可重试，直接进入死信。

    必须定义在模块顶层，保证子进程可以 pickle。
    """


class JobTimeoutError(JobQueueError):
    """任务执行超过配置的超时时间。"""


@dataclass
class Job:
    """一条任务记录（内存视图，持久化由 storage 层负责）。"""

    id: str
    business_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0                     # 已开始执行的次数（首次执行为 1）
    max_retries: int = 0                  # 失败后额外允许的重试次数
    timeout_seconds: float = 30.0
    run_after: float = 0.0                # UNIX 时间戳，到期才可调度
    last_error: str | None = None
    failure_reason: FailureReason | None = None
    result: Any = None
    version: int = 0                      # 乐观锁版本，保证状态流转的原子性
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL
