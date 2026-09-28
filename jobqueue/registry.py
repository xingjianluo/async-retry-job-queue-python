"""任务处理器注册表：按 job_type 找到可调用对象。"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

JobHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class HandlerNotFound(KeyError):
    """提交了未注册的 job_type。"""


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        """注册（或覆盖）一个任务处理器。"""
        if not job_type:
            raise ValueError("job_type 不能为空")
        self._handlers[job_type] = handler

    def get(self, job_type: str) -> JobHandler:
        try:
            return self._handlers[job_type]
        except KeyError:
            raise HandlerNotFound(job_type) from None

    def types(self) -> list[str]:
        return sorted(self._handlers)
