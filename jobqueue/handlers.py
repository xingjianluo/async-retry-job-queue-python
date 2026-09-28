"""内置演示与测试用任务处理器。

注意：子进程处理器必须是模块顶层的可 pickle 函数（spawn 模式下按
"jobqueue.handlers.xxx" 导入），不能用闭包/lambda。测试里如需定制，
可通过 registry 传入顶层函数（fork 模式），或在测试模块定义顶层函数。
"""

from __future__ import annotations

import os
import time
from typing import Any

from .models import NonRetryableError


def echo(payload: dict[str, Any]) -> dict[str, Any]:
    """演示：原样返回 payload。"""
    return {"echo": payload}


def flaky(payload: dict[str, Any]) -> str:
    """前 fail_times 次调用失败，之后成功（按调用次数文件计数，跨进程持久）。"""
    state_file = payload["state_file"]
    fail_times = int(payload.get("fail_times", 1))
    n = 0
    try:
        with open(state_file, encoding="utf-8") as fh:
            n = int(fh.read().strip() or "0")
    except (OSError, ValueError):
        n = 0
    n += 1
    with open(state_file, "w", encoding="utf-8") as fh:
        fh.write(str(n))
    if n <= fail_times:
        raise RuntimeError(f"transient failure #{n}")
    return f"succeeded on attempt {n}"


def always_fail(payload: dict[str, Any]) -> None:
    """始终抛可重试异常，用于测试重试耗尽。"""
    raise RuntimeError("always fails")


def always_fatal(payload: dict[str, Any]) -> None:
    """始终抛不可重试异常，用于测试直接死信。"""
    raise NonRetryableError("permanently invalid payload")


def slow_handler(payload: dict[str, Any]) -> str:
    """休眠指定秒数，用于超时/停机测试。"""
    seconds = float(payload.get("seconds", 5.0))
    time.sleep(seconds)
    return f"slept {seconds}s in pid {os.getpid()}"
