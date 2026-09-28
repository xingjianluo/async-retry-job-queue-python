"""内置示例任务处理器，用于本地验证与测试。

所有处理器签名均为 ``async def handler(payload: dict) -> Any``。
"""
from __future__ import annotations

import asyncio
from typing import Any

from .errors import NonRetryable

# 以 business_key 维度记录被调用的次数，便于演示「失败 N 次后成功」。
CALLS: dict[str, int] = {}


async def succeed(payload: dict[str, Any]) -> dict[str, Any]:
    """恒成功，回显 payload。"""
    return {"ok": True, "echo": payload}


async def fail_then_succeed(payload: dict[str, Any]) -> dict[str, Any]:
    """前 fail_times 次抛可重试错误，之后成功。

    payload:
        key: 调用计数命名空间（默认 "__default__"），多任务请传不同 key。
        fail_times: 需要失败的次数（默认 2）。
    """
    key = str(payload.get("key", "__default__"))
    fail_times = int(payload.get("fail_times", 2))
    count = CALLS.get(key, 0) + 1
    CALLS[key] = count
    if count <= fail_times:
        raise RuntimeError(f"模拟瞬时错误：第 {count}/{fail_times} 次失败")
    return {"ok": True, "calls": count}


async def always_retryable(payload: dict[str, Any]) -> None:
    """恒抛可重试错误，直到重试耗尽进入死信（RETRIES_EXHAUSTED）。"""
    raise RuntimeError("模拟持续失败的可重试错误")


async def always_non_retryable(payload: dict[str, Any]) -> None:
    """首次即抛不可重试错误，直接死信（NON_RETRYABLE_ERROR）。"""
    raise NonRetryable("模拟业务校验失败：不可重试的确定性错误")


async def slow(payload: dict[str, Any]) -> str:
    """睡眠 seconds 秒，用于超时与关停收尾测试。"""
    seconds = float(payload.get("seconds", 1.0))
    await asyncio.sleep(seconds)
    return f"slept {seconds}s"


def reset_calls() -> None:
    """测试辅助：清空共享调用计数。"""
    CALLS.clear()


def default_registry():  # noqa: ANN201 - 避免顶层导入产生循环
    """构造并注册全部内置处理器的注册表。"""
    from .registry import HandlerRegistry

    registry = HandlerRegistry()
    registry.register("succeed", succeed)
    registry.register("fail_then_succeed", fail_then_succeed)
    registry.register("always_retryable", always_retryable)
    registry.register("always_non_retryable", always_non_retryable)
    registry.register("slow", slow)
    return registry
