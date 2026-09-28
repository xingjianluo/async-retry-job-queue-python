"""执行层：在隔离子进程中运行任务处理器，支持硬超时。

为什么用子进程而不是线程：超时的任务可能卡在 C 扩展 / 死循环里，
线程无法被强制终止；子进程可以 terminate/kill，单个坏任务不会拖垮
调度进程，也不会长期占用并发槽位。

约定：
- 处理器必须是「模块顶层函数」，按 name 注册（name 建议为 "模块.函数名"），
  spawn 启动方式下子进程靠 import 找到它；
- payload 必须可 pickle；返回值建议可 JSON 序列化（结果落 SQLite）；
- 处理器抛 NonRetryableError => FATAL（不可重试，直接死信）；
- 其它异常 => FAILURE（可重试）；超时 => TIMEOUT（默认可重试）。
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from .models import NonRetryableError  # re-export for handlers  # noqa: F401

# 任务处理器签名：接收 payload，返回可 JSON 序列化的结果
TaskHandler = Callable[[dict[str, Any]], Any]


class AttemptOutcome(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"          # 普通失败（默认可重试）
    FATAL = "FATAL"              # NonRetryableError 等不可重试失败
    TIMEOUT = "TIMEOUT"          # 执行超时被强杀
    INTERRUPTED = "INTERRUPTED"  # 队列关停导致未拿到完整结论


@dataclass
class AttemptResult:
    outcome: AttemptOutcome
    value: Any = None
    error: str | None = None
    error_type: str | None = None

    @property
    def retryable(self) -> bool:
        return self.outcome in (AttemptOutcome.FAILURE, AttemptOutcome.TIMEOUT)


# 活跃子进程登记表：关停时统一终止，避免任务进程成为孤儿
_processes: dict[int, "mp.process.BaseProcess"] = {}
_processes_lock = threading.Lock()


def _register(proc: "mp.process.BaseProcess") -> None:
    with _processes_lock:
        _processes[proc.pid] = proc


def _unregister(proc: "mp.process.BaseProcess") -> None:
    with _processes_lock:
        _processes.pop(proc.pid, None)


def terminate_registered(timeout: float = 2.0) -> int:
    """终止所有仍在执行的任务子进程，返回终止的数量。优雅关停时调用。"""
    with _processes_lock:
        procs = list(_processes.values())
    count = 0
    for proc in procs:
        if proc.is_alive():
            proc.terminate()
            count += 1
    for proc in procs:
        proc.join(timeout)
        if proc.is_alive():  # SIGTERM 无效则强杀
            proc.kill()
            proc.join(timeout)
    return count


def _resolve(handler_name: str, registry: dict[str, TaskHandler]) -> TaskHandler:
    if handler_name in registry:
        return registry[handler_name]
    # 允许直接传 "pkg.module.func" 形式，spawn 子进程里按导入路径解析
    module_name, _, attr = handler_name.rpartition(".")
    if not module_name:
        raise KeyError(handler_name)
    import importlib

    mod = importlib.import_module(module_name)
    return getattr(mod, attr)


def _child_entry(
    handler_name: str,
    payload: dict[str, Any],
    result_path: str,
    registry: dict[str, TaskHandler] | None = None,
) -> None:
    """子进程入口。执行结果（成功或失败）写到 result_path(JSON)。"""
    body: dict[str, Any]
    try:
        handler = _resolve(handler_name, registry or {})
        value = handler(payload)
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            value = repr(value)
        body = {"outcome": AttemptOutcome.SUCCESS.value, "value": value}
    except NonRetryableError as exc:
        body = {
            "outcome": AttemptOutcome.FATAL.value,
            "error": str(exc) or exc.__class__.__name__,
            "error_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        }
    except Exception as exc:  # 业务普通异常一律视为可重试失败
        body = {
            "outcome": AttemptOutcome.FAILURE.value,
            "error": f"{type(exc).__name__}: {exc}",
            "error_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
            "traceback": traceback.format_exc(limit=5),
        }
    with open(result_path, "w", encoding="utf-8") as fh:
        json.dump(body, fh, ensure_ascii=False)


def _read_result(path: str) -> AttemptResult:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return AttemptResult(
            AttemptOutcome.FATAL,
            error=f"worker exited without readable result: {exc}",
            error_type="ExecutorResultError",
        )
    return AttemptResult(
        AttemptOutcome(data["outcome"]),
        value=data.get("value"),
        error=data.get("error"),
        error_type=data.get("error_type"),
    )


def run_attempt(
    handler_name: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    registry: dict[str, TaskHandler],
    *,
    drain_deadline: Callable[[], float | None] | None = None,
) -> AttemptResult:
    """起一个隔离子进程执行一次任务，超时则杀掉并返回 TIMEOUT。

    drain_deadline：返回关停收尾的绝对截止时刻（UNIX 时间戳）的回调；
    返回 None 表示未关停。任务即使在关停开始前就已在 join，也能在截止
    时刻被杀掉并返回 INTERRUPTED（区别于任务自身 timeout 的 TIMEOUT）。
    """
    fd, result_path = tempfile.mkstemp(prefix="jobresult-", suffix=".json")
    os.close(fd)
    # 子进程拿到的 registry 在 fork 下可用；spawn 下走 import 解析
    proc = mp.get_context("fork").Process(
        target=_child_entry,
        args=(handler_name, payload, result_path, registry),
        daemon=True,
    )
    proc.start()
    _register(proc)
    started = time.monotonic()
    try:
        while True:
            task_budget_left = timeout_seconds - (time.monotonic() - started)
            if task_budget_left <= 0:
                timed_out = True
                interrupted = False
                break
            wait_chunk = min(0.05, task_budget_left)
            deadline_ts = drain_deadline() if drain_deadline else None
            if deadline_ts is not None:
                drain_left = deadline_ts - time.time()
                if drain_left <= 0:
                    timed_out, interrupted = False, True
                    break
                wait_chunk = min(wait_chunk, drain_left)
            proc.join(wait_chunk)
            if not proc.is_alive():
                timed_out = interrupted = False
                break
        if proc.is_alive():
            proc.terminate()
            proc.join(1.0)
            if proc.is_alive():
                proc.kill()
                proc.join(1.0)
            if interrupted:
                return AttemptResult(
                    AttemptOutcome.INTERRUPTED,
                    error="interrupted by graceful shutdown, drain budget exhausted",
                    error_type="ShutdownInterrupted",
                )
            return AttemptResult(
                AttemptOutcome.TIMEOUT,
                error=f"exceeded timeout {timeout_seconds}s, worker killed",
                error_type="JobTimeoutError",
            )
        if proc.exitcode != 0 and not os.path.exists(result_path):
            return AttemptResult(
                AttemptOutcome.FAILURE,
                error=f"worker exited abnormally, exitcode={proc.exitcode}",
                error_type="AbnormalWorkerExit",
            )
        return _read_result(result_path)
    finally:
        _unregister(proc)
        try:
            os.unlink(result_path)
        except OSError:
            pass
