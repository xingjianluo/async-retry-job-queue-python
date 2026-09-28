"""测试专用顶层处理器（子进程需要可导入/可 pickle，不能放闭包里）。"""

from __future__ import annotations

import fcntl
import os
import time
from typing import Any

from jobqueue.models import NonRetryableError


def counting_handler(payload: dict[str, Any]) -> int:
    """每次被调用都对文件计数器 +1，用于断言「只执行了一次」。"""
    path = payload["counter_file"]
    with open(path + ".lock", "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            with open(path, encoding="utf-8") as fh:
                n = int(fh.read().strip() or "0")
        except (OSError, ValueError):
            n = 0
        n += 1
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(str(n))
        return n


def concurrency_probe(payload: dict[str, Any]) -> dict[str, Any]:
    """记录同时在跑的实例数峰值，用于断言并发上限。"""
    path = payload["state_file"]
    hold = float(payload.get("hold", 0.25))
    with open(path + ".lock", "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        try:
            with open(path, encoding="utf-8") as fh:
                active, peak = (int(x) for x in fh.read().split(","))
        except (OSError, ValueError):
            active, peak = 0, 0
        active += 1
        peak = max(peak, active)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(f"{active},{peak}")
    time.sleep(hold)
    with open(path + ".lock", "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        with open(path, encoding="utf-8") as fh:
            active, peak = (int(x) for x in fh.read().split(","))
        active -= 1
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(f"{active},{peak}")
    return {"pid": os.getpid(), "peak": peak}


def read_peak(path: str) -> int:
    with open(path, encoding="utf-8") as fh:
        _, peak = (int(x) for x in fh.read().split(","))
    return peak


def sleep_once(payload: dict[str, Any]) -> str:
    """第一次调用长睡（模拟卡住），后续调用立即成功，用于停机恢复测试。"""
    marker = payload["marker_file"]
    if not os.path.exists(marker):
        with open(marker, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
        time.sleep(float(payload.get("sleep", 60.0)))
        return "slept"
    return "resumed after restart"


def fatal_handler(payload: dict[str, Any]) -> None:
    raise NonRetryableError("explicitly non-retryable from test")
