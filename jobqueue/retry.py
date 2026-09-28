"""退避策略：只做纯计算，便于单测。

采用「指数退避 + 全抖动」：第 n 次失败后，等待
    random(0, min(backoff_max, backoff_base * factor ** (n - 1)))
抖动可以避免多个相同任务在同一时刻被同时唤醒（惊群）。
"""
from __future__ import annotations

import random

from .models import RetryPolicy


def compute_backoff(
    policy: RetryPolicy,
    failed_attempts: int,
    *,
    rng: random.Random | None = None,
) -> float:
    """根据已失败次数计算下一次尝试前的退避秒数。

    参数:
        policy: 重试配置（base/factor/max）。
        failed_attempts: 已经失败的尝试次数（>=1 才有退避）。
        rng: 可注入的随机源，便于测试确定性断言。

    返回: [0, cap] 之间的秒数。cap = min(backoff_max, base * factor**(n-1))。
    """
    if failed_attempts <= 0:
        return 0.0
    cap = policy.backoff_base * (policy.backoff_factor ** (failed_attempts - 1))
    cap = min(policy.backoff_max, cap)
    if cap <= 0:
        return 0.0
    source = rng if rng is not None else random
    return source.uniform(0.0, cap)
