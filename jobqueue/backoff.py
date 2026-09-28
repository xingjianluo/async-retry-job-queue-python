"""失败重试退避策略：根据重试序号计算下一次可调度的延迟秒数。"""

from __future__ import annotations

import random
from dataclasses import dataclass


class Backoff:
    """退避策略接口。"""

    def delay_for(self, retry_index: int) -> float:
        """retry_index 从 1 开始（第 1 次失败后），返回延迟秒数。"""
        raise NotImplementedError


@dataclass
class ExponentialBackoff(Backoff):
    """指数退避 + 全抖动（full jitter）。

    delay = random(0, min(cap, base * factor ** (retry_index - 1)))

    抖动通过注入 rng 实现，测试可传固定随机源以获得确定性。
    """

    base: float = 1.0
    factor: float = 2.0
    cap: float = 60.0
    rng: random.Random | None = None

    def delay_for(self, retry_index: int) -> float:
        if retry_index < 1:
            raise ValueError("retry_index 必须从 1 开始")
        if self.base < 0 or self.cap < 0:
            raise ValueError("退避参数不能为负")
        rng = self.rng or random
        ceiling = min(self.cap, self.base * (self.factor ** (retry_index - 1)))
        return rng.uniform(0.0, ceiling)


@dataclass
class FixedBackoff(Backoff):
    """固定间隔退避，便于测试与确定性场景。"""

    delay: float = 1.0

    def delay_for(self, retry_index: int) -> float:
        if retry_index < 1:
            raise ValueError("retry_index 必须从 1 开始")
        return self.delay


@dataclass
class NoBackoff(Backoff):
    """无退避，立即重试。"""

    def delay_for(self, retry_index: int) -> float:
        return 0.0
