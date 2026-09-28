"""异步任务队列：worker 池 + 并发上限 + 单次超时 + 退避重试 + 死信 + 优雅关停。

并发模型
--------
* 单个 dispatcher 协程轮询存储层 ``claim_due_job`` 抢占到期任务。抢占在
  SQLite 事务内完成，所以即使多个队列实例/线程并发，一个任务也只会被一个
  执行者拿到。
* dispatcher 用 ``asyncio.Semaphore(max_workers)`` 控制实际并发：占不到
  名额就停止抢占，从而严格限制并发执行数。
* 每个任务在独立任务协程中执行；超时用 ``asyncio.wait_for`` 实现，单个
  任务卡住只会在自己的时限后被取消，绝不阻塞其他任务。

重试与退避
----------
* 失败后按 :class:`~jobqueue.models.RetryPolicy` 计算指数退避（带全抖动），
  写入 ``not_before``，任务回到 RETRY_WAIT，到期自动再次被抢占。
* 超过 max_attempts 或抛出 :class:`~jobqueue.errors.NonRetryable` 即进入
  DEAD，并写入可区分的 failure_reason。

优雅关停
--------
收到 stop() 后：立即停止接收新提交与新抢占；在途尝试被协作式取消，统一
回退为 RETRY_WAIT（run_after_shutdown=1，not_before=now），因此进程重启
后必然被恢复重试；状态机守卫保证不会出现终态被覆盖或状态回退。
若进程被 kill -9，残留 RUNNING 由下次启动的 recover_on_startup 回收。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from .errors import JobTimeout, classify_exception, describe_failure
from .models import (
    AttemptOutcome,
    FailureReason,
    Job,
    JobStatus,
    RetryPolicy,
)
from .registry import HandlerRegistry
from .retry import compute_backoff
from .storage import JobStore

logger = logging.getLogger("jobqueue")


class QueueShuttingDown(RuntimeError):
    """关停过程中拒绝新提交。"""


class JobQueue:
    def __init__(
        self,
        store: JobStore,
        registry: HandlerRegistry,
        *,
        max_workers: int = 4,
        poll_interval: float = 0.02,
        shutdown_timeout: float = 15.0,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers 必须 >= 1")
        self.store = store
        self.registry = registry
        self.max_workers = max_workers
        self.poll_interval = poll_interval
        self.shutdown_timeout = shutdown_timeout

        # 异步原语在 start() 内创建，确保绑定到当前运行的事件循环
        # （队列实例可能先于 asyncio.run 构造，且支持 stop 后再次 start）。
        self._db_lock: asyncio.Lock | None = None
        self._sem: asyncio.Semaphore | None = None
        self._shutdown: asyncio.Event | None = None
        self._accepting = False
        self._dispatcher: asyncio.Task[None] | None = None
        self._executors: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        """恢复上次残留任务并启动 dispatcher。"""
        # 先确保异步原语绑定当前事件循环，再做任何 await，
        # 避免 start() 尚未挂起就并发 submit 时原语还是 None。
        self._db_lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(self.max_workers)
        self._shutdown = asyncio.Event()
        self._executors.clear()

        now = time.time()
        async with self._db_lock:
            recovered = await asyncio.to_thread(self.store.recover_on_startup, now)
        if recovered:
            logger.warning("启动恢复：检测到 %d 个上次关停时仍在 RUNNING 的任务，已回收重试",
                           recovered)
        self._accepting = True
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="jobqueue-dispatcher")
        logger.info("任务队列已启动：max_workers=%d", self.max_workers)

    async def stop(self) -> None:
        """优雅关停：停收新任务、停止抢占、收尾在途执行体并持久化其安全状态。"""
        if not self._accepting and self._dispatcher is None:
            return
        logger.info("收到关停信号：停止接收新任务与新抢占，开始收尾（最长 %.1fs）",
                    self.shutdown_timeout)
        # 与 submit 争用同一把锁：拿到锁后再翻转标志，保证没有提交被卡在
        # 「已检查 _accepting 但尚未落库」的半途中。
        async with self._db_lock:
            self._accepting = False
            self._shutdown.set()

        # dispatcher 轮询会在一个 poll_interval 内看到关停信号并退出。
        # 在途执行体立即收到取消：wait_for 包裹的业务协程随之取消，_execute
        # 的 SHUTDOWN_INTERRUPTED 分支把任务安全回退为 RETRY_WAIT，这就是
        # 「收尾」——每次在途尝试都有确定的落库状态，而不是任其悬挂在 RUNNING。
        running = {t for t in self._executors if not t.done()}
        for t in running:
            t.cancel()
        pending = running | ({self._dispatcher} if self._dispatcher is not None else set())
        if pending:
            await asyncio.wait(pending, timeout=self.shutdown_timeout)
        # 兜底：所有执行体理论上都应在落库完成后退出；仍残留则强取消并吞掉结果。
        leftovers = [
            t for t in list(self._executors) + ([self._dispatcher] if self._dispatcher else [])
            if not t.done()
        ]
        for t in leftovers:
            logger.warning("关停超时，强制取消任务协程 %s", t.get_name())
            t.cancel()
        for t in leftovers:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._dispatcher = None
        self._executors.clear()
        logger.info("任务队列已关停")

    # ------------------------------------------------------------------ #
    # 对外 API
    # ------------------------------------------------------------------ #
    async def submit(
        self,
        business_key: str,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        policy: RetryPolicy | None = None,
    ) -> tuple[Job, bool]:
        """幂等提交。返回 (任务, 是否首次创建)。关停中提交抛出 QueueShuttingDown。"""
        if not business_key:
            raise ValueError("business_key 不能为空")
        if not self._accepting:
            raise QueueShuttingDown("队列正在关停，拒绝接收新任务")
        # 未注册的 job_type 直接拒绝，避免产生永远失败的任务。
        self.registry.get(job_type)

        policy = policy or RetryPolicy()
        now = time.time()
        job = Job(
            id=uuid.uuid4().hex,
            business_key=business_key,
            job_type=job_type,
            payload=dict(payload or {}),
            status=JobStatus.PENDING,
            max_attempts=policy.max_attempts,
            attempt_timeout=policy.attempt_timeout,
            retry_backoff_base=policy.backoff_base,
            retry_backoff_factor=policy.backoff_factor,
            retry_backoff_max=policy.backoff_max,
            not_before=now,
            created_at=now,
            updated_at=now,
        )
        async with self._db_lock:
            # 持锁后复查：stop() 同样需要获取此锁才能进入收尾，
            # 因此要么提交完整落库，要么明确收到拒绝，不会出现半提交。
            if not self._accepting:
                raise QueueShuttingDown("队列正在关停，拒绝接收新任务")
            saved, created = await asyncio.to_thread(self.store.create_job_if_absent, job)
        if created:
            logger.info(
                "任务已提交 job_id=%s business_key=%s type=%s max_attempts=%d timeout=%.2fs",
                saved.id, business_key, job_type, policy.max_attempts, policy.attempt_timeout,
            )
        else:
            logger.info(
                "重复提交命中幂等 business_key=%s -> job_id=%s 当前状态=%s attempts=%d",
                business_key, saved.id, saved.status.value, saved.attempts,
            )
        return saved, created

    async def get_status(self, job_id: str) -> Job:
        async with self._db_lock:
            return await asyncio.to_thread(self.store.get_job, job_id)

    async def get_status_by_key(self, business_key: str) -> Job:
        async with self._db_lock:
            return await asyncio.to_thread(self.store.get_job_by_business_key, business_key)

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #
    async def _dispatch_loop(self) -> None:
        while not self._shutdown.is_set():
            # 先取名额再抢占：没有空闲 worker 就不产生新执行体，严格限制并发。
            await self._sem.acquire()
            if self._shutdown.is_set():
                self._sem.release()
                break
            try:
                async with self._db_lock:
                    job = await asyncio.to_thread(self.store.claim_due_job, time.time())
            except asyncio.CancelledError:
                self._sem.release()
                raise
            except BaseException:
                self._sem.release()
                logger.exception("抢占到期任务时发生存储异常")
                await asyncio.sleep(self.poll_interval)
                continue
            if job is None:
                self._sem.release()
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=self.poll_interval)
                except TimeoutError:
                    pass
                continue
            task = asyncio.create_task(self._execute(job), name=f"job-{job.id[:8]}-a{job.attempts}")
            self._executors.add(task)
            task.add_done_callback(self._on_executor_done)

    def _on_executor_done(self, task: asyncio.Task[None]) -> None:
        self._executors.discard(task)
        self._sem.release()
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("执行体异常逃逸: %r", exc, exc_info=exc)

    # ------------------------------------------------------------------ #
    # 执行
    # ------------------------------------------------------------------ #
    async def _execute(self, claimed: Job) -> None:
        job_id = claimed.id
        attempt_no = claimed.attempts
        handler = self.registry.get(claimed.job_type)
        started_at = time.time()
        logger.info(
            "开始执行 job_id=%s attempt=%d/%d timeout=%.2fs type=%s 判定依据=dispatcher抢占成功",
            job_id, attempt_no, claimed.max_attempts, claimed.attempt_timeout, claimed.job_type,
        )
        outcome: AttemptOutcome
        error_type: str | None = None
        error_message: str | None = None
        result: Any = None
        try:
            result = await asyncio.wait_for(handler(claimed.payload), timeout=claimed.attempt_timeout)
            outcome = AttemptOutcome.SUCCESS
        except asyncio.TimeoutError:
            outcome = AttemptOutcome.TIMEOUT
            error_type = JobTimeout.__name__
            error_message = (
                f"第 {attempt_no} 次尝试超过单次时限 {claimed.attempt_timeout:.2f}s"
            )
        except asyncio.CancelledError:
            outcome = AttemptOutcome.SHUTDOWN_INTERRUPTED
            error_type = "ShutdownInterrupted"
            error_message = "进程关停，本次尝试被取消，任务回退等待重启后重试"
            logger.warning("job_id=%s attempt=%d 被关停中断，回退 RETRY_WAIT",
                           job_id, attempt_no)
        except Exception as exc:  # noqa: BLE001 - 业务异常一律分类处理，绝不逃逸
            outcome = classify_exception(exc)
            error_type = type(exc).__name__
            error_message = str(exc) or repr(exc)
        finished_at = time.time()
        await self._finalize(
            claimed, attempt_no, outcome, result,
            error_type=error_type, error_message=error_message,
            started_at=started_at, finished_at=finished_at,
        )

    async def _finalize(
        self,
        claimed: Job,
        attempt_no: int,
        outcome: AttemptOutcome,
        result: Any,
        *,
        error_type: str | None,
        error_message: str | None,
        started_at: float,
        finished_at: float,
    ) -> None:
        job_id = claimed.id
        policy = RetryPolicy(
            max_attempts=claimed.max_attempts,
            attempt_timeout=claimed.attempt_timeout,
            backoff_base=claimed.retry_backoff_base,
            backoff_factor=claimed.retry_backoff_factor,
            backoff_max=claimed.retry_backoff_max,
        )
        now = finished_at

        if outcome is AttemptOutcome.SUCCESS:
            async with self._db_lock:
                await asyncio.to_thread(
                    self.store.record_attempt, job_id, attempt_no, outcome,
                    error_type=None, error_message=None,
                    started_at=started_at, finished_at=finished_at,
                )
                await asyncio.to_thread(
                    self.store.transition_to_success, job_id, attempt_no, result, now
                )
            logger.info("执行成功 job_id=%s attempt=%d/%d 判定依据=handler正常返回",
                        job_id, attempt_no, claimed.max_attempts)
            return

        # 失败路径：先追加尝试历史，再做状态流转。
        reason: FailureReason | None = None
        next_status = JobStatus.RETRY_WAIT
        not_before = now

        if outcome is AttemptOutcome.SHUTDOWN_INTERRUPTED:
            # 关停：无论是否还剩重试次数，都安全回退等待，由下次启动接管。
            not_before = now
        elif outcome is AttemptOutcome.NON_RETRYABLE_ERROR:
            reason = FailureReason.NON_RETRYABLE_ERROR
            next_status = JobStatus.DEAD
        elif attempt_no >= claimed.max_attempts:
            reason = (
                FailureReason.TIMEOUT_EXHAUSTED
                if outcome is AttemptOutcome.TIMEOUT
                else FailureReason.RETRIES_EXHAUSTED
            )
            next_status = JobStatus.DEAD
        else:
            delay = compute_backoff(policy, attempt_no)
            not_before = now + delay

        async with self._db_lock:
            await asyncio.to_thread(
                self.store.record_attempt, job_id, attempt_no, outcome,
                error_type=error_type, error_message=error_message,
                started_at=started_at, finished_at=finished_at,
            )
            if next_status is JobStatus.DEAD:
                explanation = describe_failure(outcome, attempt_no, claimed.max_attempts)
                await asyncio.to_thread(
                    self.store.transition_to_dead, job_id, attempt_no, reason,
                    error_type=error_type,
                    error_message=f"{error_message} | {explanation}",
                    now=now,
                )
                logger.error(
                    "任务进入死信 job_id=%s attempt=%d/%d reason=%s error=%s: %s %s",
                    job_id, attempt_no, claimed.max_attempts, reason.value,
                    error_type, error_message, explanation,
                )
            else:
                interrupted = outcome is AttemptOutcome.SHUTDOWN_INTERRUPTED
                await asyncio.to_thread(
                    self.store.transition_to_wait, job_id, attempt_no, not_before,
                    error_type=error_type, error_message=error_message, now=now,
                    run_after_shutdown=True if interrupted else None,
                )
                if interrupted:
                    logger.warning(
                        "任务已回退等待 job_id=%s attempt=%d 判定依据=关停中断，重启后继续",
                        job_id, attempt_no,
                    )
                else:
                    logger.warning(
                        "尝试失败将退避重试 job_id=%s attempt=%d/%d retry_in=%.3fs error=%s: %s"
                        " 判定依据=可重试错误且未超最大次数",
                        job_id, attempt_no, claimed.max_attempts,
                        not_before - now, error_type, error_message,
                    )
