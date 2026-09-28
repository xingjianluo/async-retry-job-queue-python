"""队列门面：JobQueue —— 幂等提交、并发调度、退避重试、死信与优雅关停。

并发模型：
- 一个调度线程周期性 claim_due（SQLite 原子抢占），把到期任务提交给
  有界 ThreadPoolExecutor，max_concurrency 即硬并发上限；
- 每个执行槽位在隔离子进程里跑处理器（见 executor），超时被杀，
  因此卡死的任务最多占用一个槽位到 timeout，不会拖垮其他任务；
- 状态写回带乐观锁 version，保证状态机单调、结果不会串任务。

关停模型（shutdown）：
1. closing 置位，submit 立即拒绝（QueueClosedError），调度线程停止抢占；
2. 等待仍在执行的任务收完尾（取结果/判定超时），写回最终状态或 RETRY_WAIT；
3. 兜底把仍停留在 RUNNING 的任务重置为 RETRY_WAIT（attempts 回退，
   不计重试）；PENDING / RETRY_WAIT 任务本来就在库里，下次 start 继续。
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from . import executor
from .backoff import Backoff, ExponentialBackoff
from .executor import AttemptOutcome, AttemptResult, TaskHandler, terminate_registered
from .models import (
    FailureReason,
    Job,
    JobQueueError,
    JobStatus,
    QueueClosedError,
)
from .storage import JobStore, StaleVersionError

logger = logging.getLogger("jobqueue")


class JobQueue:
    def __init__(
        self,
        store: JobStore | None = None,
        *,
        db_path: str = ":memory:",
        max_concurrency: int = 4,
        backoff: Backoff | None = None,
        poll_interval: float = 0.05,
        default_max_retries: int = 3,
        default_timeout_seconds: float = 30.0,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须 >= 1")
        self._store = store if store is not None else JobStore(db_path)
        self._max_concurrency = max_concurrency
        self._backoff: Backoff = backoff or ExponentialBackoff(
            base=0.2, factor=2.0, cap=30.0
        )
        self._poll_interval = poll_interval
        self._default_max_retries = default_max_retries
        self._default_timeout = default_timeout_seconds

        self._registry: dict[str, TaskHandler] = {}
        self._handlers_lock = threading.Lock()

        self._scheduler: threading.Thread | None = None
        self._pool: ThreadPoolExecutor | None = None
        self._running: set[Any] = set()   # 尚未完成（含写回）的 attempt，关停等待用
        self._slots: set[Any] = set()     # 占用并发槽位的 attempt，调度限额用
        self._inflight_lock = threading.Lock()

        self._closing = threading.Event()
        self._closed = threading.Event()
        self._drain_deadline: float | None = None  # 关停收尾预算，超时后强停在跑任务
        self._wakeup = threading.Event()
        self._terminal_events: dict[str, threading.Event] = {}
        self._terminal_lock = threading.Lock()

    # ------------------------------------------------------------- 处理器
    def register_handler(self, name: str, handler: TaskHandler) -> None:
        with self._handlers_lock:
            self._registry[name] = handler

    # ---------------------------------------------------------------- 启停
    def start(self) -> None:
        if self._scheduler is not None:
            return
        # 上次进程异常退出遗留在 RUNNING 的任务：立即重新可调度（attempts 回退）
        recovered = self._store.reset_running_for_shutdown(time.time())
        if recovered:
            logger.warning(
                "startup recovered stale RUNNING jobs (count=%d, ids=%s), "
                "reason=previous process did not finish them",
                len(recovered), recovered,
            )
        self._pool = ThreadPoolExecutor(
            max_workers=self._max_concurrency, thread_name_prefix="job-worker"
        )
        self._scheduler = threading.Thread(
            target=self._schedule_loop, name="job-scheduler", daemon=True
        )
        self._scheduler.start()
        logger.info(
            "job queue started, max_concurrency=%d, backoff=%s",
            self._max_concurrency, type(self._backoff).__name__,
        )

    @property
    def is_closing(self) -> bool:
        return self._closing.is_set()

    # ---------------------------------------------------------------- 提交
    def submit(
        self,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        business_key: str,
        max_retries: int | None = None,
        timeout_seconds: float | None = None,
    ) -> tuple[Job, bool]:
        """幂等提交。返回 (任务, 是否首次创建)。

        - 同一 business_key 重复提交（任何状态下）返回同一条任务与当前状态，
          既不会产生第二个执行，也不会报错丢弃；
        - 关停流程开始后提交抛 QueueClosedError。
        """
        if self._closing.is_set():
            raise QueueClosedError("队列正在关停，拒绝接收新任务")
        with self._handlers_lock:
            known = task_type in self._registry
        if not known:
            # 不强制注册（可能走 import 路径解析），但给出明确日志
            logger.warning(
                "submit business_key=%s task_type=%s has no locally registered "
                "handler; will attempt import resolution at execution",
                business_key, task_type,
            )
        now = time.time()
        job = Job(
            id=uuid.uuid4().hex,
            business_key=business_key,
            task_type=task_type,
            payload=dict(payload or {}),
            status=JobStatus.PENDING,
            attempts=0,
            max_retries=(
                self._default_max_retries if max_retries is None else max_retries
            ),
            timeout_seconds=(
                self._default_timeout if timeout_seconds is None else timeout_seconds
            ),
            run_after=now,
            version=0,
            created_at=now,
            updated_at=now,
        )
        stored, created = self._store.insert_if_absent(job)
        if created:
            logger.info(
                "job accepted job_id=%s business_key=%s task_type=%s "
                "max_retries=%d timeout=%.2fs verdict=created",
                stored.id, stored.business_key, stored.task_type,
                stored.max_retries, stored.timeout_seconds,
            )
            self._notify()  # 唤醒调度线程，尽快开跑
        else:
            logger.info(
                "duplicate submit rejected-as-duplicate job_id=%s business_key=%s "
                "status=%s attempts=%d verdict=idempotent_hit",
                stored.id, stored.business_key, stored.status.value, stored.attempts,
            )
        return stored, created

    # ---------------------------------------------------------------- 查询
    def get_job(self, job_id: str) -> Job | None:
        return self._store.get(job_id)

    def get_by_business_key(self, business_key: str) -> Job | None:
        return self._store.get_by_business_key(business_key)

    def wait_for_terminal(self, job_id: str, timeout: float = 5.0) -> Job:
        """阻塞等待任务到终态（供测试/同步场景使用）。"""
        with self._terminal_lock:
            job = self._store.get(job_id)
            if job is None:
                raise JobQueueError(f"unknown job {job_id}")
            if job.is_terminal:
                return job
            ev = self._terminal_events.get(job_id)
            if ev is None:
                ev = threading.Event()
                self._terminal_events[job_id] = ev
        if not ev.wait(timeout):
            raise TimeoutError(f"job {job_id} 在 {timeout}s 内未到达终态")
        latest = self._store.get(job_id)
        assert latest is not None
        return latest

    # ------------------------------------------------------------ 调度循环
    def _schedule_loop(self) -> None:
        assert self._pool is not None
        while not self._closing.is_set():
            try:
                self._dispatch_batch()
            except Exception:
                logger.exception("scheduler iteration failed; will retry")
            # 到期事件唤醒或超时轮询；关停时立即跳出
            self._wakeup.wait(self._poll_interval)
            self._wakeup.clear()
        logger.info("scheduler loop stopped accepting new claims")

    def _dispatch_batch(self) -> None:
        assert self._pool is not None
        with self._inflight_lock:
            free = self._max_concurrency - len(self._slots)
        if free <= 0:
            return
        now = time.time()
        jobs = self._store.claim_due(free, now)
        for job in jobs:
            future = self._pool.submit(self._run_one, job)
            with self._inflight_lock:
                self._slots.add(future)
                self._running.add(future)
            future.add_done_callback(self._future_done)
            logger.info(
                "job claimed job_id=%s business_key=%s attempt=%d",
                job.id, job.business_key, job.attempts,
            )

    def _future_done(self, future: Any) -> None:
        with self._inflight_lock:
            self._slots.discard(future)
            self._running.discard(future)
        # 槽位空出，立即唤醒调度（新任务可立刻补位，不必等 poll）
        self._notify()
        try:
            future.result()  # 暴露 worker 内部未预期异常到日志
        except Exception:
            logger.exception("worker crashed unexpectedly")

    # -------------------------------------------------------------- 执行流
    def _run_one(self, job: Job) -> None:
        """在工作线程中完成一次 attempt 并写回状态。"""
        attempt_no = job.attempts  # claim 时已 +1
        registry = dict(self._registry)
        # 正常执行按任务自身 timeout 收尾；关停时 drain 截止时刻动态生效
        result = executor.run_attempt(
            job.task_type, job.payload, job.timeout_seconds, registry,
            drain_deadline=lambda: self._drain_deadline,
        )
        self._commit(job, result, attempt_no)

    def _commit(self, job: Job, result: AttemptResult, attempt_no: int) -> None:
        """根据 attempt 结果与重试预算决定下一个状态，并原子写回。"""
        now = time.time()
        next_status: JobStatus
        run_after = now
        failure_reason: FailureReason | None = None
        error_msg = result.error
        retries_used = attempt_no - 1  # 已消耗的「重试次数」（不含首次执行）
        budget_left = job.max_retries - retries_used  # 还允许几次重试
        verdict: str

        if result.outcome is AttemptOutcome.SUCCESS:
            next_status, verdict = JobStatus.SUCCEEDED, "succeeded"
            error_msg = None
        elif result.outcome is AttemptOutcome.INTERRUPTED:
            # 停机中断：回退计数，立即重新可调度（下次进程 start 时执行）
            next_status = JobStatus.RETRY_WAIT
            run_after = now
            verdict = "interrupted_by_shutdown_requeued"
        elif result.outcome is AttemptOutcome.FATAL:
            next_status = JobStatus.DEAD
            failure_reason = FailureReason.NON_RETRYABLE
            verdict = "dead_non_retryable"
        elif budget_left <= 0:
            next_status = JobStatus.DEAD
            failure_reason = (
                FailureReason.TIMEOUT_RETRIES_EXHAUSTED
                if result.outcome is AttemptOutcome.TIMEOUT
                else FailureReason.RETRIES_EXHAUSTED
            )
            verdict = f"dead_{failure_reason.value.lower()}"
        else:
            # 还有重试预算：第 retries_used+1 次重试（即本次失败后的第几次）
            delay = self._backoff.delay_for(retries_used + 1)
            next_status = JobStatus.RETRY_WAIT
            run_after = now + delay
            verdict = f"retry_scheduled_in_{delay:.3f}s"

        try:
            self._store.save_result(
                job_id=job.id,
                version=job.version,
                status=next_status,
                run_after=run_after,
                last_error=error_msg,
                failure_reason=failure_reason,
                result=result.value if next_status is JobStatus.SUCCEEDED else None,
                now=now,
                decrement_attempt=(result.outcome is AttemptOutcome.INTERRUPTED),
            )
        except StaleVersionError:
            # 正常情况下一个任务同一时刻只有一个执行者；走到这里说明有外部
            # 推进，状态机保护让本次结果被拒绝，保证单调不覆盖。
            logger.warning(
                "job_id=%s attempt=%d commit rejected by optimistic lock, "
                "verdict=stale_version_dropped", job.id, attempt_no
            )
            return

        logger.info(
            "job resolved job_id=%s business_key=%s attempt=%d retries_used=%d "
            "max_retries=%d status=%s failure_reason=%s verdict=%s error=%s",
            job.id, job.business_key, attempt_no, retries_used, job.max_retries,
            next_status.value,
            failure_reason.value if failure_reason else "-",
            verdict, error_msg,
        )

        if next_status in (JobStatus.SUCCEEDED, JobStatus.DEAD):
            with self._terminal_lock:
                ev = self._terminal_events.pop(job.id, None)
            if ev is not None:
                ev.set()
        else:
            self._notify()  # 有 RETRY_WAIT 任务入队，到期需要被调度

    # ------------------------------------------------------------ 优雅关停
    def shutdown_idle(self) -> bool:
        """当前是否没有在飞任务（关停等待用）。"""
        with self._inflight_lock:
            return not self._running

    async def shutdown(self, timeout: float = 10.0) -> None:
        """优雅关停。可在任意线程事件循环中 await，亦可通过 shutdown_threadsafe。"""
        self._begin_shutdown(timeout)
        self._wait_drain(timeout)
        self._finish_shutdown()

    def shutdown_blocking(self, timeout: float = 10.0) -> None:
        """同步版本的优雅关停，供非 asyncio 场景（脚本/测试）使用。"""
        self._begin_shutdown(timeout)
        self._wait_drain(timeout)
        self._finish_shutdown()

    def _wait_drain(self, timeout: float) -> None:
        # 在跑任务按正常执行逻辑收尾（成功/失败/自身超时都照实写回）；
        # 到 drain 截止仍未结束的，由 run_attempt 按 kill_after 中断并重排。
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.shutdown_idle():
                return
            time.sleep(0.02)

    def _begin_shutdown(self, drain_timeout: float = 10.0) -> None:
        if self._closing.is_set():
            return
        self._drain_deadline = time.time() + drain_timeout
        with self._inflight_lock:
            inflight = len(self._running)
        logger.warning(
            "graceful shutdown begin: stop accepting new jobs, "
            "inflight=%d drain_timeout=%.2fs verdict=draining",
            inflight, drain_timeout,
        )
        self._closing.set()
        self._notify()

    def _finish_shutdown(self) -> None:
        if self._closed.is_set():
            return
        # 等调度线程退出
        if self._scheduler is not None:
            self._scheduler.join(timeout=2.0)
            self._scheduler = None
        if self._pool is not None:
            # worker 线程全部在 join 子进程，wait=True 会阻塞到 attempt 收尾
            self._pool.shutdown(wait=True)
            self._pool = None
        # 兜底：理论上 inflight 已空，不会有 RUNNING 残留；仍统一重置以防遗漏
        leftover = self._store.reset_running_for_shutdown(time.time())
        if leftover:
            logger.warning(
                "shutdown reset unfinished RUNNING jobs ids=%s "
                "verdict=requeued_for_next_start", leftover
            )
        terminate_registered(timeout=2.0)
        self._closed.set()
        logger.warning(
            "graceful shutdown complete: no job lost, pending/retry jobs remain "
            "persisted and will resume on next start, verdict=closed"
        )

    def close_store(self) -> None:
        self._store.close()

    # --------------------------------------------------------------- 辅助
    def _notify(self) -> None:
        self._wakeup.set()
