"""持久化层：基于标准库 sqlite3 的任务仓储（无外部依赖）。

设计要点：
- 单连接 + 进程内 RLock，所有读写串行化，配合 WAL 允许外部只读观察；
- business_key 上的 UNIQUE 约束 + INSERT OR IGNORE 保证提交幂等；
- claim_due 用单条 UPDATE...RETURNING 原子完成「挑选到期任务 + 抢占 + 计数」，
  多 worker / 多线程下也不会重复执行；
- save_result 按 version 乐观锁写回，保证状态机单调、结果不串任务。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from .models import (
    Job,
    JobQueueError,
    JobStatus,
    FailureReason,
    NON_TERMINAL,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    business_key    TEXT NOT NULL UNIQUE,
    task_type       TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL,
    max_retries     INTEGER NOT NULL,
    timeout_seconds REAL NOT NULL,
    run_after       REAL NOT NULL,
    last_error      TEXT,
    failure_reason  TEXT,
    result          TEXT,
    version         INTEGER NOT NULL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(status, run_after);
"""


class StaleVersionError(JobQueueError):
    """乐观锁冲突：任务已被其他执行者推进，本次写回失效。"""


class JobStore:
    """SQLite 任务仓储。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        # check_same_thread=False：worker 线程会共享连接，访问由 self._lock 串行化
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ 映射
    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            business_key=row["business_key"],
            task_type=row["task_type"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            timeout_seconds=row["timeout_seconds"],
            run_after=row["run_after"],
            last_error=row["last_error"],
            failure_reason=(
                FailureReason(row["failure_reason"])
                if row["failure_reason"]
                else None
            ),
            result=json.loads(row["result"]) if row["result"] is not None else None,
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # --------------------------------------------------------------- 幂等插入
    def insert_if_absent(self, job: Job) -> tuple[Job, bool]:
        """按 business_key 幂等插入，返回 (存储中的任务, 是否本次新建)。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """
                INSERT OR IGNORE INTO jobs(
                    id, business_key, task_type, payload, status, attempts,
                    max_retries, timeout_seconds, run_after, last_error,
                    failure_reason, result, version, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    job.id, job.business_key, job.task_type,
                    json.dumps(job.payload, ensure_ascii=False),
                    job.status.value, job.attempts, job.max_retries,
                    job.timeout_seconds, job.run_after, job.last_error,
                    job.failure_reason.value if job.failure_reason else None,
                    json.dumps(job.result, ensure_ascii=False)
                    if job.result is not None else None,
                    job.version, job.created_at, job.updated_at,
                ),
            )
            if cur.rowcount == 1:
                return job, True
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE business_key = ?", (job.business_key,)
            ).fetchone()
            return self._row_to_job(row), False

    # ------------------------------------------------------------------ 查询
    def get(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            return self._row_to_job(row) if row else None

    def get_by_business_key(self, business_key: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE business_key = ?", (business_key,)
            ).fetchone()
            return self._row_to_job(row) if row else None

    def list_non_terminal(self) -> list[Job]:
        placeholders = ",".join("?" for _ in NON_TERMINAL)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) "
                f"ORDER BY run_after",
                [s.value for s in NON_TERMINAL],
            ).fetchall()
            return [self._row_to_job(r) for r in rows]

    # ------------------------------------------------------------- 原子抢占
    def claim_due(self, limit: int, now: float) -> list[Job]:
        """抢占到期任务。

        一条 UPDATE 完成挑选 + 置 RUNNING + attempts+1，RETURNING 返回行。
        仅 PENDING / RETRY_WAIT 可被抢占，RUNNING 不会被第二个执行者拿到。
        """
        with self._lock, self._conn:
            rows = self._conn.execute(
                """
                UPDATE jobs SET
                    status = ?,
                    attempts = attempts + 1,
                    run_after = ?,
                    updated_at = ?,
                    version = version + 1
                WHERE id IN (
                    SELECT id FROM jobs
                    WHERE status IN (?, ?) AND run_after <= ?
                    ORDER BY run_after
                    LIMIT ?
                )
                RETURNING *
                """,
                (JobStatus.RUNNING.value, now, now,
                 JobStatus.PENDING.value, JobStatus.RETRY_WAIT.value,
                 now, limit),
            ).fetchall()
            return [self._row_to_job(r) for r in rows]

    # --------------------------------------------------------- 结果乐观写回
    def save_result(
        self,
        *,
        job_id: str,
        version: int,
        status: JobStatus,
        run_after: float,
        last_error: str | None,
        failure_reason: FailureReason | None,
        result: Any,
        now: float,
        decrement_attempt: bool = False,
    ) -> None:
        """按 version 乐观锁写回一次执行结果。

        decrement_attempt=True 时同时把 attempts 回退 1（停机中断：
        这次尝试没有结论，不消耗重试预算）。
        """
        attempts_sql = "attempts = attempts - 1, " if decrement_attempt else ""
        with self._lock, self._conn:
            cur = self._conn.execute(
                f"""
                UPDATE jobs SET
                    {attempts_sql}
                    status = ?, run_after = ?, last_error = ?,
                    failure_reason = ?, result = ?, updated_at = ?,
                    version = version + 1
                WHERE id = ? AND version = ?
                """,
                (
                    status.value, run_after, last_error,
                    failure_reason.value if failure_reason else None,
                    json.dumps(result, ensure_ascii=False)
                    if result is not None else None,
                    now, job_id, version,
                ),
            )
            if cur.rowcount != 1:
                raise StaleVersionError(
                    f"job {job_id} version={version} 写回冲突，结果丢弃"
                )

    # ------------------------------------------------------------- 停机恢复
    def reset_running_for_shutdown(self, now: float) -> list[str]:
        """把所有 RUNNING 重置为 RETRY_WAIT，attempts 回退（本次尝试不计入）。

        返回被重置的任务 id 列表。下次 start() 时这些任务立即重新可调度。
        """
        with self._lock, self._conn:
            rows = self._conn.execute(
                """
                UPDATE jobs SET
                    status = ?,
                    attempts = MAX(attempts - 1, 0),
                    run_after = ?,
                    last_error = COALESCE(last_error, ?),
                    updated_at = ?,
                    version = version + 1
                WHERE status = ?
                RETURNING id
                """,
                (
                    JobStatus.RETRY_WAIT.value, now,
                    "interrupted by graceful shutdown before attempt completed",
                    now, JobStatus.RUNNING.value,
                ),
            ).fetchall()
            return [r["id"] for r in rows]
