"""基于 SQLite 的任务存储：幂等唯一键、原子抢占、状态机守卫、崩溃恢复。

设计要点
--------
* SQLite 文件位于本地磁盘（默认 ``.data/jobs.db``），无需任何外部服务；
  测试可传 ``:memory:``。WAL 模式保证进程异常退出后文件仍一致。
* ``business_key`` 在 jobs 表上有 UNIQUE 约束，重复提交由数据库兜底幂等。
* 抢占用单条条件 UPDATE 完成（``status IN (PENDING, RETRY_WAIT) AND
  not_before <= now``），天然互斥：无论多少 worker 并发，一个任务一次只会
  被一个 worker 抢到。
* 所有状态流转 SQL 都带 ``AND status = <期望前置态>``，构成状态机守卫，
  保证状态只能单调向后，不可能回退或被旧写入覆盖。
* 历史表 attempts 追加记录每次尝试的结果，状态流转全程可追溯。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from .models import (
    AttemptOutcome,
    AttemptRecord,
    FailureReason,
    Job,
    JobNotFoundError,
    JobStatus,
)

_TERMINAL = (JobStatus.SUCCEEDED.value, JobStatus.DEAD.value)


class JobStore:
    def __init__(self, path: str = ".data/jobs.db") -> None:
        self._path = path
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        self._conn = self._connect()
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            isolation_level=None,  # autocommit；事务用显式 BEGIN IMMEDIATE
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id                  TEXT PRIMARY KEY,
                business_key        TEXT NOT NULL UNIQUE,
                job_type            TEXT NOT NULL,
                payload             TEXT NOT NULL DEFAULT '{}',
                status              TEXT NOT NULL,
                attempts            INTEGER NOT NULL DEFAULT 0,
                max_attempts        INTEGER NOT NULL,
                attempt_timeout     REAL NOT NULL,
                retry_backoff_base  REAL NOT NULL,
                retry_backoff_factor REAL NOT NULL,
                retry_backoff_max   REAL NOT NULL,
                failure_reason      TEXT,
                last_error_type     TEXT,
                last_error_message  TEXT,
                result              TEXT,
                not_before          REAL NOT NULL DEFAULT 0,
                run_after_shutdown  INTEGER NOT NULL DEFAULT 0,
                created_at          REAL NOT NULL,
                updated_at          REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_due
                ON jobs(status, not_before);

            CREATE TABLE IF NOT EXISTS attempts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT NOT NULL REFERENCES jobs(id),
                attempt_no    INTEGER NOT NULL,
                outcome       TEXT NOT NULL,
                error_type    TEXT,
                error_message TEXT,
                started_at    REAL,
                finished_at   REAL
            );
            CREATE INDEX IF NOT EXISTS idx_attempts_job ON attempts(job_id, attempt_no);
            """
        )

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ #
    # 模型序列化
    # ------------------------------------------------------------------ #
    @staticmethod
    def _dump(value: Any) -> str | None:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, default=str)

    def _row_to_job(self, row: sqlite3.Row, history: list[AttemptRecord] | None = None) -> Job:
        return Job(
            id=row["id"],
            business_key=row["business_key"],
            job_type=row["job_type"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            attempt_timeout=row["attempt_timeout"],
            retry_backoff_base=row["retry_backoff_base"],
            retry_backoff_factor=row["retry_backoff_factor"],
            retry_backoff_max=row["retry_backoff_max"],
            failure_reason=FailureReason(row["failure_reason"]) if row["failure_reason"] else None,
            last_error_type=row["last_error_type"],
            last_error_message=row["last_error_message"],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            not_before=row["not_before"],
            run_after_shutdown=bool(row["run_after_shutdown"]),
            history=history if history is not None else [],
        )

    def _load_history(self, job_id: str) -> list[AttemptRecord]:
        rows = self._conn.execute(
            "SELECT attempt_no, outcome, error_type, error_message, started_at, finished_at "
            "FROM attempts WHERE job_id = ? ORDER BY attempt_no",
            (job_id,),
        ).fetchall()
        return [
            AttemptRecord(
                attempt_no=r["attempt_no"],
                outcome=AttemptOutcome(r["outcome"]),
                error_type=r["error_type"],
                error_message=r["error_message"],
                started_at=r["started_at"],
                finished_at=r["finished_at"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #
    def create_job_if_absent(self, job: Job) -> tuple[Job, bool]:
        """按 business_key 幂等创建。返回 (最新任务, 是否新建)。

        UNIQUE(business_key) 冲突时读取既有行返回，既不重复执行也不丢弃。
        """
        try:
            self._conn.execute(
                "INSERT INTO jobs (id, business_key, job_type, payload, status, attempts,"
                " max_attempts, attempt_timeout, retry_backoff_base, retry_backoff_factor,"
                " retry_backoff_max, failure_reason, last_error_type, last_error_message,"
                " result, not_before, run_after_shutdown, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job.id, job.business_key, job.job_type, self._dump(job.payload),
                    job.status.value, job.attempts, job.max_attempts, job.attempt_timeout,
                    job.retry_backoff_base, job.retry_backoff_factor, job.retry_backoff_max,
                    None, None, None, None, job.not_before, int(job.run_after_shutdown),
                    job.created_at, job.updated_at,
                ),
            )
            return job, True
        except sqlite3.IntegrityError:
            return self.get_job_by_business_key(job.business_key), False

    def record_attempt(
        self,
        job_id: str,
        attempt_no: int,
        outcome: AttemptOutcome,
        *,
        error_type: str | None,
        error_message: str | None,
        started_at: float,
        finished_at: float,
    ) -> None:
        self._conn.execute(
            "INSERT INTO attempts (job_id, attempt_no, outcome, error_type, error_message,"
            " started_at, finished_at) VALUES (?,?,?,?,?,?,?)",
            (job_id, attempt_no, outcome.value, error_type, error_message, started_at, finished_at),
        )

    def transition_to_wait(
        self,
        job_id: str,
        attempt_no: int,
        not_before: float,
        *,
        error_type: str | None,
        error_message: str | None,
        now: float,
        run_after_shutdown: bool | None = None,
    ) -> None:
        sets = ["status = ?", "not_before = ?", "last_error_type = ?",
                "last_error_message = ?", "updated_at = ?"]
        params: list[Any] = [
            JobStatus.RETRY_WAIT.value, not_before, error_type, error_message, now,
        ]
        if run_after_shutdown is not None:
            sets.append("run_after_shutdown = ?")
            params.append(int(run_after_shutdown))
        params += [job_id, JobStatus.RUNNING.value, attempt_no]
        cur = self._conn.execute(
            f"UPDATE jobs SET {', '.join(sets)}"
            " WHERE id = ? AND status = ? AND attempts = ?",
            params,
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"状态机守卫失败：job={job_id} attempt={attempt_no} 无法从 RUNNING 转为 RETRY_WAIT"
            )

    def transition_to_dead(
        self,
        job_id: str,
        attempt_no: int,
        reason: FailureReason,
        *,
        error_type: str | None,
        error_message: str | None,
        now: float,
    ) -> None:
        cur = self._conn.execute(
            "UPDATE jobs SET status = ?, failure_reason = ?, last_error_type = ?,"
            " last_error_message = ?, run_after_shutdown = 0, updated_at = ?"
            " WHERE id = ? AND status = ? AND attempts = ?",
            (JobStatus.DEAD.value, reason.value, error_type, error_message, now,
             job_id, JobStatus.RUNNING.value, attempt_no),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"状态机守卫失败：job={job_id} attempt={attempt_no} 无法从 RUNNING 转为 DEAD"
            )

    def transition_to_success(self, job_id: str, attempt_no: int, result: object, now: float) -> None:
        cur = self._conn.execute(
            "UPDATE jobs SET status = ?, result = ?, failure_reason = NULL,"
            " last_error_type = NULL, last_error_message = NULL,"
            " run_after_shutdown = 0, updated_at = ?"
            " WHERE id = ? AND status = ? AND attempts = ?",
            (JobStatus.SUCCEEDED.value, self._dump(result), now,
             job_id, JobStatus.RUNNING.value, attempt_no),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"状态机守卫失败：job={job_id} attempt={attempt_no} 无法从 RUNNING 转为 SUCCEEDED"
            )

    # ------------------------------------------------------------------ #
    # 读取 / 调度
    # ------------------------------------------------------------------ #
    def claim_due_job(self, now: float) -> Job | None:
        """原子抢占一个到期任务。

        单条事务内：选出最早到期的一个 PENDING/RETRY_WAIT 任务并置 RUNNING，
        行锁保证并发 worker 中只有一个能抢到。
        """
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT id, attempts FROM jobs"
                " WHERE status IN (?, ?) AND not_before <= ?"
                " ORDER BY not_before, rowid LIMIT 1",
                (JobStatus.PENDING.value, JobStatus.RETRY_WAIT.value, now),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            attempt_no = row["attempts"] + 1
            cur = conn.execute(
                "UPDATE jobs SET status = ?, attempts = ?, updated_at = ?"
                " WHERE id = ? AND status IN (?, ?)",
                (JobStatus.RUNNING.value, attempt_no, now, row["id"],
                 JobStatus.PENDING.value, JobStatus.RETRY_WAIT.value),
            )
            if cur.rowcount != 1:  # 理论不可达：行锁内没有竞争者
                conn.execute("ROLLBACK")
                return None
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        return self.get_job(row["id"])

    def get_job(self, job_id: str) -> Job:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise JobNotFoundError(job_id)
        return self._row_to_job(row, self._load_history(job_id))

    def get_job_by_business_key(self, business_key: str) -> Job:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE business_key = ?", (business_key,)
        ).fetchone()
        if row is None:
            raise JobNotFoundError(business_key)
        return self._row_to_job(row, self._load_history(row["id"]))

    def recover_on_startup(self, now: float) -> int:
        """把上次进程残留的 RUNNING 任务恢复为 RETRY_WAIT 并立即到期。

        崩溃/被 kill -9 时可能有任务停留在 RUNNING（attempts 已包含中断掉的
        那次尝试）。重启时把它们放回等待队列并追加一条 SHUTDOWN_INTERRUPTED
        历史，下一次以 attempts+1 继续，保证任务不丢失、不永远卡在 RUNNING、
        流转可追溯。返回恢复的任务数。
        """
        rows = self._conn.execute(
            "SELECT id, attempts FROM jobs WHERE status = ?",
            (JobStatus.RUNNING.value,),
        ).fetchall()
        for row in rows:
            self._conn.execute(
                "INSERT INTO attempts (job_id, attempt_no, outcome, error_type, error_message,"
                " started_at, finished_at) VALUES (?,?,?,?,?,?,?)",
                (row["id"], row["attempts"], AttemptOutcome.SHUTDOWN_INTERRUPTED.value,
                 "ShutdownInterrupted", "上次进程关停时该尝试仍在执行，已回收等待重试。",
                 None, now),
            )
        cur = self._conn.execute(
            "UPDATE jobs SET status = ?, not_before = ?, run_after_shutdown = 1, updated_at = ?"
            " WHERE status = ?",
            (JobStatus.RETRY_WAIT.value, now, now, JobStatus.RUNNING.value),
        )
        return cur.rowcount

    def has_unfinished(self) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM jobs WHERE status NOT IN (?, ?) LIMIT 1", _TERMINAL
        ).fetchone()
        return row is not None

    def list_unfinished(self) -> list[Job]:
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status NOT IN (?, ?) ORDER BY not_before, rowid",
            _TERMINAL,
        ).fetchall()
        return [self._row_to_job(r) for r in rows]
