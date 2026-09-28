"""FastAPI 装配：提交/查询接口 + lifespan 生命周期管理。

关停信号（SIGTERM/SIGINT，或 uvicorn 的 reload/停止）由 FastAPI lifespan
统一接管：shutdown 阶段调用 ``queue.stop()``，完成优雅收尾。
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import handlers as demo_handlers
from .models import Job, JobNotFoundError, JobStatus, RetryPolicy
from .queue import JobQueue, QueueShuttingDown
from .registry import HandlerRegistry
from .storage import JobStore


class SubmitRequest(BaseModel):
    business_key: str = Field(min_length=1)
    job_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    attempt_timeout: float | None = Field(default=None, gt=0, le=3600)


class AttemptOut(BaseModel):
    attempt_no: int
    outcome: str
    error_type: str | None
    error_message: str | None
    started_at: float | None
    finished_at: float | None


class JobResponse(BaseModel):
    job_id: str
    business_key: str
    job_type: str
    status: JobStatus
    attempts: int
    max_attempts: int
    attempt_timeout: float
    failure_reason: str | None
    last_error_type: str | None
    last_error_message: str | None
    result: Any | None
    not_before: float
    run_after_shutdown: bool
    history: list[AttemptOut]


class SubmitResponse(BaseModel):
    job: JobResponse
    created: bool


def _to_response(job: Job) -> JobResponse:
    return JobResponse(
        job_id=job.id,
        business_key=job.business_key,
        job_type=job.job_type,
        status=job.status,
        attempts=job.attempts,
        max_attempts=job.max_attempts,
        attempt_timeout=job.attempt_timeout,
        failure_reason=job.failure_reason.value if job.failure_reason else None,
        last_error_type=job.last_error_type,
        last_error_message=job.last_error_message,
        result=job.result,
        not_before=job.not_before,
        run_after_shutdown=job.run_after_shutdown,
        history=[
            AttemptOut(
                attempt_no=a.attempt_no,
                outcome=a.outcome.value,
                error_type=a.error_type,
                error_message=a.error_message,
                started_at=a.started_at,
                finished_at=a.finished_at,
            )
            for a in job.history
        ],
    )


def create_app(queue: JobQueue, *, known_types: list[str] | None = None) -> FastAPI:
    """在给定队列实例上装配 HTTP 应用（测试可传入内存队列）。"""

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ANN202
        # SIGTERM/SIGINT 或 uvicorn 停止时进入 shutdown 分支，完成优雅收尾。
        await queue.start()
        try:
            yield
        finally:
            await queue.stop()

    app = FastAPI(title="async-retry-job-queue", version="0.1.0", lifespan=lifespan)
    app.state.queue = queue
    app.state.known_types = known_types

    @app.get("/")
    def read_root() -> dict[str, Any]:
        return {
            "service": "async-retry-job-queue",
            "job_types": known_types or queue.registry.types(),
            "endpoints": ["POST /jobs", "GET /jobs/{job_id}", "GET /jobs/by-key/{business_key}"],
        }

    @app.post("/jobs", response_model=SubmitResponse, status_code=202)
    async def submit_job(req: SubmitRequest) -> SubmitResponse:
        policy = RetryPolicy(
            max_attempts=req.max_attempts or RetryPolicy().max_attempts,
            attempt_timeout=req.attempt_timeout or RetryPolicy().attempt_timeout,
        )
        try:
            job, created = await queue.submit(
                req.business_key, req.job_type, req.payload, policy=policy
            )
        except QueueShuttingDown as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except KeyError as exc:  # HandlerNotFound
            raise HTTPException(status_code=400, detail=f"未知的 job_type: {exc.args[0]}") from exc
        # 提交后重新读取一次，拿到最新状态（可能已被 worker 执行）。
        latest = await queue.get_status(job.id)
        return SubmitResponse(job=_to_response(latest), created=created)

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    async def get_job(job_id: str) -> JobResponse:
        try:
            job = await queue.get_status(job_id)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"任务不存在: {job_id}") from exc
        return _to_response(job)

    @app.get("/jobs/by-key/{business_key}", response_model=JobResponse)
    async def get_job_by_key(business_key: str) -> JobResponse:
        try:
            job = await queue.get_status_by_key(business_key)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"任务不存在: {business_key}") from exc
        return _to_response(job)

    return app


def build_default_app(
    *,
    db_path: str = ".data/jobs.db",
    max_workers: int = 4,
    shutdown_timeout: float = 15.0,
) -> FastAPI:
    """本地一键启动：SQLite 文件存储 + 内置示例处理器。"""
    store = JobStore(db_path)
    registry = demo_handlers.default_registry()
    queue = JobQueue(
        store, registry, max_workers=max_workers, shutdown_timeout=shutdown_timeout
    )
    return create_app(queue, known_types=registry.types())
