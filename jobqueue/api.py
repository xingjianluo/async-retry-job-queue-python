"""FastAPI 装配层：把 JobQueue 暴露为 HTTP 接口，并通过 lifespan 管理启停。"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .handlers import echo
from .models import Job, QueueClosedError
from .queue import JobQueue


class SubmitRequest(BaseModel):
    task_type: str = Field(..., description="处理器名称，如 echo")
    payload: dict[str, Any] = Field(default_factory=dict)
    business_key: str = Field(..., description="业务幂等键，相同键只产生一个任务")
    max_retries: int | None = Field(default=None, ge=0)
    timeout_seconds: float | None = Field(default=None, gt=0)


def _job_to_dict(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "business_key": job.business_key,
        "task_type": job.task_type,
        "status": job.status.value,
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "run_after": job.run_after,
        "last_error": job.last_error,
        "failure_reason": (
            job.failure_reason.value if job.failure_reason else None
        ),
        "result": job.result,
        "version": job.version,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }


def create_app(queue: JobQueue | None = None) -> FastAPI:
    """构造 FastAPI 应用。

    queue=None 时用 JOBQUEUE_DB（默认本地文件 jobqueue.db）自建队列，
    随 lifespan 启动与优雅关停。测试可注入已构造好的 JobQueue。
    """
    owned = queue is None

    async def lifespan(app: FastAPI) -> Any:
        nonlocal queue
        if queue is None:
            db_path = os.environ.get("JOBQUEUE_DB", "jobqueue.db")
            queue = JobQueue(
                db_path=db_path,
                max_concurrency=int(os.environ.get("JOBQUEUE_CONCURRENCY", "4")),
                default_max_retries=int(
                    os.environ.get("JOBQUEUE_MAX_RETRIES", "3")
                ),
                default_timeout_seconds=float(
                    os.environ.get("JOBQUEUE_TIMEOUT", "30")
                ),
            )
        queue.register_handler("echo", echo)
        queue.start()
        app.state.queue = queue
        try:
            yield
        finally:
            # uvicorn 收到 SIGTERM/SIGINT 后会进入这里：拒绝新任务、收尾在跑任务
            await queue.shutdown(
                timeout=float(os.environ.get("JOBQUEUE_SHUTDOWN_TIMEOUT", "15"))
            )
            if owned:
                queue.close_store()

    app = FastAPI(title="async-retry-job-queue", lifespan=lifespan)

    @app.post("/jobs", status_code=202)
    def submit_job(req: SubmitRequest) -> dict[str, Any]:
        q: JobQueue = app.state.queue
        try:
            job, created = q.submit(
                req.task_type,
                req.payload,
                business_key=req.business_key,
                max_retries=req.max_retries,
                timeout_seconds=req.timeout_seconds,
            )
        except QueueClosedError as exc:
            # 关停期间：503，明确告知调用方未被接收，可稍后重试
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        body = _job_to_dict(job)
        body["created"] = created
        return body

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        q: JobQueue = app.state.queue
        job = q.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_to_dict(job)

    @app.get("/jobs")
    def get_job_by_key(business_key: str) -> dict[str, Any]:
        q: JobQueue = app.state.queue
        job = q.get_by_business_key(business_key)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_to_dict(job)

    @app.get("/health")
    def health() -> dict[str, Any]:
        q: JobQueue = app.state.queue
        return {"status": "draining" if q.is_closing else "running"}

    @app.get("/")
    def read_root() -> dict[str, str]:
        return {"service": "async-retry-job-queue"}

    return app
