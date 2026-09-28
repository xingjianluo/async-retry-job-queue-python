"""本地启动入口：uv run uvicorn main:app 或 python main.py"""
from __future__ import annotations

import os

import uvicorn

from jobqueue.api import build_default_app

app = build_default_app(
    db_path=os.environ.get("JOBQUEUE_DB", ".data/jobs.db"),
    max_workers=int(os.environ.get("JOBQUEUE_WORKERS", "4")),
    shutdown_timeout=float(os.environ.get("JOBQUEUE_SHUTDOWN_TIMEOUT", "15")),
)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
