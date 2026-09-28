"""pytest 公共夹具与日志配置。

要求：单测日志打印任务标识(job_id)、重试次数(attempts/retries_used)
与判定依据(verdict/failure_reason)。jobqueue 的 INFO 日志已包含这些字段，
这里把它们接到 pytest 实时输出，并给测试提供一个统一的断言记录器。
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import sys
from pathlib import Path

import pytest

from jobqueue.backoff import NoBackoff
from jobqueue.queue import JobQueue
from jobqueue.storage import JobStore

# fork 让 registry 注入与测试状态文件都简单可靠（测试处理器仍是顶层函数）
mp.set_start_method("fork", force=True)

_FORMAT = (
    "%(asctime)s %(levelname)s [%(name)s] %(message)s "
    "# fields: job_id/attempts/retries_used/verdict"
)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT))
    root = logging.getLogger("jobqueue")
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False


_configure_logging()
testlog = logging.getLogger("jobqueue.tests")


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "jobs.db")


def _register_all(q: JobQueue) -> None:
    from jobqueue.handlers import always_fail, always_fatal, echo, flaky, slow_handler
    from tests import testhandlers as th

    q.register_handler("echo", echo)
    q.register_handler("flaky", flaky)
    q.register_handler("always_fail", always_fail)
    q.register_handler("always_fatal", always_fatal)
    q.register_handler("slow_handler", slow_handler)
    q.register_handler("counting_handler", th.counting_handler)
    q.register_handler("concurrency_probe", th.concurrency_probe)
    q.register_handler("sleep_once", th.sleep_once)
    q.register_handler("fatal_handler", th.fatal_handler)


@pytest.fixture()
def make_queue(db_path: str):
    created: list[JobQueue] = []

    def _make(concurrency: int = 4, **kwargs) -> JobQueue:
        store = kwargs.pop("store", None) or JobStore(db_path)
        q = JobQueue(
            store,
            max_concurrency=concurrency,
            backoff=kwargs.pop("backoff", NoBackoff()),
            poll_interval=0.01,
            default_timeout_seconds=kwargs.pop("default_timeout_seconds", 5.0),
            **kwargs,
        )
        _register_all(q)
        q.start()
        created.append(q)
        return q

    yield _make

    for q in created:
        if not q._closed.is_set():
            q.shutdown_blocking(timeout=5.0)
        q.close_store()
