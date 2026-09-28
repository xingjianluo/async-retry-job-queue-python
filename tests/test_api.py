"""HTTP 层集成测试：提交幂等、状态查询、关停后 503。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from jobqueue.backoff import NoBackoff
from jobqueue.models import FailureReason, JobStatus
from jobqueue.queue import JobQueue
from jobqueue.storage import JobStore
from tests.conftest import _register_all, testlog


@pytest.fixture()
def client(tmp_path):
    store = JobStore(str(tmp_path / "api.db"))
    q = JobQueue(store, max_concurrency=2, backoff=NoBackoff(),
                 poll_interval=0.01, default_timeout_seconds=5.0)
    _register_all(q)
    from jobqueue.api import create_app

    app = create_app(q)
    with TestClient(app) as c:
        c.queue = q  # type: ignore[attr-defined]
        yield c
    if not q._closed.is_set():
        q.shutdown_blocking(5)
    q.close_store()


def test_submit_duplicate_and_query(client):
    r1 = client.post("/jobs", json={
        "task_type": "echo", "payload": {"x": 1}, "business_key": "http-1",
    })
    assert r1.status_code == 202
    body1 = r1.json()
    assert body1["created"] is True
    job_id = body1["id"]

    r2 = client.post("/jobs", json={
        "task_type": "echo", "payload": {"x": 2}, "business_key": "http-1",
    })
    body2 = r2.json()
    testlog.info(
        "http duplicate job_id=%s created1=%s created2=%s status2=%s "
        "verdict=idempotent_over_http",
        job_id, body1["created"], body2["created"], body2["status"],
    )
    assert r2.status_code == 202
    assert body2["created"] is False
    assert body2["id"] == job_id

    final = client.queue.wait_for_terminal(job_id, 5)
    got = client.get(f"/jobs/{job_id}").json()
    by_key = client.get("/jobs", params={"business_key": "http-1"}).json()
    assert got["status"] == JobStatus.SUCCEEDED.value
    assert got["result"] == {"echo": {"x": 1}}
    assert by_key["id"] == job_id


def test_dead_reason_visible_over_http(client):
    r = client.post("/jobs", json={
        "task_type": "always_fatal", "business_key": "http-dead",
        "max_retries": 2,
    })
    job_id = r.json()["id"]
    final = client.queue.wait_for_terminal(job_id, 5)
    got = client.get(f"/jobs/{job_id}").json()
    testlog.info(
        "http dead letter job_id=%s status=%s attempts=%d failure_reason=%s "
        "verdict=reason_distinguishable_over_http",
        job_id, got["status"], got["attempts"], got["failure_reason"],
    )
    assert got["status"] == JobStatus.DEAD.value
    assert got["attempts"] == 1
    assert got["failure_reason"] == FailureReason.NON_RETRYABLE.value


def test_unknown_job_404(client):
    assert client.get("/jobs/nope").status_code == 404


def test_submit_after_shutdown_returns_503(client):
    client.queue.shutdown_blocking(3)
    r = client.post("/jobs", json={"task_type": "echo", "business_key": "late"})
    testlog.info(
        "http closed submit job_id=- attempts=- status_code=%d "
        "verdict=503_explicit_not_silent_drop", r.status_code,
    )
    assert r.status_code == 503
    assert client.get("/health").json()["status"] == "draining"
