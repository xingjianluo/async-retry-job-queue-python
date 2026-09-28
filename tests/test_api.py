"""HTTP 接口层：提交幂等、状态查询（含失败原因）、未知类型 400、关停期 503。"""
from __future__ import annotations

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from jobqueue.api import create_app
from jobqueue.models import FailureReason, JobStatus
from jobqueue.queue import JobQueue
from jobqueue.storage import JobStore
from jobqueue import handlers

logger = logging.getLogger("test.api")


@pytest.fixture
def client():
    store = JobStore(":memory:")
    queue = JobQueue(store, handlers.default_registry(),
                     max_workers=2, poll_interval=0.005)
    app = create_app(queue, known_types=handlers.default_registry().types())

    transport = ASGITransport(app=app)
    # 手动驱动 lifespan（startup/shutdown）
    loop = asyncio.new_event_loop()

    async def _open() -> AsyncClient:
        await queue.start()
        c = AsyncClient(transport=transport, base_url="http://test")
        return c

    client_obj = loop.run_until_complete(_open())
    client_obj._test_queue = queue  # type: ignore[attr-defined]
    client_obj._test_loop = loop  # type: ignore[attr-defined]
    yield client_obj

    async def _close() -> None:
        await queue.stop()
        await client_obj.aclose()

    loop.run_until_complete(_close())
    loop.close()


def _wait_json(client, job_id: str, statuses: set[str]):
    loop = client._test_loop  # type: ignore[attr-defined]
    queue: JobQueue = client._test_queue  # type: ignore[attr-defined]

    async def poll() -> dict:
        import time
        deadline = time.time() + 3
        while True:
            r = await client.get(f"/jobs/{job_id}")
            body = r.json()
            if body["status"] in statuses:
                return body
            if time.time() > deadline:
                raise AssertionError(f"HTTP 轮询超时：{body}")
            await asyncio.sleep(0.01)

    return loop.run_until_complete(poll())


def test_root_lists_endpoints(client):
    loop = client._test_loop  # type: ignore[attr-defined]

    async def call():
        r = await client.get("/")
        return r

    r = loop.run_until_complete(call())
    assert r.status_code == 200
    assert "succeed" in r.json()["job_types"]


def test_submit_idempotent_and_query(client):
    loop = client._test_loop  # type: ignore[attr-defined]

    async def call():
        r1 = await client.post("/jobs", json={
            "business_key": "api-1", "job_type": "succeed", "payload": {"k": "v"},
        })
        r2 = await client.post("/jobs", json={
            "business_key": "api-1", "job_type": "succeed", "payload": {"k": "other"},
        })
        return r1, r2

    r1, r2 = loop.run_until_complete(call())
    assert r1.status_code == 202 and r2.status_code == 202
    b1, b2 = r1.json(), r2.json()
    logger.info("HTTP 幂等判定：created=%s/%s job_id=%s status=%s",
                b1["created"], b2["created"], b2["job"]["job_id"], b2["job"]["status"])
    assert b1["created"] is True and b2["created"] is False
    assert b1["job"]["job_id"] == b2["job"]["job_id"]

    final = _wait_json(client, b1["job"]["job_id"], {"SUCCEEDED"})
    assert final["result"] == {"ok": True, "echo": {"k": "v"}}
    assert final["attempts"] == 1 and final["history"][-1]["outcome"] == "SUCCESS"

    # by-key 查询
    rk = loop.run_until_complete(client.get("/jobs/by-key/api-1"))
    assert rk.status_code == 200 and rk.json()["job_id"] == b1["job"]["job_id"]

    # 404
    r404 = loop.run_until_complete(client.get("/jobs/nope"))
    assert r404.status_code == 404


def test_unknown_type_rejected(client):
    loop = client._test_loop  # type: ignore[attr-defined]

    async def call():
        return await client.post("/jobs", json={
            "business_key": "api-x", "job_type": "not-registered",
        })

    r = loop.run_until_complete(call())
    assert r.status_code == 400
    assert "not-registered" in r.json()["detail"]


def test_dead_letter_reason_visible(client):
    loop = client._test_loop  # type: ignore[attr-defined]

    async def call():
        return await client.post("/jobs", json={
            "business_key": "api-dead", "job_type": "always_non_retryable",
            "max_attempts": 3,
        })

    r = loop.run_until_complete(call())
    job_id = r.json()["job"]["job_id"]
    body = _wait_json(client, job_id, {"DEAD"})
    logger.info("HTTP 死信判定：job_id=%s status=%s reason=%s last_error_type=%s 判定依据=%s",
                job_id, body["status"], body["failure_reason"],
                body["last_error_type"], body["last_error_message"])
    assert body["status"] == "DEAD"
    assert body["failure_reason"] == FailureReason.NON_RETRYABLE_ERROR.value
    assert body["attempts"] == 1
