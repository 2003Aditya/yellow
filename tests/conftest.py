import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.migrate import migrate

ROOT = Path(__file__).resolve().parent.parent
TEST_DB = os.environ.get("TEST_DATABASE_URL", "postgresql://coupon:coupon@localhost:55432/coupons_test")


@pytest.fixture
async def db():
    """Direct autocommit connection for setup and invariant checks; wipes state per test."""
    async with await psycopg.AsyncConnection.connect(TEST_DB) as mconn:
        await migrate(mconn)
    conn = await psycopg.AsyncConnection.connect(TEST_DB, autocommit=True)
    await conn.execute("TRUNCATE redemptions, idempotency_keys, rate_limits, coupons RESTART IDENTITY CASCADE")
    yield conn
    await conn.close()


@pytest.fixture
async def make_client(db):
    async with AsyncExitStack() as stack:

        async def _make(**overrides) -> AsyncClient:
            settings = Settings(**{"database_url": TEST_DB, "rate_limit_enabled": False, "db_pool_max": 30, **overrides})
            app = create_app(settings)
            await stack.enter_async_context(app.router.lifespan_context(app))
            client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
            client.app = app
            return await stack.enter_async_context(client)

        yield _make


@pytest.fixture
async def client(make_client) -> AsyncClient:
    return await make_client()


def iso_in(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


async def seed(client, code="SAVE10", max_redemptions=5, type="STANDARD", discount=10, expires_at=None):
    resp = await client.post(
        "/coupons",
        json={
            "code": code,
            "max_redemptions": max_redemptions,
            "discount_percent": discount,
            "expires_at": expires_at or iso_in(3600),
            "type": type,
        },
    )
    assert resp.status_code in (200, 201), resp.text
    return resp


async def redeem(client, code="SAVE10", customer="c1", order=None, key=None):
    return await client.post(
        "/redeem",
        json={"code": code, "customer_id": customer, "order_id": order or f"o-{uuid.uuid4().hex[:8]}"},
        headers={"Idempotency-Key": key or uuid.uuid4().hex},
    )


async def assert_counter_consistent(db, code):
    """Invariant: the counter always equals the number of ACTIVE redemptions."""
    cur = await db.execute(
        "SELECT c.redeemed_count, (SELECT count(*) FROM redemptions r WHERE r.coupon_code = c.code AND r.status = 'ACTIVE') "
        "FROM coupons c WHERE c.code = %s",
        (code,),
    )
    counter, active = await cur.fetchone()
    assert counter == active, f"redeemed_count={counter} but active redemptions={active}"
    return counter


# --- two real app processes sharing one Postgres (multi-instance + DB-failure tests) ---

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def instances():
    """Boot two independent app processes and wait until both report ready."""
    procs, urls, logs = [], [], []
    for i in range(2):
        port = _free_port()
        log = tempfile.NamedTemporaryFile(prefix=f"app{i + 1}-", suffix=".log", delete=False)
        env = {
            **os.environ,
            "DATABASE_URL": TEST_DB,
            "RATE_LIMIT_ENABLED": "false",
            "DB_POOL_MAX": "20",
            "DB_POOL_TIMEOUT_S": "2",     # fail fast (503) while the DB is down
            # DB-level tests hold a row lock on purpose while inspecting pg_stat_activity; note
            # statement_timeout also bounds lock waits, so raise both for these instances.
            "LOCK_TIMEOUT_MS": "8000",
            "STATEMENT_TIMEOUT_MS": "10000",
            "INSTANCE_ID": f"app-{i + 1}",
        }
        procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
            )
        )
        urls.append(f"http://127.0.0.1:{port}")
        logs.append(log.name)

    deadline = time.monotonic() + 30
    try:
        for url, proc, log in zip(urls, procs, logs):
            while True:
                if proc.poll() is not None:
                    raise RuntimeError(f"instance {url} exited:\n{Path(log).read_text()}")
                try:
                    if httpx.get(f"{url}/readyz", timeout=1).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError(f"instance {url} not ready:\n{Path(log).read_text()}")
                time.sleep(0.2)
        yield urls
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait(timeout=10)


@pytest.fixture
async def http():
    async with httpx.AsyncClient(timeout=30, limits=httpx.Limits(max_connections=400)) as client:
        yield client
