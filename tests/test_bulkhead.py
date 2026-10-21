"""A hot coupon must not starve the service.

Every redeem for a coupon queues on that coupon's row lock. Without a cap, enough requests for
one hot coupon hold every pooled connection while they wait, and requests for *other* coupons
can't get a connection at all. The bulkhead caps in-flight transactions per coupon, so the
excess waits in the app (no connection, no lock) and the rest of the pool stays available.
"""
import asyncio
import time

import psycopg
import pytest

from tests.conftest import TEST_DB, redeem, seed

pytestmark = pytest.mark.concurrency

POOL = dict(db_pool_max=6, db_pool_timeout_s=1.0, lock_timeout_ms=8000, statement_timeout_ms=10000)


async def _flood_hot_coupon_then_try_other(client):
    await seed(client, "HOT", max_redemptions=100, type="STACKABLE")
    await seed(client, "OTHER", max_redemptions=100, type="STACKABLE")
    gate = await psycopg.AsyncConnection.connect(TEST_DB)
    await gate.execute("SELECT 1 FROM coupons WHERE code = 'HOT' FOR UPDATE")
    try:
        hot = [asyncio.create_task(redeem(client, "HOT", f"c{i}")) for i in range(12)]
        await asyncio.sleep(0.3)  # let the flood reach the lock queue
        started = time.monotonic()
        other = await redeem(client, "OTHER", "someone")
        other_latency = time.monotonic() - started
    finally:
        await gate.rollback()
        await gate.close()
    return other, other_latency, await asyncio.gather(*hot)


async def test_without_bulkhead_one_hot_coupon_starves_the_pool(make_client, db):
    """Demonstrates the problem: cap == pool size behaves like having no bulkhead."""
    client = await make_client(redeem_max_inflight_per_coupon=6, **POOL)
    other, _, _ = await _flood_hot_coupon_then_try_other(client)
    assert other.status_code == 503, "the unrelated coupon could not get a DB connection"


async def test_with_bulkhead_other_coupons_keep_flowing(make_client, db):
    client = await make_client(redeem_max_inflight_per_coupon=2, **POOL)
    other, latency, hot = await _flood_hot_coupon_then_try_other(client)
    assert other.status_code == 200 and latency < 0.5, f"{other.status_code} after {latency:.2f}s"
    assert all(r.status_code == 200 for r in hot), "the hot coupon's requests all complete once unlocked"


async def test_bulkhead_queue_timeout_is_retryable_and_persists_nothing(make_client, db):
    client = await make_client(redeem_max_inflight_per_coupon=1, redeem_queue_timeout_s=0.3, **POOL)
    await seed(client, "Q", max_redemptions=5, type="STACKABLE")
    gate = await psycopg.AsyncConnection.connect(TEST_DB)
    await gate.execute("SELECT 1 FROM coupons WHERE code = 'Q' FOR UPDATE")
    try:
        first = asyncio.create_task(redeem(client, "Q", "c1", "o1", key="k1"))
        await asyncio.sleep(0.1)
        queued = await redeem(client, "Q", "c2", "o2", key="k2")
    finally:
        await gate.rollback()
        await gate.close()
    assert queued.status_code == 503 and queued.json()["error"]["code"] == "RETRYABLE"
    assert queued.headers["Retry-After"] == "1"
    assert (await first).status_code == 200
    cur = await db.execute("SELECT count(*) FROM idempotency_keys WHERE key = 'k2'")
    assert (await cur.fetchone())[0] == 0
    assert (await redeem(client, "Q", "c2", "o2", key="k2")).status_code == 200  # retry works
