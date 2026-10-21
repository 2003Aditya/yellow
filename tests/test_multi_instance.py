"""The required concurrency proof, run against TWO SEPARATE app processes sharing one Postgres.

Each instance is its own `uvicorn` OS process with its own connection pool and memory, and
requests are spread across both over real HTTP. If correctness depended on an in-process lock
or cache, these tests would fail.
"""
import asyncio
import itertools
import uuid

import httpx
import pytest

from tests.conftest import assert_counter_consistent, iso_in

pytestmark = [pytest.mark.multiinstance, pytest.mark.concurrency]


def _code(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:6]}".upper()  # unique per test: each process caches metadata


async def _seed(http, url, code, max_redemptions, type="STANDARD"):
    resp = await http.post(
        f"{url}/coupons",
        json={"code": code, "max_redemptions": max_redemptions, "discount_percent": 20,
              "expires_at": iso_in(3600), "type": type},
    )
    assert resp.status_code == 201, resp.text


async def _redeem(http, url, code, customer, order, key):
    return await http.post(
        f"{url}/redeem",
        json={"code": code, "customer_id": customer, "order_id": order},
        headers={"Idempotency-Key": key},
    )


async def test_two_processes_are_really_separate(instances, http):
    ids = {(await http.get(f"{u}/healthz")).headers["X-Instance-ID"] for u in instances}
    assert ids == {"app-1", "app-2"}


async def test_burst_over_capacity_across_instances_is_exact(instances, http, db):
    """300 simultaneous redeems for 25 slots, alternating between the two processes."""
    code, cap, n = _code("FLASH"), 25, 300
    await _seed(http, instances[0], code, cap)

    targets = itertools.cycle(instances)
    responses = await asyncio.gather(
        *[_redeem(http, next(targets), code, f"cust-{i}", f"ord-{i}", f"key-{i}") for i in range(n)]
    )

    ok = [r for r in responses if r.status_code == 200]
    rejected = [r for r in responses if r.status_code != 200]
    assert len(ok) == cap
    assert len(rejected) == n - cap
    assert {r.json()["error"]["code"] for r in rejected} == {"NO_REDEMPTIONS_LEFT"}
    assert {r.headers["X-Instance-ID"] for r in ok} == {"app-1", "app-2"}, "both processes must have granted slots"

    # every instance reports the same exact number, and it matches the rows on disk
    for url in instances:
        status = (await http.get(f"{url}/coupons/{code}")).json()
        assert (status["redeemed_count"], status["remaining"], status["max_redemptions"]) == (cap, 0, cap)
    assert await assert_counter_consistent(db, code) == cap
    remaining_seen = sorted(r.json()["remaining"] for r in ok)
    assert remaining_seen == list(range(cap)), "each slot handed out exactly once"


async def test_same_idempotency_key_retried_is_charged_once(instances, http, db):
    """One request plus retries with the same Idempotency-Key, sent to both processes."""
    code = _code("IDEM")
    await _seed(http, instances[0], code, 10)
    key, body = f"retry-{uuid.uuid4().hex}", ("alice", "order-1")

    # (a) the brief's case: original then the same request retried twice, hitting different instances
    first = await _redeem(http, instances[0], code, *body, key)
    retry1 = await _redeem(http, instances[1], code, *body, key)
    retry2 = await _redeem(http, instances[0], code, *body, key)
    assert first.status_code == retry1.status_code == retry2.status_code == 200
    assert "replayed" not in first.json()
    for retry in (retry1, retry2):
        assert retry.json()["replayed"] is True and retry.headers["Idempotent-Replayed"] == "true"
        assert {k: v for k, v in retry.json().items() if k != "replayed"} == first.json()

    # (b) harder: the "client timed out" case — retries race the original, in flight on both processes
    key2 = f"race-{uuid.uuid4().hex}"
    racing = await asyncio.gather(
        *[_redeem(http, instances[i % 2], code, "bob", "order-2", key2) for i in range(20)]
    )
    assert all(r.status_code == 200 for r in racing)
    assert sum("replayed" not in r.json() for r in racing) == 1
    assert len({r.json()["redeemed_at"] for r in racing}) == 1

    # charged once per key: 2 keys -> exactly 2 redemptions
    cur = await db.execute("SELECT order_id FROM redemptions WHERE coupon_code = %s ORDER BY order_id", (code,))
    assert [r[0] for r in await cur.fetchall()] == ["order-1", "order-2"]
    assert await assert_counter_consistent(db, code) == 2
    assert (await http.get(f"{instances[1]}/coupons/{code}")).json()["redeemed_count"] == 2


async def test_cancel_twice_returns_slot_once(instances, http, db):
    code = _code("CANCEL")
    await _seed(http, instances[0], code, 3)
    for i in range(3):
        assert (await _redeem(http, instances[i % 2], code, f"c{i}", f"o{i}", f"k{i}")).status_code == 200
    assert (await http.get(f"{instances[0]}/coupons/{code}")).json()["remaining"] == 0

    # (a) the brief's case: cancel the same order twice, once on each instance
    first = await http.post(f"{instances[0]}/orders/o0/cancel")
    second = await http.post(f"{instances[1]}/orders/o0/cancel")
    assert first.json()["released"] is True
    assert second.status_code == 200 and second.json()["released"] is False
    assert second.json()["reason"] == "ALREADY_CANCELLED"
    assert await assert_counter_consistent(db, code) == 2

    # (b) harder: 20 simultaneous cancels of another order, split across both processes
    racing = await asyncio.gather(*[http.post(f"{instances[i % 2]}/orders/o1/cancel") for i in range(20)])
    assert sum(r.json()["released"] for r in racing) == 1
    assert await assert_counter_consistent(db, code) == 1

    for url in instances:
        status = (await http.get(f"{url}/coupons/{code}")).json()
        assert (status["redeemed_count"], status["remaining"]) == (1, 2)

    # the returned slots are genuinely reusable — and only those two
    refill = await asyncio.gather(
        *[_redeem(http, instances[i % 2], code, f"n{i}", f"n{i}", f"nk{i}") for i in range(10)]
    )
    assert sum(r.status_code == 200 for r in refill) == 2
    assert await assert_counter_consistent(db, code) == 3
