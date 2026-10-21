import asyncio

import pytest

from tests.conftest import assert_counter_consistent, redeem, seed


async def test_cancel_releases_slot_once(client, db):
    await seed(client, "C", max_redemptions=1)
    assert (await redeem(client, "C", "c1", "o1")).status_code == 200
    assert (await client.get("/coupons/C")).json()["remaining"] == 0

    first = await client.post("/orders/o1/cancel")
    second = await client.post("/orders/o1/cancel")
    assert first.json()["released"] is True and first.json()["remaining"] == 1
    assert second.status_code == 200 and second.json()["released"] is False
    assert second.json()["reason"] == "ALREADY_CANCELLED"
    assert (await client.get("/coupons/C")).json()["redeemed_count"] == 0
    await assert_counter_consistent(db, "C")

    # the freed slot is usable by someone else
    assert (await redeem(client, "C", "c2", "o2")).status_code == 200


async def test_cancel_order_without_coupon(client):
    resp = await client.post("/orders/nope/cancel")
    assert resp.status_code == 200 and resp.json()["reason"] == "NO_COUPON_ON_ORDER"


@pytest.mark.concurrency
async def test_concurrent_cancels_release_exactly_once(client, db):
    await seed(client, "CC", max_redemptions=5, type="STACKABLE")
    for i in range(3):
        await redeem(client, "CC", "c1", f"o{i}")
    responses = await asyncio.gather(*[client.post("/orders/o0/cancel") for _ in range(20)])
    assert sum(1 for r in responses if r.json()["released"]) == 1
    assert await assert_counter_consistent(db, "CC") == 2


@pytest.mark.concurrency
async def test_cancels_and_redeems_interleaved(client, db):
    await seed(client, "MIX", max_redemptions=10, type="STACKABLE")
    for i in range(10):
        await redeem(client, "MIX", f"c{i}", f"o{i}")
    tasks = [client.post(f"/orders/o{i}/cancel") for i in range(10)]
    tasks += [redeem(client, "MIX", f"n{i}", f"n{i}") for i in range(30)]
    await asyncio.gather(*tasks)
    count = await assert_counter_consistent(db, "MIX")
    assert count <= 10
