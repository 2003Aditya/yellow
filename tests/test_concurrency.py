import asyncio

import pytest

from tests.conftest import assert_counter_consistent, redeem, seed

pytestmark = pytest.mark.concurrency


async def test_flash_sale_never_over_redeems(client, db):
    await seed(client, "FLASH", max_redemptions=10, type="STANDARD")
    responses = await asyncio.gather(*[redeem(client, "FLASH", f"c{i}", f"o{i}") for i in range(200)])
    codes = [r.status_code for r in responses]
    assert codes.count(200) == 10
    assert all(r.json()["error"]["code"] == "NO_REDEMPTIONS_LEFT" for r in responses if r.status_code != 200)
    status = (await client.get("/coupons/FLASH")).json()
    assert (status["redeemed_count"], status["remaining"]) == (10, 0)
    assert await assert_counter_consistent(db, "FLASH") == 10


async def test_standard_same_customer_burst(client, db):
    await seed(client, "STD", max_redemptions=100, type="STANDARD")
    responses = await asyncio.gather(*[redeem(client, "STD", "c1", f"o{i}") for i in range(50)])
    assert sum(r.status_code == 200 for r in responses) == 1
    assert all(r.json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER" for r in responses if r.status_code != 200)
    assert await assert_counter_consistent(db, "STD") == 1


async def test_stackable_same_customer_burst_respects_global_cap(client, db):
    await seed(client, "REF", max_redemptions=25, type="STACKABLE")
    responses = await asyncio.gather(*[redeem(client, "REF", "c1", f"o{i}") for i in range(60)])
    assert sum(r.status_code == 200 for r in responses) == 25
    assert await assert_counter_consistent(db, "REF") == 25


async def test_get_is_exact_during_burst(client, db):
    await seed(client, "LIVE", max_redemptions=40, type="STACKABLE")
    seen = []

    async def poll():
        for _ in range(20):
            seen.append((await client.get("/coupons/LIVE")).json())
            await asyncio.sleep(0)

    await asyncio.gather(poll(), *[redeem(client, "LIVE", "c", f"o{i}") for i in range(40)])
    for snap in seen:
        assert snap["redeemed_count"] + snap["remaining"] == 40
        assert 0 <= snap["redeemed_count"] <= 40
    final = (await client.get("/coupons/LIVE")).json()
    assert final["redeemed_count"] == 40 == await assert_counter_consistent(db, "LIVE")
