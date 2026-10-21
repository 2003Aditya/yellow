import asyncio

import pytest

from tests.conftest import assert_counter_consistent, redeem, seed


async def test_retry_replays_original_response(client, db):
    await seed(client, "IDEM", max_redemptions=5)
    first = await redeem(client, "IDEM", "c1", "o1", key="k-1")
    second = await redeem(client, "IDEM", "c1", "o1", key="k-1")
    assert first.status_code == second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json().pop("replayed") is True
    assert {k: v for k, v in second.json().items() if k != "replayed"} == first.json()
    assert await assert_counter_consistent(db, "IDEM") == 1


async def test_business_errors_are_replayed_too(client):
    await seed(client, "ONE", max_redemptions=1)
    await redeem(client, "ONE", "c1")
    first = await redeem(client, "ONE", "c2", "o2", key="k-err")
    assert first.json()["error"]["code"] == "NO_REDEMPTIONS_LEFT"
    replay = await redeem(client, "ONE", "c2", "o2", key="k-err")
    assert replay.status_code == 409 and replay.json()["replayed"] is True


async def test_same_key_different_body_rejected(client):
    await seed(client, "IDEM", max_redemptions=5)
    assert (await redeem(client, "IDEM", "c1", "o1", key="k-x")).status_code == 200
    resp = await redeem(client, "IDEM", "c2", "o2", key="k-x")
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


@pytest.mark.concurrency
async def test_concurrent_retries_same_key_redeem_once(client, db):
    await seed(client, "BURST", max_redemptions=100, type="STACKABLE")
    responses = await asyncio.gather(*[redeem(client, "BURST", "c1", "o1", key="same") for _ in range(50)])
    assert all(r.status_code == 200 for r in responses)
    assert sum(1 for r in responses if r.json().get("replayed")) == 49
    assert len({r.json()["redeemed_at"] for r in responses}) == 1
    assert await assert_counter_consistent(db, "BURST") == 1
