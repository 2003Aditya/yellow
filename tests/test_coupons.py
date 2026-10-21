import asyncio

import psycopg
import pytest

from tests.conftest import iso_in, seed

PAYLOAD = {"code": "DUP", "max_redemptions": 5, "discount_percent": 10, "type": "STANDARD"}


async def test_create_then_get(client):
    resp = await seed(client, "save10", max_redemptions=5)
    assert resp.status_code == 201 and resp.json()["created"] is True

    body = (await client.get("/coupons/SAVE10")).json()
    assert (body["code"], body["redeemed_count"], body["remaining"], body["max_redemptions"]) == ("SAVE10", 0, 5, 5)
    assert body["expired"] is False


async def test_code_is_case_insensitive(client):
    await seed(client, "  Mixed10 ")
    assert (await client.get("/coupons/mixed10")).status_code == 200


async def test_duplicate_code_with_different_definition_is_rejected(client, db):
    expires = iso_in(3600)
    assert (await client.post("/coupons", json={**PAYLOAD, "expires_at": expires})).status_code == 201
    for change in ({"max_redemptions": 50}, {"discount_percent": 25}, {"type": "STACKABLE"}, {"code": "dup"}):
        resp = await client.post("/coupons", json={**PAYLOAD, "expires_at": expires, **change})
        if change == {"code": "dup"}:  # same code in another case, same definition -> idempotent re-send
            assert resp.status_code == 200 and resp.json()["created"] is False
            continue
        assert resp.status_code == 409, change
        assert resp.json()["error"]["code"] == "COUPON_ALREADY_EXISTS"
    cur = await db.execute("SELECT count(*), max(max_redemptions) FROM coupons")
    assert await cur.fetchone() == (1, 5), "original coupon untouched, no second row"


async def test_identical_resend_is_idempotent(client):
    expires = iso_in(3600)
    first = await client.post("/coupons", json={**PAYLOAD, "expires_at": expires})
    again = await client.post("/coupons", json={**PAYLOAD, "expires_at": expires})
    assert (first.status_code, again.status_code) == (201, 200)
    assert again.json()["created"] is False


@pytest.mark.concurrency
async def test_concurrent_creates_of_same_code_yield_exactly_one_coupon(client, db):
    expires = iso_in(3600)
    responses = await asyncio.gather(
        *[client.post("/coupons", json={**PAYLOAD, "max_redemptions": i + 1, "expires_at": expires}) for i in range(30)]
    )
    assert sorted(r.status_code for r in responses).count(201) == 1
    assert all(r.json()["error"]["code"] == "COUPON_ALREADY_EXISTS" for r in responses if r.status_code != 201)
    cur = await db.execute("SELECT count(*) FROM coupons WHERE code = 'DUP'")
    assert (await cur.fetchone())[0] == 1


async def test_database_rejects_duplicate_codes_on_its_own(db):
    """No app involved: the schema alone refuses a second coupon with the same code, in any case."""
    insert = ("INSERT INTO coupons (code, type, max_redemptions, discount_percent, expires_at) "
              "VALUES (%s, 'STANDARD', 1, 10, now() + interval '1 day')")
    await db.execute(insert, ("SAVE10",))
    with pytest.raises(psycopg.errors.UniqueViolation):
        await db.execute(insert, ("SAVE10",))
    with pytest.raises(psycopg.errors.CheckViolation):  # non-canonical spelling can't sneak in
        await db.execute(insert, ("save10",))
    with pytest.raises(psycopg.errors.CheckViolation):
        await db.execute(insert, (" SAVE10",))


async def test_validation_errors(client):
    bad = [
        {"code": "X", "max_redemptions": 0, "discount_percent": 10, "expires_at": iso_in(60), "type": "STANDARD"},
        {"code": "X", "max_redemptions": 1, "discount_percent": 101, "expires_at": iso_in(60), "type": "STANDARD"},
        {"code": "X", "max_redemptions": 1, "discount_percent": 10, "expires_at": "2030-01-01T00:00:00", "type": "STANDARD"},
        {"code": "X", "max_redemptions": 1, "discount_percent": 10, "expires_at": iso_in(60), "type": "BOGUS"},
        {"code": "  ", "max_redemptions": 1, "discount_percent": 10, "expires_at": iso_in(60), "type": "STANDARD"},
    ]
    for payload in bad:
        resp = await client.post("/coupons", json=payload)
        assert resp.status_code == 422, payload
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_get_unknown(client):
    resp = await client.get("/coupons/NOPE")
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "UNKNOWN_COUPON"
