from tests.conftest import assert_counter_consistent, iso_in, redeem, seed


async def test_success_and_remaining(client, db):
    await seed(client, "SAVE10", max_redemptions=3)
    resp = await redeem(client, "save10", "c1", "o1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True and body["remaining"] == 2 and body["discount_percent"] == 10
    assert (await client.get("/coupons/SAVE10")).json()["redeemed_count"] == 1
    await assert_counter_consistent(db, "SAVE10")


async def test_unknown_code(client):
    resp = await redeem(client, "GHOST")
    assert resp.status_code == 404 and resp.json()["error"]["code"] == "UNKNOWN_COUPON"


async def test_expired(client):
    await seed(client, "OLD", expires_at=iso_in(-1))
    resp = await redeem(client, "OLD")
    assert resp.status_code == 410 and resp.json()["error"]["code"] == "COUPON_EXPIRED"


async def test_standard_once_per_customer(client):
    await seed(client, "STD", max_redemptions=10, type="STANDARD")
    assert (await redeem(client, "STD", "c1")).status_code == 200
    resp = await redeem(client, "STD", "c1")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER"
    assert (await redeem(client, "STD", "c2")).status_code == 200


async def test_standard_once_ever_even_after_cancel(client):
    await seed(client, "STD", max_redemptions=10, type="STANDARD")
    assert (await redeem(client, "STD", "c1", "o1")).status_code == 200
    assert (await client.post("/orders/o1/cancel")).json()["released"] is True
    resp = await redeem(client, "STD", "c1", "o2")
    assert resp.json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER"


async def test_stackable_no_per_customer_limit(client):
    await seed(client, "REF", max_redemptions=3, type="STACKABLE")
    for _ in range(3):
        assert (await redeem(client, "REF", "c1")).status_code == 200
    resp = await redeem(client, "REF", "c1")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "NO_REDEMPTIONS_LEFT"


async def test_no_redemptions_left(client):
    await seed(client, "ONE", max_redemptions=1)
    assert (await redeem(client, "ONE", "c1")).status_code == 200
    resp = await redeem(client, "ONE", "c2")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "NO_REDEMPTIONS_LEFT"


async def test_order_cannot_take_two_coupons(client):
    await seed(client, "A", type="STACKABLE")
    await seed(client, "B", type="STACKABLE")
    assert (await redeem(client, "A", "c1", "o1")).status_code == 200
    resp = await redeem(client, "B", "c1", "o1")
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "ORDER_ALREADY_HAS_COUPON"


async def test_error_precedence_expired_beats_exhausted(client, db):
    await seed(client, "P", max_redemptions=1)
    assert (await redeem(client, "P", "c1")).status_code == 200
    await db.execute("UPDATE coupons SET expires_at = now() - interval '1 second' WHERE code = 'P'")
    assert (await redeem(client, "P", "c2")).json()["error"]["code"] == "COUPON_EXPIRED"


async def test_missing_idempotency_key(client):
    await seed(client, "K")
    resp = await client.post("/redeem", json={"code": "K", "customer_id": "c", "order_id": "o"})
    assert resp.status_code == 400 and resp.json()["error"]["code"] == "IDEMPOTENCY_KEY_REQUIRED"


async def test_validation(client):
    resp = await client.post(
        "/redeem", json={"code": "K", "customer_id": "", "order_id": "o"}, headers={"Idempotency-Key": "k"}
    )
    assert resp.status_code == 422


async def test_standard_precedence_already_used_beats_sold_out(client):
    """Same request, same answer: a STANDARD customer who already used the coupon is told so,
    whether or not the coupon has since sold out."""
    await seed(client, "PREC", max_redemptions=2, type="STANDARD")
    assert (await redeem(client, "PREC", "c1")).status_code == 200
    assert (await redeem(client, "PREC", "c1")).json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER"
    assert (await redeem(client, "PREC", "c2")).status_code == 200  # now sold out
    assert (await redeem(client, "PREC", "c1")).json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER"
    assert (await redeem(client, "PREC", "c3")).json()["error"]["code"] == "NO_REDEMPTIONS_LEFT"


async def test_same_order_new_key_is_deterministic(client):
    """Client lost its key and resends the same order with a new one: never a second redemption."""
    await seed(client, "ORD", max_redemptions=5, type="STANDARD")
    assert (await redeem(client, "ORD", "c1", "o1")).status_code == 200
    resp = await redeem(client, "ORD", "c2", "o1")
    assert resp.json()["error"]["code"] == "ORDER_ALREADY_HAS_COUPON"
