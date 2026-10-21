"""Seed sample coupons and walk through the main flows against a running server.

usage: python scripts/demo.py [seed|demo] [base_url]
"""
import json
import sys
import uuid

import httpx

COUPONS = [
    {"code": "FLASH50", "max_redemptions": 3, "discount_percent": 50, "expires_at": "2030-01-01T00:00:00Z", "type": "STANDARD"},
    {"code": "REFER10", "max_redemptions": 100, "discount_percent": 10, "expires_at": "2030-01-01T00:00:00Z", "type": "STACKABLE"},
]


def show(label: str, resp: httpx.Response) -> None:
    replayed = resp.headers.get("Idempotent-Replayed")
    suffix = f"  [Idempotent-Replayed: {replayed}]" if replayed else ""
    print(f"--- {label}: HTTP {resp.status_code}{suffix}\n{json.dumps(resp.json(), indent=2)}")


def seed(c: httpx.Client) -> None:
    for coupon in COUPONS:
        show(f"create {coupon['code']}", c.post("/coupons", json=coupon))


def demo(c: httpx.Client) -> None:
    seed(c)
    run = uuid.uuid4().hex[:6]
    o1, o2 = f"o1-{run}", f"o2-{run}"
    body = {"code": "flash50", "customer_id": f"alice-{run}", "order_id": o1}
    key = {"Idempotency-Key": f"demo-{run}"}
    show("redeem", c.post("/redeem", json=body, headers=key))
    show("retry with same Idempotency-Key", c.post("/redeem", json=body, headers=key))
    show("same key, different body", c.post("/redeem", json={**body, "order_id": o2}, headers=key))
    show("same customer again (STANDARD)", c.post("/redeem", json={**body, "order_id": o2}, headers={"Idempotency-Key": uuid.uuid4().hex}))
    show("unknown code", c.post("/redeem", json={**body, "code": "NOPE"}, headers={"Idempotency-Key": uuid.uuid4().hex}))
    show("status", c.get("/coupons/FLASH50"))
    show("cancel order", c.post(f"/orders/{o1}/cancel"))
    show("cancel order again", c.post(f"/orders/{o1}/cancel"))
    show("status", c.get("/coupons/FLASH50"))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "demo"
    base = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8000"
    with httpx.Client(base_url=base, timeout=10) as client:
        {"seed": seed, "demo": demo}[cmd](client)
