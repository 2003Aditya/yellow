import hashlib
import json

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from app.bulkhead import Bulkhead
from app.db import retry_on_conflict
from app.errors import STATUS, ApiError, ErrorCode, error_body
from app.models import RedeemIn

CLAIM_KEY_SQL = """
INSERT INTO idempotency_keys (key, request_hash, response_status, response_body)
VALUES (%s, %s, 0, '{}'::jsonb)
ON CONFLICT (key) DO NOTHING
RETURNING key
"""

# Unlocked read. Every rejection it can produce is final for this request: expiry is decided
# by the txn timestamp and expires_at never changes, a STANDARD customer can never become
# eligible again ("once ever"), an order never loses its coupon row, and "sold out" is a
# committed state the request is simply ordered after.
PRECHECK_SQL = """
SELECT c.type, c.max_redemptions, c.redeemed_count, c.discount_percent, c.expires_at,
       now()                 AS evaluated_at,
       now() >= c.expires_at AS expired
FROM coupons c WHERE c.code = %s
"""

LOCK_SQL = "SELECT redeemed_count, max_redemptions FROM coupons WHERE code = %s FOR UPDATE"

# Run as its own statement *after* the lock is held. In READ COMMITTED a statement's snapshot
# is taken when the statement starts, so a subquery inside the FOR UPDATE statement itself
# would miss rows committed by the transaction we were queued behind.
USAGE_SQL = """
SELECT EXISTS (SELECT 1 FROM redemptions
               WHERE coupon_code = %(code)s AND customer_id = %(customer)s) AS customer_used,
       EXISTS (SELECT 1 FROM redemptions WHERE order_id = %(order)s)    AS order_used
"""

INSERT_REDEMPTION_SQL = """
INSERT INTO redemptions (coupon_code, coupon_type, customer_id, order_id, status, redeemed_at)
VALUES (%s, %s, %s, %s, 'ACTIVE', %s)
"""


def request_hash(req: RedeemIn) -> str:
    canonical = json.dumps(req.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _fail(code: ErrorCode, message: str) -> tuple[int, dict]:
    return STATUS[code], error_body(code, message)


def _reject(req: RedeemIn, coupon: dict, usage: dict, redeemed_count: int, max_redemptions: int):
    """Single source of truth for rule order, used before and after taking the lock:
    UNKNOWN (caller) -> EXPIRED -> ALREADY_REDEEMED_BY_CUSTOMER (STANDARD) -> ORDER_ALREADY_HAS_COUPON
    -> NO_REDEMPTIONS_LEFT. Customer- and order-specific answers come before "sold out" so the
    same request always gets the same error, whatever else is happening concurrently."""
    if coupon["expired"]:
        return _fail(ErrorCode.COUPON_EXPIRED, f"Coupon {req.code} expired at {coupon['expires_at'].isoformat()}")
    if coupon["type"] == "STANDARD" and usage["customer_used"]:
        return _fail(
            ErrorCode.ALREADY_REDEEMED_BY_CUSTOMER, f"Customer {req.customer_id} has already used coupon {req.code}"
        )
    if usage["order_used"]:
        return _fail(ErrorCode.ORDER_ALREADY_HAS_COUPON, f"Order {req.order_id} already has a coupon applied")
    if redeemed_count >= max_redemptions:
        return _fail(ErrorCode.NO_REDEMPTIONS_LEFT, f"Coupon {req.code} has no redemptions left")
    return None


@retry_on_conflict
async def redeem(pool: AsyncConnectionPool, bulkhead: Bulkhead, idem_key: str, req: RedeemIn) -> tuple[int, dict, bool]:
    """Redeem a coupon. Returns (http_status, body, replayed).

    One transaction covers: claiming the idempotency key, the rule checks, the row lock, the
    redemption insert, the counter update and storing the final response. Infrastructure
    failures roll all of it back (key included), so a client retry is always safe.
    """
    req_hash = request_hash(req)
    # Queue per coupon *before* taking a connection: waiting here holds no connection and no lock.
    async with bulkhead.slot(req.code), pool.connection() as conn, conn.transaction():
        # 1. Claim the key. A concurrent request holding the same key makes this INSERT wait
        #    on the unique index until that transaction commits or rolls back.
        claimed = await (await conn.execute(CLAIM_KEY_SQL, (idem_key, req_hash))).fetchone()
        if claimed is None:
            cur = await conn.execute(
                "SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE key = %s",
                (idem_key,),
            )
            stored_hash, status, body = await cur.fetchone()
            if stored_hash != req_hash:
                raise ApiError(
                    ErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "Idempotency-Key was already used with a different request body",
                )
            return status, body, True

        # 2. Business outcome (success or business error) is stored under the key.
        status, body = await _apply(conn, req)
        await conn.execute(
            "UPDATE idempotency_keys SET response_status = %s, response_body = %s WHERE key = %s",
            (status, Jsonb(body), idem_key),
        )
        return status, body, False


async def _apply(conn: AsyncConnection, req: RedeemIn) -> tuple[int, dict]:
    cur = conn.cursor(row_factory=dict_row)
    usage_params = {"code": req.code, "customer": req.customer_id, "order": req.order_id}

    # Phase 1, no lock: requests that are going to be rejected never queue on the hot row.
    # After a flash sale sells out, every later request is answered without touching the lock.
    coupon = await (await cur.execute(PRECHECK_SQL, (req.code,))).fetchone()
    if coupon is None:
        return _fail(ErrorCode.UNKNOWN_COUPON, f"Coupon {req.code} does not exist")
    usage = await (await cur.execute(USAGE_SQL, usage_params)).fetchone()
    rejection = _reject(req, coupon, usage, coupon["redeemed_count"], coupon["max_redemptions"])
    if rejection:
        return rejection

    # Phase 2, row lock: serialises every grant for this coupon across all instances.
    # now() is still the txn start time, so the expiry decision from phase 1 stands.
    locked = await (await cur.execute(LOCK_SQL, (req.code,))).fetchone()
    usage = await (await cur.execute(USAGE_SQL, usage_params)).fetchone()  # fresh snapshot after the lock
    rejection = _reject(req, coupon, usage, locked["redeemed_count"], locked["max_redemptions"])
    if rejection:
        return rejection

    try:
        async with conn.transaction():  # savepoint: a unique violation must not abort the txn
            await conn.execute(
                INSERT_REDEMPTION_SQL,
                (req.code, coupon["type"], req.customer_id, req.order_id, coupon["evaluated_at"]),
            )
    except errors.UniqueViolation as e:
        # Backstop for the one race the coupon lock can't cover: the same order_id being
        # redeemed against a *different* coupon at the same moment. The unique indexes decide.
        if e.diag.constraint_name == "uq_standard_once_per_customer":
            return _fail(
                ErrorCode.ALREADY_REDEEMED_BY_CUSTOMER,
                f"Customer {req.customer_id} has already used coupon {req.code}",
            )
        if e.diag.constraint_name == "redemptions_order_id_key":
            return _fail(ErrorCode.ORDER_ALREADY_HAS_COUPON, f"Order {req.order_id} already has a coupon applied")
        raise

    row = await (
        await conn.execute(
            "UPDATE coupons SET redeemed_count = redeemed_count + 1 WHERE code = %s "
            "RETURNING max_redemptions - redeemed_count",
            (req.code,),
        )
    ).fetchone()
    return 200, {
        "success": True,
        "code": req.code,
        "customer_id": req.customer_id,
        "order_id": req.order_id,
        "discount_percent": float(coupon["discount_percent"]),
        "remaining": row[0],
        "redeemed_at": coupon["evaluated_at"].isoformat(),
    }
