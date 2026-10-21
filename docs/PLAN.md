> Original implementation plan, approved on 2026-10-21. Kept for history; later changes (create-only coupons instead of upsert, rate limits on every endpoint, per-coupon bulkhead, migrations) are described in README.md and ARCHITECTURE.md.

# Coupon Redemption Service — Final Plan

## Context
This is an AI-assisted backend coding round: build a coupon redemption service for e-commerce checkout, starting from an empty directory (`/Users/apple/yellow`). The grade depends on correctness under concurrency (flash-sale bursts, per-customer limits, consistent expiry, cancels that run only once, retry-safe idempotency), and also on production qualities: **rate limiting, caching, fail-safety, duplicate handling and upserts**.

**Stack (decided):** Python 3 + FastAPI, **PostgreSQL 16** (the only datastore; **no Redis**), docker-compose, psycopg 3 async + psycopg_pool, raw SQL (no ORM, so locking stays explicit), pytest + httpx, **Makefile** for every workflow.

**Core principles**
1. **Postgres alone decides correctness.** Each invariant is enforced in one transaction, using row locks, unique constraints, CHECK constraints and the DB clock.
2. **The cache is only an accelerator** (in-process, metadata only). The **rate limiter is a Postgres table** written with an upsert. If either one fails, the service stays correct.
3. **Fail closed on money.** If the DB can't confirm a redemption, it is never granted. Every error path either commits the full result or rolls back to nothing, so there are no partial states.

---

## Data model (`app/schema.sql`, applied idempotently on startup with `CREATE ... IF NOT EXISTS`)

```sql
coupons (
  code             text PRIMARY KEY,          -- normalised: trim + UPPER, so 'save10' == 'SAVE10'
  type             text NOT NULL CHECK (type IN ('STANDARD','STACKABLE')),
  max_redemptions  int  NOT NULL CHECK (max_redemptions > 0),
  redeemed_count   int  NOT NULL DEFAULT 0,
  discount_percent numeric(5,2) NOT NULL CHECK (discount_percent > 0 AND discount_percent <= 100),
  expires_at       timestamptz NOT NULL,
  version          int  NOT NULL DEFAULT 1,   -- bumped on every upsert; used as the cache key version
  created_at / updated_at timestamptz,
  CHECK (redeemed_count >= 0 AND redeemed_count <= max_redemptions)   -- DB-level last line of defence
)

redemptions (
  id bigserial PK, coupon_code text FK, coupon_type text, customer_id text NOT NULL,
  order_id text NOT NULL UNIQUE,              -- one coupon per order; duplicate orders rejected by the DB
  status text CHECK (status IN ('ACTIVE','CANCELLED')),
  redeemed_at timestamptz NOT NULL, cancelled_at timestamptz
)
CREATE UNIQUE INDEX uq_standard_once_per_customer
  ON redemptions (coupon_code, customer_id) WHERE coupon_type = 'STANDARD';

idempotency_keys (
  key text PRIMARY KEY, request_hash text NOT NULL,
  response_status int NOT NULL, response_body jsonb NOT NULL,
  created_at timestamptz DEFAULT now()
)

rate_limits (                                 -- fixed-window counters, maintained by upsert
  bucket text PRIMARY KEY,                    -- e.g. 'redeem:cust:C1', 'ip:1.2.3.4'
  window_start timestamptz NOT NULL,
  hits int NOT NULL
)
```

---

## Duplicate handling: one layer per kind of duplicate

| Duplicate | Guard |
|---|---|
| Same coupon seeded twice | `INSERT ... ON CONFLICT (code) DO UPDATE` (an upsert), plus code normalisation |
| Same redeem request retried, including concurrent retries | `idempotency_keys` PK + `INSERT ... ON CONFLICT DO NOTHING`, replaying the stored response |
| Same key, different body | `request_hash` mismatch → `422 IDEMPOTENCY_KEY_REUSED` |
| STANDARD coupon, same customer | partial unique index → `409 ALREADY_REDEEMED_BY_CUSTOMER` |
| Same order, a second coupon | `order_id UNIQUE` → `409 ORDER_ALREADY_HAS_COUPON` |
| Over-redemption | `FOR UPDATE` row lock + count check + CHECK constraint |
| Double cancel, including concurrent cancels | conditional `UPDATE ... WHERE status='ACTIVE'` → the second cancel is a no-op |

---

## Endpoints

### `POST /coupons`: an **upsert**
```sql
INSERT INTO coupons (...) VALUES (...)
ON CONFLICT (code) DO UPDATE SET
  discount_percent = EXCLUDED.discount_percent,
  expires_at       = EXCLUDED.expires_at,
  max_redemptions  = EXCLUDED.max_redemptions,
  version          = coupons.version + 1, updated_at = now()
WHERE coupons.type = EXCLUDED.type                       -- type can't change after creation
  AND EXCLUDED.max_redemptions >= coupons.redeemed_count -- can't shrink below slots already used
RETURNING *, (xmax = 0) AS inserted;
```
- `201` when created, `200` when updated. If no row comes back, the WHERE guard failed → `409 COUPON_CONFLICT` with the reason.
- Re-sending the same seed is harmless, so the call is idempotent by nature.
- After commit, the cache entry for the code is invalidated.
- Pydantic validation: timezone-aware `expires_at`, type enum, numeric ranges → `422`.

### `POST /redeem`: one transaction, READ COMMITTED
0. Middleware: request-ID, **rate limit** (see below), missing `Idempotency-Key` → `400 IDEMPOTENCY_KEY_REQUIRED`. The body is validated and the code normalised.
2. `INSERT INTO idempotency_keys ... ON CONFLICT (key) DO NOTHING RETURNING key`.
   - Concurrent same-key requests block on the unique index until the first one commits. That gives in-flight dedupe with no extra state machine.
   - On conflict: same hash → replay the stored status and body with `Idempotent-Replayed: true` / `"replayed": true`. Different hash → `422 IDEMPOTENCY_KEY_REUSED`.
3. `SELECT *, now() AS evaluated_at FROM coupons WHERE code=$1 FOR UPDATE`. This row lock serialises redemptions of one coupon. No row → `404 UNKNOWN_COUPON`.
4. **Expiry:** `now() >= expires_at` → `410 COUPON_EXPIRED`. Postgres `now()` is the transaction start time on one clock (the DB's), so the decision doesn't depend on lock wait time or on any app server's clock. The exact instant counts as expired. The same timestamp is stored as `redeemed_at`, so the decision can be audited.
5. **Capacity:** `redeemed_count >= max_redemptions` → `409 NO_REDEMPTIONS_LEFT`.
6. INSERT into `redemptions` inside a SAVEPOINT. A unique violation maps to `ALREADY_REDEEMED_BY_CUSTOMER` or `ORDER_ALREADY_HAS_COUPON`. Then `UPDATE coupons SET redeemed_count = redeemed_count + 1`.
7. UPDATE the idempotency row with the final response (success **or** business error) → COMMIT.
   - Infrastructure failures (timeouts, DB down) roll back everything, key included, and return `503 RETRYABLE` with `Retry-After`. A retry is then safe because nothing landed.

Success: `200 { success: true, code, order_id, discount_percent, remaining }`.
Errors (uniform): `{ success: false, error: { code, message, request_id } }`.
Precedence: UNKNOWN → EXPIRED → NO_REDEMPTIONS_LEFT → ALREADY_REDEEMED / ORDER_ALREADY_HAS_COUPON.

### `POST /orders/:order_id/cancel`: one transaction
1. Look up `coupon_code` by `order_id`. No row → `200 { released:false, reason:"NO_COUPON_ON_ORDER" }`.
2. `SELECT ... FROM coupons WHERE code=$c FOR UPDATE`. This uses the **same lock order as redeem** (coupon, then redemption), so the two paths can't deadlock.
3. `UPDATE redemptions SET status='CANCELLED', cancelled_at=now() WHERE order_id=$1 AND status='ACTIVE' RETURNING id`.
   - 1 row → decrement `redeemed_count` → `200 { released:true, remaining }`.
   - 0 rows → `200 { released:false, reason:"ALREADY_CANCELLED" }`. Calling cancel twice, even concurrently, is a no-op the second time.

### `GET /coupons/:code`
- `redeemed_count` and `remaining` are **always read from Postgres**, from the same row the redeem transaction updates, so the numbers are exact at every moment.
- Static fields (type, discount, expires_at, max) come from the cache when available.
- `404` if the code is unknown.

### `GET /healthz` (liveness) and `GET /readyz` (DB reachable)

---

## Caching (in-process, cache-aside, no Redis)
- **What's cached:** coupon *metadata* only (type, discount, expires_at, max, version), in an in-process TTL dict (`app/cache.py`, 30s TTL, size-bounded).
- **What's never cached: `redeemed_count` and `remaining`.** The brief requires "correct at all times", and any cached counter has a stale window. A PK read in Postgres is cheap. The README will state this trade-off.
- **Where it's used:** only for the static fields of `GET /coupons/:code`. `POST /redeem` never consults the cache, because it must lock the row in the DB anyway, so a cache adds no safety there and only adds risk.
- **Invalidation:** after an upsert commits, the code's entry is evicted. With multiple instances, another instance's static fields can be stale for up to the 30s TTL. The counts are always live, and a production system would add LISTEN/NOTIFY invalidation.

## Rate limiting (Postgres, fixed window via upsert)
```sql
INSERT INTO rate_limits (bucket, window_start, hits) VALUES ($1, now(), 1)
ON CONFLICT (bucket) DO UPDATE SET
  window_start = CASE WHEN rate_limits.window_start < now() - $window THEN now() ELSE rate_limits.window_start END,
  hits         = CASE WHEN rate_limits.window_start < now() - $window THEN 1 ELSE rate_limits.hits + 1 END
RETURNING hits, window_start;
```
- The statement is atomic and works across instances. It runs in its **own short transaction** before the redeem transaction.
- `POST /redeem`: per `customer_id` (default 10 / 10s) and per client IP (default 100 / 10s). Other endpoints: per IP.
- Over the limit → `429 RATE_LIMITED` with `Retry-After`. It is not stored under the idempotency key, so a later retry with the same key works.
- Configurable through env, and `RATE_LIMIT_ENABLED=false` for burst tests. If the limiter query itself errors, it **fails open** and logs a warning.

## Fail-safety
- **Atomic transactions:** each write is all-or-nothing, and the CHECK and UNIQUE constraints back up the app logic.
- **Timeouts:** Postgres `statement_timeout=3s` and `lock_timeout=2s` (a request stuck behind a hot-row lock fails fast instead of piling up); a pool checkout timeout. All of these map to `503 RETRYABLE` with a rollback, so retrying with the same Idempotency-Key is safe.
- **Bounded internal retry:** deadlock or serialization errors (`40P01`/`40001`) are retried up to 3× with jitter, inside the server.
- **Fail closed:** if the DB is unavailable, the redeem is rejected (503), never optimistically granted.
- Global exception handler: there are no stack-trace leaks, every response carries a `request_id`, and logs are structured JSON.
- Startup: wait-for-DB with backoff; schema is applied idempotently. Shutdown is graceful: the pool drains on lifespan exit.
- A docker-compose health check on db.

---

## Project layout
```
Makefile                    # see below
docker-compose.yml          # postgres:16 with healthcheck
requirements.txt            # fastapi, uvicorn, psycopg[binary,pool], pydantic-settings, pytest, pytest-asyncio, httpx
app/main.py                 # app factory, lifespan (pool, schema), middleware
app/config.py               # pydantic-settings: DB URL, timeouts, rate limits
app/db.py                   # pool, transaction helper, retry-on-deadlock decorator
app/cache.py                # in-process TTL metadata cache + invalidation
app/ratelimit.py            # Postgres upsert limiter + FastAPI dependency
app/errors.py               # ErrorCode enum, ApiError, handlers (uniform shape)
app/models.py               # pydantic request/response models, code normalisation
app/schema.sql
app/services/coupons.py     # upsert / get
app/services/redemption.py  # redeem + idempotency
app/services/orders.py      # cancel
app/routes.py               # endpoints + /healthz /readyz
tests/conftest.py           # real PG via compose, truncate per test, live uvicorn for bursts
tests/test_coupons.py  tests/test_redeem.py  tests/test_idempotency.py  tests/test_cancel.py
tests/test_concurrency.py  tests/test_expiry.py  tests/test_ratelimit.py  tests/test_cache.py  tests/test_failsafe.py
README.md                   # run, curl walkthrough, design decisions, trade-offs
```

### Makefile targets
`make venv` (create .venv + install) · `make db-up` / `make db-down` / `make db-reset` (compose, wait for healthy) · `make run` (uvicorn --reload) · `make test` (db-up + pytest) · `make test-concurrency` · `make psql` · `make seed` (curl sample coupons) · `make demo` (curl walkthrough) · `make clean`.

## Assumptions (I'll state these to the interviewer)
- "Once per customer, **ever**" is read strictly: after a STANDARD redemption is cancelled, the global slot comes back but the same customer can't reuse the code. It's a one-line change (`AND status='ACTIVE'` on the index) if they want otherwise.
- One coupon per order.
- Coupon `type` is immutable after creation, and `max_redemptions` can't drop below `redeemed_count`.
- Cancelling an order whose coupon has since expired still frees the slot.
- Idempotency keys are kept indefinitely; a production system would add a TTL purge job.
- The hot-row lock caps throughput per coupon at roughly one transaction at a time. The README will mention the scaling path (sharded counters / slot pre-allocation), but it won't be built.

## Verification
1. `make venv && make db-up && make run`.
2. `make test` (pytest against real Postgres):
   - **Flash-sale burst:** max=10, 200 concurrent redeems from distinct customers → exactly 10 successes, 190 `NO_REDEMPTIONS_LEFT`, and GET shows `10/0`.
   - **STANDARD same customer**, 50 concurrent → 1 success. **STACKABLE** → successes up to max.
   - **Idempotency:** 50 concurrent requests with the same key → 1 redemption row, identical bodies, 49 marked replayed. Same key with a different body → 422.
   - **Cancel:** 20 concurrent cancels → count drops by exactly 1. Afterwards the freed slot can be taken by a new customer.
   - **Expiry:** a coupon expiring about 300ms out, hammered across the boundary → every success has `redeemed_at < expires_at`, and everything after the boundary is `COUPON_EXPIRED`.
   - **Upsert:** the same seed twice → 201 then 200, with no duplicate row. Shrinking max below the count → 409. Changing type → 409. Lowercase codes resolve to the same coupon.
   - **Rate limit:** exceeding the per-customer limit → 429 with `Retry-After`, and a later retry with the same key succeeds.
   - **Cache:** GET counts stay exact during a burst. The cache is invalidated after an upsert.
   - **Fail-safe:** a redeem blocked past `lock_timeout` (a test holds the coupon row lock) → 503, nothing persisted, and a retry with the same key succeeds.
   - Invariant `redeemed_count == count(ACTIVE redemptions)` checked after every concurrency test.
3. Manual curl walkthrough documented in the README.
