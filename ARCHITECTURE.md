# Architecture — Coupon Redemption Service

## 1. Goals and constraints

| Requirement | Design response |
|---|---|
| Never exceed `max_redemptions`, even in a burst | Pessimistic row lock on the coupon + DB `CHECK` constraint |
| STANDARD: once per customer, ever | Partial `UNIQUE` index in Postgres |
| STACKABLE: no per-customer limit, global cap still applies | Excluded from that index; same lock + cap path |
| Consistent outcome at the exact expiry instant | Single clock: Postgres transaction timestamp `now()` |
| Cancel gives the slot back once per order | Conditional state transition `ACTIVE → CANCELLED` |
| Retries must not double-redeem | `Idempotency-Key` stored in the **same transaction** as the redemption |
| `GET` count is exact, not eventually correct | Counter changes atomically with the redemption rows; never cached |
| Production concerns | Upserts, rate limiting, caching, fail-safe error handling, duplicate guards |

**Guiding principle:** *Postgres is the single source of truth and the only thing that decides correctness.* The app layer is stateless. The cache and rate limiter only speed things up or protect capacity; neither ever decides whether a redemption is valid.

---

## 2. System overview

```
                ┌──────────────────────────────────────────────────────────┐
  Client ──────▶│  FastAPI app  (stateless; N workers / instances)         │
 (checkout)     │                                                          │
                │  Middleware: X-Request-ID · JSON access log              │
                │      │                                                   │
                │  routes.py  ── validation (pydantic) · rate limit check  │
                │      │                                                   │
                │  services/                                               │
                │   ├─ coupons.py     upsert / get   ◀──▶ MetadataCache    │
                │   ├─ redemption.py  redeem + idempotency  (in-process)   │
                │   └─ orders.py      cancel                               │
                │      │                                                   │
                │  db.py: AsyncConnectionPool · deadlock retry · timeouts  │
                │  ratelimit.py: upsert counter                            │
                └──────┼───────────────────────────────────────────────────┘
                       │  psycopg 3 (async), raw SQL
                       ▼
                ┌──────────────────────────────────────────────────────────┐
                │  PostgreSQL 16                                           │
                │   coupons · redemptions · idempotency_keys · rate_limits │
                │   row locks · UNIQUE / CHECK constraints · now() clock   │
                └──────────────────────────────────────────────────────────┘
```

There is no Redis or message queue: one datastore and one consistency domain.

---

## 3. Code layout and responsibilities

```
app/
  main.py            App factory, lifespan (pool open, schema bootstrap, graceful close),
                     request-id middleware, structured JSON logging, exception → HTTP mapping
  routes.py          HTTP layer only: parse, validate, rate-limit, call a service, shape the response
  models.py          Pydantic models; code normalisation (trim + UPPER); tz-aware datetimes
  errors.py          ErrorCode enum → HTTP status; uniform error body; ApiError
  config.py          pydantic-settings (env-driven): DB, pool, timeouts, limits, cache
  db.py              Pool creation (session timeouts), runs migrations on boot, @retry_on_conflict
  migrate.py         Hand-rolled migration runner (checksummed, advisory-locked, transactional)
  cache.py           Size-bounded TTL metadata cache, version-checked
  ratelimit.py       Fixed-window limiter built on one atomic upsert
  services/
    coupons.py       Upsert coupon, read coupon status
    redemption.py    Redeem transaction (idempotency + business rules)
    orders.py        Cancel transaction
db/migrations/       Hand-written SQL migrations (0001_init.sql), tracked in schema_migrations
tests/               Real-Postgres tests: flows, bursts, races, expiry boundary, timeouts,
                     and test_multi_instance.py (two separate app processes, one DB)
scripts/demo.py      make seed / make demo
Makefile, docker-compose.yml, db/init.sql (creates the coupons_test DB)
```

**Layering rule:** routes → services → SQL. Services own transactions, and each public service function is exactly one transaction. Routes never touch SQL.

---

## 4. Data model

```
┌──────────────────────────────┐        ┌─────────────────────────────────────┐
│ coupons                      │ 1    * │ redemptions                         │
├──────────────────────────────┤────────├─────────────────────────────────────┤
│ code PK (normalised)         │        │ id PK                               │
│ type STANDARD|STACKABLE      │        │ coupon_code FK → coupons.code       │
│ max_redemptions  > 0         │        │ coupon_type (denormalised)          │
│ redeemed_count               │        │ customer_id                         │
│ discount_percent (0,100]     │        │ order_id  UNIQUE                    │
│ expires_at timestamptz       │        │ status ACTIVE|CANCELLED             │
│ version (bumped on upsert)   │        │ redeemed_at (= txn now())           │
│ CHECK 0 ≤ count ≤ max        │        │ cancelled_at                        │
└──────────────────────────────┘        └─────────────────────────────────────┘
   UNIQUE (coupon_code, customer_id) WHERE coupon_type = 'STANDARD'

┌──────────────────────────────┐        ┌─────────────────────────────────────┐
│ idempotency_keys             │        │ rate_limits                         │
├──────────────────────────────┤        ├─────────────────────────────────────┤
│ key PK                       │        │ bucket PK  ('customer:c1','ip:…')   │
│ request_hash (sha256 body)   │        │ window_start                        │
│ response_status, body jsonb  │        │ hits                                │
└──────────────────────────────┘        └─────────────────────────────────────┘
```

Why these choices:
- **`redeemed_count` is a denormalised counter**, so `GET` is a single PK read. Correctness comes from changing it in the same transaction as `redemptions`. Tests check the invariant `redeemed_count == count(ACTIVE redemptions)`.
- **`coupon_type` is copied into `redemptions`** because a partial index can't reference another table.
- **Cancelled rows still count toward the STANDARD index** ("once per customer, *ever*"). To allow reuse after cancel, add `AND status='ACTIVE'` to the predicate.
- **`version`** lets the cache validate entries against the DB, which keeps it safe across instances.

---

## 5. Request flows

### 5.1 `POST /redeem`: one transaction

```
route: header present? ── no ──▶ 400 IDEMPOTENCY_KEY_REQUIRED
       rate limit (customer, ip) ── over ──▶ 429 (not stored under the key)
       │
BEGIN  (READ COMMITTED; now() fixed here = decision time)
 1. INSERT idempotency_keys(key, hash, placeholder) ON CONFLICT DO NOTHING
      ├─ conflict, same hash  ──▶ return stored response  (replayed: true)
      ├─ conflict, diff hash  ──▶ 422 IDEMPOTENCY_KEY_REUSED
      └─ (if same key in flight: this INSERT WAITS for that txn to finish)
 2. SELECT coupon … FOR UPDATE           ── none ──▶ UNKNOWN_COUPON (404)
 3. now() >= expires_at                  ── yes  ──▶ COUPON_EXPIRED (410)
 4. redeemed_count >= max                ── yes  ──▶ NO_REDEMPTIONS_LEFT (409)
 5. SAVEPOINT; INSERT redemption
      ├─ uq_standard_once_per_customer   ──▶ ALREADY_REDEEMED_BY_CUSTOMER (409)
      └─ redemptions_order_id_key        ──▶ ORDER_ALREADY_HAS_COUPON (409)
    UPDATE coupons SET redeemed_count + 1
 6. UPDATE idempotency_keys SET response = <outcome>   (success OR business error)
COMMIT
```

Business errors are committed together with the key, so a retry gets the same answer. Infrastructure errors roll back everything, including the key, so a retry runs fresh.

### 5.2 `POST /orders/{id}/cancel`: one transaction

```
BEGIN
 1. SELECT coupon_code FROM redemptions WHERE order_id      ── none ──▶ 200 NO_COUPON_ON_ORDER
 2. SELECT coupon … FOR UPDATE            (same lock order as redeem)
 3. UPDATE redemptions SET CANCELLED WHERE order_id AND status='ACTIVE'
      ├─ 0 rows ──▶ 200 released:false ALREADY_CANCELLED
      └─ 1 row  ──▶ UPDATE coupons SET redeemed_count - 1 ──▶ 200 released:true
COMMIT
```

### 5.3 `POST /coupons`: upsert

```
INSERT … ON CONFLICT (code) DO UPDATE SET …, version = version + 1
  WHERE coupons.type = EXCLUDED.type AND EXCLUDED.max >= coupons.redeemed_count
RETURNING (xmax = 0) AS inserted         → 201 created / 200 updated
no row returned → 409 COUPON_TYPE_IMMUTABLE | MAX_BELOW_REDEEMED
after commit → cache.invalidate(code)
```

### 5.4 `GET /coupons/{code}`

```
SELECT version, redeemed_count, max, remaining, expired  (always live from DB)
cache.get(code, version) ── hit ──▶ static fields from cache
                         └─ miss ─▶ SELECT static fields WHERE version = v → cache.put
```

---

## 6. Concurrency model

| Race | Mechanism | Why it's safe |
|---|---|---|
| 200 checkouts on 10 slots | `FOR UPDATE` on the coupon row | Check-then-increment runs serially per coupon; the `CHECK` constraint is the backstop |
| Same customer, STANDARD, in parallel | Partial unique index | The second insert blocks on the index entry, then fails once the first commits |
| Same `Idempotency-Key` in parallel | PK + `ON CONFLICT DO NOTHING` | The second insert waits for the first txn, then reads the committed final response |
| Two cancels of one order | Lock + `WHERE status='ACTIVE'` | Under READ COMMITTED the blocked UPDATE re-checks the committed row and matches nothing |
| Redeem vs. cancel | Both lock coupon row → redemption row | A single global lock order means no deadlock cycle |
| Redemption at the expiry instant | `now()` = txn start, DB clock | One clock, fixed before any lock wait; boundary is exclusive (`now() < expires_at` is valid) |
| `GET` during a burst | Counter in the same txn | Readers see only committed state, so `count + remaining = max` always holds |

**Isolation level:** READ COMMITTED with explicit row locks. SERIALIZABLE isn't needed because every conflict goes through a locked row or a unique index. Explicit locks also give predictable blocking instead of abort-and-retry storms.

**Expiry semantics in detail:** a request's validity is decided by its transaction start time. If request A starts at `T-1ms` and waits 5 ms for the lock, it still succeeds. A request B starting at `T` is rejected. Both outcomes are deterministic and auditable, because the same timestamp is stored as `redeemed_at`.

---

## 7. Cross-cutting concerns

### Duplicate handling
| Duplicate | Guard |
|---|---|
| Coupon seeded twice | Upsert + code normalisation |
| Redeem retried / concurrent with the same key | `idempotency_keys` + replay |
| Key reused with a different body | SHA-256 request-hash comparison → 422 |
| STANDARD reuse by a customer | Partial unique index |
| Second coupon on an order | `UNIQUE(order_id)` |
| Double cancel | Conditional status transition |

### Rate limiting (`ratelimit.py`)
- A fixed window kept in `rate_limits`, maintained by **one atomic upsert** that resets or increments and returns `hits` plus `retry_after`. It works across instances without Redis.
- Buckets: `customer:{id}` (default 10/10s) and `ip:{addr}` (100/10s) on `/redeem`; IP-only on other endpoints.
- It runs in its own short transaction **before** the redeem transaction, and a 429 is never written under the idempotency key.
- It **fails open**: the limiter protects capacity, not correctness.

### Caching (`cache.py`)
- In-process, bounded LRU with a TTL. It holds static metadata only (type, discount, expires_at), tagged with the row's `version`.
- Every read validates the version against the DB, so an entry made stale by an update on another instance is a miss, never wrong data.
- **Counters are never cached.** `/redeem` never reads the cache because it has to lock the row anyway.

### Fail-safety
| Failure | Behaviour |
|---|---|
| Hot-row lock wait > `lock_timeout` (2s) | Txn aborted → `503 RETRYABLE` + `Retry-After`; nothing persisted |
| Slow query > `statement_timeout` (3s) | Same |
| Pool exhausted (> 5s wait) | Same |
| Deadlock / serialization failure | Retried in-server up to 3× with jitter, then 503 |
| DB down | **Fail closed**: redemption refused, never optimistically granted; `/readyz` → 503 |
| Limiter error | Fail open, warning logged |
| Unexpected exception | `500 INTERNAL`, no stack trace leaked, logged with the request id |
| Startup before DB ready | Pool waits up to 30s; pending migrations applied in one txn under an advisory lock |
| Shutdown | Lifespan closes the pool and waits for checked-out connections |

The idempotency key and the redemption commit or roll back together, so any 503 is safe to retry with the same key.

### Errors
There is one shape for all errors: `{"success": false, "error": {"code", "message"}}`. `errors.py` maps each `ErrorCode` to its HTTP status. Business outcomes are *returned* from services, so they can be committed alongside the idempotency key. Request-level failures are *raised* as `ApiError`.

### Observability
Every response carries `X-Request-ID` (taken from the caller or generated). Access logs are JSON with method, path, status, duration and request id. `/healthz` reports liveness and `/readyz` reports DB reachability.

---

## 8. Configuration
All settings come from environment variables via `config.py` (also `.env`): `DATABASE_URL`, `DB_POOL_MIN/MAX`, `DB_POOL_TIMEOUT_S`, `STATEMENT_TIMEOUT_MS`, `LOCK_TIMEOUT_MS`, `RATE_LIMIT_ENABLED`, `RATE_LIMIT_WINDOW_S`, `RATE_LIMIT_PER_CUSTOMER`, `RATE_LIMIT_PER_IP`, `CACHE_TTL_S`, `CACHE_MAX_ENTRIES`.

## 9. Testing strategy
- Every test runs against a **real Postgres** (`coupons_test` DB, truncated per test). No mocks, because the guarantees live in the database.
- Concurrency tests use `asyncio.gather` with a 30-connection pool, so the database sees real parallel transactions. Tests are marked `@pytest.mark.concurrency` (`make test-concurrency`).
- Every race test checks the invariant `redeemed_count == count(ACTIVE)`.
- Fail-safe test: a second connection holds the coupon lock, so redeem returns 503 with nothing persisted, and a retry with the same key then succeeds.
- **Multi-instance proof** (`make test-multi`): two `uvicorn` processes share one Postgres, with requests alternated between them: a 300-on-25 burst, same-key retries on both instances, and a double cancel across instances. Swapping the row lock for an in-process lock makes this test fail, which confirms it detects in-memory correctness.

## 10. Trade-offs and evolution
| Decision | Trade-off | Next step if needed |
|---|---|---|
| Row lock per coupon | Roughly one redemption txn at a time per coupon (a few ms each) | Slot pre-allocation with `FOR UPDATE SKIP LOCKED`, or sharded counters |
| Postgres rate limiter | An extra small write per request | Move it to Redis or an API gateway at high volume |
| In-process cache | Per-instance; small benefit | LISTEN/NOTIFY invalidation or a shared cache |
| Keys and limiter rows kept forever | Tables grow | TTL purge job (e.g. delete keys older than 24h) |
| Hand-rolled migration runner | No down-migrations | Forward-only fixes (`0002_*.sql`), or adopt a tool if the team prefers |
| Strict "once ever" for STANDARD | A customer can't reuse after cancel | Change the index predicate (one line) |
