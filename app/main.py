import contextvars
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from app.bulkhead import Bulkhead
from app.cache import MetadataCache
from app.config import Settings, get_settings
from app.db import create_pool, open_pool
from app.errors import STATUS, ApiError, ErrorCode, error_body
from app.ratelimit import RateLimiter
from app.routes import router

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        for key in ("method", "path", "status", "duration_ms", "attempt"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("coupons")
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    root.propagate = False


log = logging.getLogger("coupons.http")

# Infrastructure failures: the transaction was rolled back, nothing was persisted, retry is safe.
RETRYABLE_ERRORS = (
    PoolTimeout,
    psycopg.OperationalError,  # includes LockNotAvailable / QueryCanceled (timeouts), lost connections
    psycopg.errors.DeadlockDetected,
    psycopg.errors.SerializationFailure,
)


def _error(code: ErrorCode, message: str, headers: dict | None = None) -> JSONResponse:
    body = error_body(code, message)
    body["error"]["request_id"] = request_id_var.get()
    return JSONResponse(body, status_code=STATUS[code], headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    _configure_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = create_pool(settings)
        await open_pool(pool, settings)
        limiter_pool = create_pool(settings, size=settings.rate_limit_pool_size, name="limiter")
        await limiter_pool.open(wait=False)
        app.state.pool = pool
        app.state.cache = MetadataCache(settings.cache_ttl_s, settings.cache_max_entries)
        app.state.limiter = RateLimiter(limiter_pool, settings)
        app.state.bulkhead = Bulkhead(settings.redeem_max_inflight_per_coupon, settings.redeem_queue_timeout_s)
        app.state.settings = settings
        try:
            yield
        finally:
            # graceful: each close waits for checked-out connections to come back
            await limiter_pool.close()
            await pool.close()

    app = FastAPI(title="Coupon Redemption Service", version="1.0.0", lifespan=lifespan)
    app.include_router(router)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        token = request_id_var.set(rid)
        start = time.perf_counter()
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = rid
            response.headers["X-Instance-ID"] = settings.instance_id
            log.info(
                "request",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round((time.perf_counter() - start) * 1000, 2),
                },
            )
            return response
        finally:
            request_id_var.reset(token)

    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError):
        return _error(exc.code, exc.message, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError):
        details = "; ".join(f"{'.'.join(str(p) for p in e['loc'][1:])}: {e['msg']}" for e in exc.errors())
        return _error(ErrorCode.VALIDATION_ERROR, details or "Invalid request")

    async def retryable_handler(_: Request, exc: Exception):
        log.warning("retryable infrastructure error: %s", type(exc).__name__)
        return _error(
            ErrorCode.RETRYABLE,
            "Temporarily unable to process the request; nothing was applied, retry with the same Idempotency-Key",
            {"Retry-After": "1"},
        )

    for exc_type in RETRYABLE_ERRORS:
        app.add_exception_handler(exc_type, retryable_handler)

    @app.exception_handler(Exception)
    async def unhandled_handler(_: Request, exc: Exception):
        log.exception("unhandled error")
        return _error(ErrorCode.INTERNAL, "Internal error")

    return app


app = create_app()
