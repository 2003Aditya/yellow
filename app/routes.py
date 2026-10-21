from fastapi import APIRouter, Header, Path, Request, Response
from fastapi.responses import JSONResponse

from app.errors import ApiError, ErrorCode
from app.models import CouponIn, RedeemIn, normalize_code
from app.ratelimit import limit_by_ip, limit_redeem
from app.services import coupons, orders, redemption

router = APIRouter()


@router.post("/coupons", status_code=201, dependencies=[limit_by_ip("coupon_create", "rate_limit_coupon_create_per_ip")])
async def create_coupon(body: CouponIn, request: Request, response: Response):
    created, view = await coupons.create_coupon(request.app.state.pool, request.app.state.cache, body)
    response.status_code = 201 if created else 200
    return {"success": True, "created": created, "coupon": view}


@router.get("/coupons/{code}", dependencies=[limit_by_ip("coupon_read", "rate_limit_coupon_read_per_ip")])
async def get_coupon(request: Request, code: str = Path(min_length=1, max_length=128)):
    return await coupons.get_coupon(request.app.state.pool, request.app.state.cache, normalize_code(code))


@router.post("/redeem", dependencies=[limit_redeem])
async def redeem(
    body: RedeemIn,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=255),
):
    if not idempotency_key or not idempotency_key.strip():
        raise ApiError(ErrorCode.IDEMPOTENCY_KEY_REQUIRED, "Idempotency-Key header is required")
    state = request.app.state
    status, payload, replayed = await redemption.redeem(state.pool, state.bulkhead, idempotency_key.strip(), body)
    headers = {}
    if replayed:
        payload = {**payload, "replayed": True}
        headers["Idempotent-Replayed"] = "true"
    return JSONResponse(payload, status_code=status, headers=headers)


@router.post("/orders/{order_id}/cancel", dependencies=[limit_by_ip("cancel", "rate_limit_cancel_per_ip")])
async def cancel_order(request: Request, order_id: str = Path(min_length=1, max_length=128)):
    return await orders.cancel_order(request.app.state.pool, order_id.strip())


@router.get("/healthz", dependencies=[limit_by_ip("health", "rate_limit_health_per_ip")])
async def healthz():
    return {"status": "ok"}


@router.get("/readyz", dependencies=[limit_by_ip("health", "rate_limit_health_per_ip")])
async def readyz(request: Request):
    try:
        async with request.app.state.pool.connection(timeout=2) as conn:
            await conn.execute("SELECT 1")
    except Exception:
        return JSONResponse({"status": "unavailable", "db": "down"}, status_code=503)
    return {"status": "ok", "db": "up"}
