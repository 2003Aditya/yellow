from enum import Enum


class ErrorCode(str, Enum):
    # redeem business outcomes
    UNKNOWN_COUPON = "UNKNOWN_COUPON"
    COUPON_EXPIRED = "COUPON_EXPIRED"
    NO_REDEMPTIONS_LEFT = "NO_REDEMPTIONS_LEFT"
    ALREADY_REDEEMED_BY_CUSTOMER = "ALREADY_REDEEMED_BY_CUSTOMER"
    ORDER_ALREADY_HAS_COUPON = "ORDER_ALREADY_HAS_COUPON"
    # idempotency
    IDEMPOTENCY_KEY_REQUIRED = "IDEMPOTENCY_KEY_REQUIRED"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    # coupon creation
    COUPON_ALREADY_EXISTS = "COUPON_ALREADY_EXISTS"
    # generic
    VALIDATION_ERROR = "VALIDATION_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    RETRYABLE = "RETRYABLE"
    INTERNAL = "INTERNAL"


STATUS = {
    ErrorCode.UNKNOWN_COUPON: 404,
    ErrorCode.COUPON_EXPIRED: 410,
    ErrorCode.NO_REDEMPTIONS_LEFT: 409,
    ErrorCode.ALREADY_REDEEMED_BY_CUSTOMER: 409,
    ErrorCode.ORDER_ALREADY_HAS_COUPON: 409,
    ErrorCode.IDEMPOTENCY_KEY_REQUIRED: 400,
    ErrorCode.IDEMPOTENCY_KEY_REUSED: 422,
    ErrorCode.COUPON_ALREADY_EXISTS: 409,
    ErrorCode.VALIDATION_ERROR: 422,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.RETRYABLE: 503,
    ErrorCode.INTERNAL: 500,
}


def error_body(code: ErrorCode, message: str) -> dict:
    return {"success": False, "error": {"code": code.value, "message": message}}


class ApiError(Exception):
    def __init__(self, code: ErrorCode, message: str, headers: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = STATUS[code]
        self.headers = headers or {}
