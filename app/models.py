from datetime import datetime
from enum import Enum
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime, BaseModel, Field, StringConstraints

Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
CouponCode = Annotated[Identifier, AfterValidator(str.upper)]


def normalize_code(code: str) -> str:
    return code.strip().upper()


class CouponType(str, Enum):
    STANDARD = "STANDARD"
    STACKABLE = "STACKABLE"


class CouponIn(BaseModel):
    code: CouponCode
    max_redemptions: int = Field(gt=0)
    discount_percent: float = Field(gt=0, le=100)
    expires_at: AwareDatetime
    type: CouponType


class RedeemIn(BaseModel):
    code: CouponCode
    customer_id: Identifier
    order_id: Identifier


class CouponOut(BaseModel):
    code: str
    type: CouponType
    max_redemptions: int
    redeemed_count: int
    remaining: int
    discount_percent: float
    expires_at: datetime
    expired: bool
