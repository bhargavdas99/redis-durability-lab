from pydantic import BaseModel, Field


class PaymentIntent(BaseModel):
    amount: float = Field(..., gt=0, description="payment amount")
    currency: str = Field("INR", description="currency code")
    customer_id: str
