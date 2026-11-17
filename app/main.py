from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import close_redis, get_redis
from app.models import PaymentIntentModel, get_db, init_db
from app.schemas import PaymentIntent
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import IntegrityError


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify Redis & initialize Postgres tables
    client = await get_redis()
    await client.ping()
    await init_db()
    yield
    # Shutdown: close redis connection
    await close_redis()


app = FastAPI(title="Redis Durability & Crash Tolerance Lab", lifespan=lifespan)


@app.post("/api/v1/payment-intents", status_code=status.HTTP_201_CREATED)
async def create_payment_intent(
    payload: PaymentIntent,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
):
    redis_client = await get_redis()
    lock_key = f"idempotency:{idempotency_key}"
    redis_available = True

    try:
        acquired = await redis_client.set(lock_key, "PROCESSING", nx=True, ex=300)

        if not acquired:
            current_state = await redis_client.get(lock_key)
            if current_state == "PROCESSING":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Concurrent request detected. Transaction is currently processing.",
                )
            return {
                "status": "DUPLICATE_RETRY",
                "message": "Transaction already executed successfully (cached response).",
                "idempotency_key": idempotency_key,
            }
    except RedisConnectionError:
        redis_available = False

    try:
        db_intent = PaymentIntentModel(
            idempotency_key=idempotency_key,
            customer_id=payload.customer_id,
            amount=payload.amount,
            currency=payload.currency,
            status="COMMITTED",
        )
        db.add(db_intent)
        await db.commit()

        if redis_available:
            try:
                await redis_client.set(lock_key, "COMMITTED", ex=300)
            except RedisConnectionError:
                pass

        return {
            "status": "SUCCESS",
            "data": {
                "customer_id": payload.customer_id,
                "amount": payload.amount,
                "currency": payload.currency,
                "state": "COMMITTED",
            },
            "idempotency_key": idempotency_key,
        }

    except IntegrityError:
        await db.rollback()
        if redis_available:
            try:
                await redis_client.set(lock_key, "COMMITTED", ex=300)
            except RedisConnectionError:
                pass

        # Un-indented: Always return the fallback response even when Redis is down
        return {
            "status": "DUPLICATE_RETRY",
            "message": "Transaction already executed successfully (PostgreSQL unique constraint fallback).",
            "idempotency_key": idempotency_key,
        }

    except Exception as e:
        await db.rollback()
        if redis_available:
            try:
                await redis_client.set(lock_key, "FAILED", ex=60)
            except RedisConnectionError:
                pass
        # Un-indented: Always raise the 500 error even when Redis is down
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
