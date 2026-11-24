import hashlib
import json
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, status
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import close_redis, get_redis
from app.models import PaymentIntentModel, get_db, init_db
from app.schemas import PaymentIntent


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle hooks.

    Verifies Redis connectivity and initializes PostgreSQL tables on startup,
    and cleanly closes the shared Redis connection pool on shutdown.
    """
    client = await get_redis()
    await client.ping()
    await init_db()
    yield
    await close_redis()


app = FastAPI(title="Redis Durability & Crash Tolerance Lab", lifespan=lifespan)


def compute_request_hash(payload: PaymentIntent) -> str:
    """Generate a deterministic SHA-256 fingerprint for a payment request.

    Normalizes customer_id, amount (rounded to 6 decimal places), and uppercase
    currency into canonical JSON so identical payloads produce identical hashes.
    """
    canonical = json.dumps(
        {
            "customer_id": payload.customer_id.strip(),
            "amount": round(float(payload.amount), 6),
            "currency": payload.currency.strip().upper(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def verify_fingerprint(stored_hash: str, incoming_hash: str) -> None:
    """Ensure an incoming retry matches the original request payload hash.

    Raises HTTP 422 Unprocessable Entity if a client reuses an existing
    Idempotency-Key with modified payment fields.
    """
    if stored_hash != incoming_hash:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key reused with a different request payload.",
        )


@app.post("/api/v1/payment-intents", status_code=status.HTTP_201_CREATED)
async def create_payment_intent(
    payload: PaymentIntent,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
):
    """Create a payment intent with dual-layer Redis + PostgreSQL idempotency.

    Guarantees:
    - Atomic request fingerprinting (SHA-256) bound at key acquisition and DB insert.
    - Crash-after-commit recovery when Redis retains a stale PROCESSING lock.
    - Fail-open degradation to PostgreSQL unique constraints when Redis is down.
    - Verbatim replay of the persisted original response on valid retries.
    """
    redis_client = await get_redis()
    lock_key = f"idempotency:{idempotency_key}"
    req_hash = compute_request_hash(payload)
    redis_available = True

    try:
        # Atomically bind both PROCESSING state and request_hash on lock acquisition
        lock_record = json.dumps({"state": "PROCESSING", "request_hash": req_hash})
        # Short 30s lock TTL so a crashed worker doesn't trap retries for 5 mins
        acquired = await redis_client.set(lock_key, lock_record, nx=True, ex=30)

        if not acquired:
            raw_cached = await redis_client.get(lock_key)
            if raw_cached:
                cached = json.loads(raw_cached)
                verify_fingerprint(cached["request_hash"], req_hash)

                if cached["state"] == "PROCESSING":
                    # Check if a crashed worker already committed to Postgres
                    existing = await db.scalar(
                        select(PaymentIntentModel).where(
                            PaymentIntentModel.idempotency_key == idempotency_key
                        )
                    )
                    if not existing:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Concurrent request detected. Transaction is currently processing.",
                        )
                    verify_fingerprint(existing.request_hash, req_hash)
                    await redis_client.set(
                        lock_key,
                        json.dumps(
                            {
                                "state": "COMMITTED",
                                "request_hash": existing.request_hash,
                                "response": existing.response_payload,
                            }
                        ),
                        ex=300,
                    )
                    return existing.response_payload

                # COMMITTED in Redis: replay the exact stored original response
                return cached["response"]
    except RedisConnectionError:
        redis_available = False

    # Build canonical response snapshot once, to be stored and replayed verbatim
    canonical_response = {
        "status": "SUCCESS",
        "data": {
            "customer_id": payload.customer_id,
            "amount": payload.amount,
            "currency": payload.currency,
            "state": "COMMITTED",
        },
        "idempotency_key": idempotency_key,
    }

    try:
        # Atomic single-row INSERT: idempotency_key + request_hash + response_payload
        db_intent = PaymentIntentModel(
            idempotency_key=idempotency_key,
            request_hash=req_hash,
            customer_id=payload.customer_id,
            amount=payload.amount,
            currency=payload.currency,
            status="COMMITTED",
            response_payload=canonical_response,
        )
        db.add(db_intent)
        await db.commit()

        if redis_available:
            try:
                await redis_client.set(
                    lock_key,
                    json.dumps(
                        {
                            "state": "COMMITTED",
                            "request_hash": req_hash,
                            "response": canonical_response,
                        }
                    ),
                    ex=300,
                )
            except RedisConnectionError:
                pass

        return canonical_response

    except IntegrityError:
        await db.rollback()
        existing = await db.scalar(
            select(PaymentIntentModel).where(
                PaymentIntentModel.idempotency_key == idempotency_key
            )
        )
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unique constraint triggered but existing record could not be retrieved.",
            )
        verify_fingerprint(existing.request_hash, req_hash)

        if redis_available:
            try:
                await redis_client.set(
                    lock_key,
                    json.dumps(
                        {
                            "state": "COMMITTED",
                            "request_hash": existing.request_hash,
                            "response": existing.response_payload,
                        }
                    ),
                    ex=300,
                )
            except RedisConnectionError:
                pass

        # Replay stored original response verbatim instead of recomputing
        return existing.response_payload

    except HTTPException:
        raise

    except Exception as e:
        await db.rollback()
        if redis_available:
            try:
                await redis_client.delete(lock_key)
            except RedisConnectionError:
                pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
