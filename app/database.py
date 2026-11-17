# async input/output redis
import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError

REDIS_URL = "redis://localhost:6379"

# global redis client
redis_client: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global redis_client

    if redis_client is None:
        redis_client = aioredis.from_url(
            url=REDIS_URL,
            decode_responses=True,
            retry=Retry(ExponentialBackoff(cap=1, base=0.1), 2),
            retry_on_error=[ConnectionError, TimeoutError],
            health_check_interval=5,
        )

    return redis_client


async def close_redis():
    global redis_client

    if redis_client:
        await redis_client.close()
        redis_client = None
