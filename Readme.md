# Redis Durability & Dual-Layer Idempotency Lab

An empirical systems engineering lab exploring Redis persistence modes, Linux kernel memory behavior, and fault-tolerant payment idempotency using FastAPI, Redis 7+, and PostgreSQL.

Rather than treating Redis as an infallible cache, this project intentionally injects `SIGKILL` crashes, stale TCP socket pools, and heavy background write loads to observe real failure modes and validate a zero-double-spend payment architecture.

---

## Architecture Overview

Payment endpoints use a **dual-layer idempotency strategy** combining low-latency Redis locks with an ACID-compliant PostgreSQL ledger:


```

Client Request (Idempotency-Key)
                │
                ▼
┌───────────────────────────────┐
│        FastAPI Gateway        │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐      Cache Hit ("COMMITTED")
│  Layer 1: Redis Atomic Lock   ├──────────────────────────────────► Return 201 (Cached)
│  SET key "PROCESSING" NX EX   │
└───────────────┬───────────────┘
                │ Lock Acquired OR Redis Unreachable (Graceful Degradation)
                ▼
┌───────────────────────────────┐Unique Constraint Violation
│ Layer 2: PostgreSQL Ledger    ├──────────────────────────► Rollback & Return 201 (DB Fallback)
│ INSERT INTO payment_intents   │
└───────────────┬───────────────┘
                │ Commit Success
                ▼
                Update Redis -> "COMMITTED"
                │
                ▼
                Return 201 (Success)

```

### Core Guarantees
- **Fast-Path Deduplication:** Atomic `SET ... NX` blocks concurrent duplicate requests (`409 Conflict`) and serves completed retries directly from memory.
- **Graceful Degradation:** If Redis suffers an OOM kill, network partition, or data loss window, the application bypasses the cache layer without failing the request.
- **Zero Double-Spends:** PostgreSQL enforces a `UNIQUE` constraint on `idempotency_key` as the final source of truth.

---

## Engineering Findings & Empirical Benchmarks

### 1. Persistence Modes Under `SIGKILL`

| Persistence Mode | Configuration | Behavior After `SIGKILL` & Restart | Retry Resolution Path |
| :--- | :--- | :--- | :--- |
| **RDB-Only** | `appendonly no`, `save 60 1000` | Memory wiped (snapshot threshold not met) | PostgreSQL `UNIQUE` constraint fallback |
| **AOF (`everysec`)** | `appendonly yes`, `appendfsync everysec` | Key recovered from disk (`COMMITTED`) | Redis cached response (0 DB queries) |
| **AOF (`always`)** | `appendonly yes`, `appendfsync always` | Key recovered from disk (`COMMITTED`) | Redis cached response (0 DB queries) |

### 2. Connection Pool Recovery & Graceful Fallback Bugs
Testing hard container kills (`docker kill --signal="SIGKILL" redis_lab`) exposed two real-world failure modes in async Python clients:
1. **Stale TCP Sockets on Restart:** After Redis restarted and recovered its AOF state, `redis-py` reused dead TCP sockets from before the `SIGKILL`, raising `ConnectionError` on the first post-crash request and falsely triggering database fallback.
   - **Fix:** Configured `Retry(ExponentialBackoff(), 2)`, `retry_on_error=[ConnectionError, TimeoutError]`, and `health_check_interval=5` on the async connection pool.
2. **Swallowed Fallback Responses:** When Redis was genuinely unreachable (`redis_available = False`), nested `return` statements inside `except IntegrityError` caused the endpoint to implicitly return `None` (`null`).
   - **Fix:** Decoupled cache state updates from HTTP response returns so PostgreSQL fallbacks always serialize cleanly.

### 3. Linux Copy-On-Write (COW) & RSS Amplification
When Redis forks a child process (`BGSAVE` or `BGREWRITEAOF`), the Linux kernel shares 4 KB physical memory pages until the parent mutates a key, forcing page duplication.

- **Test Setup:** Seeded ~82 MB dataset (`100,000` keys, 1 KB payload), triggered `BGREWRITEAOF`, and blasted `300,000` pipelined mutations (`-P 16` at ~372,670 req/s).
- **Results:**
  - Baseline `used_memory_human`: `81.92 MB` (`used_memory_rss_human`: `93.43 MB`)
  - `aof_last_cow_size`: `23,990,272 bytes` (~22.88 MB / ~28% COW overhead during a <1s rewrite)
  - Peak `used_memory_rss_human`: `138.34 MB`
- **Production Takeaway:** Because the Linux OOM Killer evaluates physical RSS rather than Redis internal memory, production instances with persistence enabled should cap `maxmemory` at **50%–65%** of container RAM limits.

### 4. Hybrid Persistence (`aof-use-rdb-preamble yes`)
Inspecting `/data/appendonlydir` after `BGREWRITEAOF` confirmed Redis 7+ Multi-Part AOF behavior:
- `appendonly.aof.6.base.rdb`: Compact binary RDB snapshot for fast container boot times.
- `appendonly.aof.6.incr.aof`: Incremental RESP command log for point-in-time recovery.
- `appendonly.aof.manifest`: Sequence tracking metadata linking base and incremental segments.

### 5. `fsync` Latency & Throughput Trade-off
Benchmarked via `redis-benchmark -t set -n 20000 -c 50 -q` on NVMe storage:

| `appendfsync` Policy | Throughput (SET req/s) | Median Latency (`p50`) | Relative Throughput |
| :--- | :--- | :--- | :--- |
| **`everysec`** | `173,913.05 req/s` | `0.151 ms` | `1.00x` (Baseline) |
| **`always`** | `35,650.62 req/s` | `1.247 ms` | `0.20x` (~80% drop) |

**Architectural Conclusion:** Running `appendfsync always` sacrifices ~80% of Redis throughput and increases median latency by >8x. Combining `appendfsync everysec` with a PostgreSQL `UNIQUE` constraint delivers near-memory performance while guaranteeing zero financial double-spends during sub-second crash windows.

---

## Project Structure

```text
redis-durability-lab/
├── app/
│   ├── main.py          # FastAPI payment intent endpoint & dual-layer idempotency logic
│   ├── database.py      # Async SQLAlchemy & PostgreSQL session setup
│   ├── models.py        # PaymentIntent ledger model with UNIQUE idempotency_key
│   └── redis_client.py  # Async Redis client with exponential backoff & socket health checks
├── config/
│   └── redis.conf       # Configurable persistence parameters (RDB / AOF / Hybrid)
├── docker-compose.yml   # Redis 7 & PostgreSQL services with named persistent volumes
└── requirements.txt

```

---

## Running the Lab Locally

### 1. Start Infrastructure & API

```bash
docker compose up -d
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload

```

### 2. Test Crash Survival (`SIGKILL`)

```bash
# 1. Execute initial payment
curl -X POST "http://localhost:8000/api/v1/payment-intents" \
     -H "Content-Type: application/json" \
     -H "Idempotency-Key: txn_test_01" \
     -d '{"amount": 4000.00, "currency": "INR", "customer_id": "cust_01"}'

# 2. Simulate abrupt power loss / OOM kill
docker kill --signal="SIGKILL" redis_lab && docker compose start redis

# 3. Retry identical request
curl -X POST "http://localhost:8000/api/v1/payment-intents" \
     -H "Content-Type: application/json" \
     -H "Idempotency-Key: txn_test_01" \
     -d '{"amount": 4000.00, "currency": "INR", "customer_id": "cust_01"}'

```

### 3. Reproduce Copy-On-Write & `fsync` Benchmarks

```bash
# Seed 100k keys and trigger background AOF rewrite under pipelined write load
docker exec -it redis_lab redis-benchmark -t set -n 100000 -r 100000 -d 1024 -q
docker exec -it redis_lab sh -c "redis-cli BGREWRITEAOF && redis-benchmark -t set -n 300000 -r 100000 -d 1024 -P 16 -q"
docker exec -it redis_lab redis-cli info persistence | grep -E "aof_last_cow_size|rdb_last_cow_size"

# Compare appendfsync policies
docker exec -it redis_lab redis-cli CONFIG SET appendfsync everysec
docker exec -it redis_lab redis-benchmark -t set -n 20000 -c 50 -q

docker exec -it redis_lab redis-cli CONFIG SET appendfsync always
docker exec -it redis_lab redis-benchmark -t set -n 20000 -c 50 -q

```