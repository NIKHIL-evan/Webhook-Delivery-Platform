# Webhook Delivery Platform

A multi-tenant webhook delivery service, in the style of the infrastructure behind Stripe or GitHub webhooks: clients submit events through an API, and the platform delivers each one as a signed HTTP request to the customer's endpoint, **at least once**, with idempotency, rate limiting, and full delivery history.

Built with **FastAPI · Redis Streams · PostgreSQL · asyncio**, and scaled one measured bottleneck at a time.

---

## Highlights

| | Before | Current | Change | Round |
|---|---|---|---|---|
| **API CPU per request** (fixed 3,000 req/s) | 1.27 ms | **0.43 ms** | **−66% (3.0× cheaper)** | 2 |
| **Max ingestion throughput** (1,000 concurrent clients) | 5,082 req/s | **13,508 req/s** | **2.66×** | 2 |
| p95 latency at 3,000 req/s | 4.57 ms | **1.31 ms** | −71% | 2 |
| API cores needed at 20,000 events/s (at 70% busy) | ~36 | **~12** | −67% | 2 |
| **Redis CPU per event** | 120 µs | **~50 µs** | **−58%** | 1 |
| Redis round trips per event (whole pipeline) | 14 | **~3** | −79% | 1 |
| Delivery throughput (6 workers, local endpoint) | 2,416/s | **2,874/s** | +19% | 1 |

*Every row is a same-session A/B on a 16-thread laptop: the code before a round vs after it, back to back, same load test. Round 1 used the original load test (git tag `baseline` vs end of Round 1). Round 2 used an improved load test (one tenant created in `setup()`, plus a fixed-rate open model), so the two rounds' throughput numbers are **not** chained into one figure. See [Performance](#performance) for the method.*

**Correctness fixes along the way:**
- a race that could silently lose an accepted event
- a worker that died permanently on a Redis hiccup
- revoked API keys that kept working for 5 minutes
- event streams that grew in memory forever
- a **40 ms stall** on responses over busy connections (Nagle + delayed ACK)
- a per-endpoint lock table that grew forever
- `/events` 404s that skipped rate limiting and always hit the database
- a fixed-window rate limiter that allowed **2× the limit** at window edges

---

## What it does

1. A tenant registers an **endpoint** (a URL) and gets an **API key**.
2. The tenant sends `POST /events` with a JSON payload and an optional **idempotency key**.
3. The API answers `202 Accepted` in **under a millisecond** (p50 0.65 ms at 3,000 req/s), without touching the database.
4. A **delivery worker** signs the payload with **HMAC-SHA256** and POSTs it to the endpoint.
5. Every event and every delivery attempt is persisted to PostgreSQL in batches, for history and auditing.

---

## Architecture

```mermaid
flowchart LR
    C["Client"] -->|"POST /events"| API["FastAPI API (12 uvicorn processes, httptools + uvloop)"]
    API -->|"1 Lua call: token bucket + idempotency + XADD"| S[("Redis stream: webhook_events")]
    S -->|"XREADGROUP"| W["Delivery workers"]
    W -->|"signed HTTP POST"| D["Customer endpoint"]
    W -->|"MULTI/EXEC: result XADD + XACK"| R[("Redis stream: webhook_results")]
    S -->|"batches of 500"| ES["Event sink"]
    R -->|"batches of 500"| RS["Result sink"]
    ES --> PG[("PostgreSQL")]
    RS --> PG
    H["Housekeeper: sweeper + stream trimmer"] -.-> S
```

**Key properties**

- **No database on the write path.** The API authenticates from a two-level cache, validates the endpoint from an in-process cache, and enqueues to Redis. PostgreSQL is written asynchronously by sink workers in batches of 500.
- **Fat messages.** Each stream entry carries everything needed to deliver it (URL, payload, signing context), so delivery workers never query the database.
- **Consumer groups.** Delivery and persistence are separate groups on the same stream, so they scale and fail independently.
- **At-least-once delivery.** An event is ACKed only after its result is recorded. Every failure mode is designed to produce, at worst, a duplicate, never a loss.
- **Stateless API.** Rate limits, idempotency, and the queue live in Redis; per-process caches hold only short-lived, read-mostly data. More API processes or machines can sit behind a load balancer without coordination.

### Life of one event

| Step | Where | Redis trips |
|---|---|---|
| Hash the API key, look up the tenant | L1 in-process cache (30 s), L2 Redis (5 min), then Postgres | ~0 (L1 hit rate 99.5% under load) |
| Validate the endpoint | in-process cache (single-flight loads, 404s cached for 60 s) | 0 |
| Rate limit (token bucket) → idempotency check → enqueue → remember key | **one atomic Lua script** | 1 |
| Read event, sign, POST to customer | delivery worker | 1 |
| Record result + ACK the event | **one `MULTI`/`EXEC` transaction** | 1 |
| Persist event and result | sink workers, batched | ~0.01 |

---

## Engineering decisions

Each decision was made against measured numbers, and each has a known condition under which I would change it.

### Round 1: Redis cost per event

**1. Fewer Redis round trips, not just fewer commands.**
`redis-benchmark` showed ~4.1 µs per command sent one at a time, but ~0.4 µs when 16 are pipelined: about **90% of a round trip's cost is the "envelope"** (network read, parse, reply), not the command itself. Redis executes commands on a single core, so I optimized for trips: **14 → ~3 per event**.

**2. Metrics aggregated in memory, flushed once per second.**
Metrics were 5 of the 14 trips and moved no event forward. Each process now counts locally and flushes in one pipeline per second (a swap with no `await` in between, so no counts are lost). Trade-off: metrics lag ≤ 1 s and a crash loses ≤ 1 s of counts, which is acceptable for metrics and never acceptable for events.

**3. An atomic Lua ingest script, ordered so failures cause duplicates, not losses.**
Previously, idempotency (`SET NX`) and enqueue (`XADD`) were two trips: if the second failed, every client retry for 24 hours was told "already queued," and the event was **silently lost**. Lua scripts run in isolation but **don't roll back**, so command order decides the failure mode. The script reads the idempotency key, enqueues, and only then stores the key: a failure halfway can produce a duplicate, never a loss.

**4. `MULTI`/`EXEC` where no decision is needed, Lua only where one is.**
`commandstats` showed Lua adds interpreter overhead per call on top of the commands inside it. So the worker's "record result + ACK" uses a plain transaction (both run or neither does) instead of another script.

**5. A two-level tenant cache with bounded revocation.**
A per-process L1 (30 s) in front of a shared Redis L2 (5 min), invalidated on revocation. It removes ~25% of Redis trips at the target scale, and a revoked key stops working within **≤ 30 s** (previously up to 5 min). I chose bounded over instant revocation deliberately, the same trade-off short-lived JWTs make; pub/sub invalidation is the documented upgrade path.

**6. Stream trimming that can't lose events.**
ACKs don't delete stream entries, so memory grew forever (~860 GB/day at 20k events/s). A naive `MAXLEN` cap would delete unread events whenever the backlog exceeds it. The trimmer computes, for each consumer group, the oldest entry it hasn't finished, and trims only below the **slowest** group (`XTRIM MINID`). Verified: with delivery workers stopped, nothing was trimmed, and all 5,000 events were delivered once they restarted.

### Round 2: API CPU per request

**7. Profile first, then remove the biggest cost.**
`py-spy` across all 12 workers showed Starlette's `BaseHTTPMiddleware` costing ~0.40 ms of a 1.70 ms request: it runs the app in a separate task, pipes the response through an in-memory stream, and spawns tasks to watch for disconnects, all to add one `X-Trace-ID` header. Rewritten as a **pure ASGI middleware** that wraps `send` in a closure: **−0.38 ms**, matching both the flame graph and a removal test. The single biggest win of the round (48% of the savings).

**8. Server configuration as code: no access log, C parser, C event loop.**
Each change measured alone, at the same fixed rate:

| Change | Saved per request | Why |
|---|---|---|
| `--no-access-log` | 0.12–0.21 ms | a formatted log line per request, mostly Python, not kernel time |
| `--http httptools` | 0.13–0.18 ms | h11 (pure Python) costs 73 µs per request vs 2.7 µs for httptools (C) in an isolated benchmark: **26.7×** |
| `--loop uvloop` | ~0.04 ms (−6%) | the event loop was already a small slice of each request |

Flags are set **explicitly**: uvicorn's `auto` silently switches implementation depending on what is installed, which once made my "baseline" already optimized.

**9. Found and fixed a 40 ms stall in the TCP layer.**
Every response over a busy connection took a flat ~41 ms extra. uvicorn writes headers and body separately; with **Nagle's algorithm** on, the kernel held the body until the headers were ACKed, and the client delayed that ACK by ~40 ms. asyncio only sets `TCP_NODELAY` when `sock.proto == IPPROTO_TCP`, but uvicorn creates its socket with `proto = 0`, so the check silently fails. uvloop sets `TCP_NODELAY` unconditionally. Result: **max throughput 7,585 → 10,127 req/s with no CPU change.**

**10. Endpoint cache with single-flight and negative caching.**
`/events` opened a database session on every request. Endpoints are now cached in process (5 min). Concurrent misses for the same endpoint share **one** `asyncio.Task` (single-flight: 100 concurrent misses → 1 query), unknown IDs are cached separately for 60 s so they can't evict real entries, and misses are rate-limited before querying. **−26% CPU per request** (0.590 → 0.437 ms, same session). There is no `await` between "check in-flight" and "register in-flight", so there is no race inside a process.

**11. Token bucket instead of a fixed window.**
A fixed window allows 2× the limit across a window edge and then locks a bursty tenant out for up to 59 s. The token bucket (capacity = limit, refilled continuously over 60 s) lives in one shared Lua function used by every rate-limited path, refills lazily from two stored fields, uses **Redis's own clock** so every process agrees on "now", and returns `Retry-After` on `429`. Cost: **+0.5 µs** of Redis time per event.

**12. Measure with an open model, not only a closed one.**
A closed load test (1,000 clients, each waiting for its reply) reports p95 = 257 ms; a fixed 3,000 req/s open test on the same code reports **4.9 ms**. The closed number is mostly a queue the test's own clients build. I use closed tests for **max throughput** and fixed-rate open tests for **cost and latency**, and compare versions only in the same session.

**13. Rejected: switching servers or languages (for now).**
- **Granian** (Rust ASGI server): +12–14% CPU per request in this app and dozens of hung requests at 1,000 connections. The headline benchmarks are hello-world apps against untuned uvicorn.
- **Go rewrite:** likely 5–10× cheaper per request, but the API now needs ~12 cores at 20k events/s, while delivery concurrency and single-core Redis are the real limits. Planned as a measured rewrite of only the hot path, later.

### Architecture choices

**Redis Streams over Kafka or RabbitMQ (for now).**
At ~3 trips per event, one Redis core handles roughly 20,000 ingested events/s, and I already run Redis for caching and rate limiting. Kafka would add durable replay and horizontal partitioning at a large operational cost. I'd revisit if I needed multi-day replay, or if one Redis became the bottleneck after optimization.

**SHA-256 for API keys, not bcrypt.**
API keys are 256 random bits, so brute force is already infeasible; bcrypt only helps low-entropy human passwords. SHA-256 is deterministic, so the hash can be indexed and looked up directly, and costs ~1 µs instead of ~100 ms (bcrypt at 20k req/s would need over 1,000 cores and be a DoS vector).

---

## Performance

**Method**
- **Same-session A/B:** the code before and after a change, measured back to back, on the same machine, plugged in, with explicit server flags.
- **Cost test (open model):** API only, k6 `constant-arrival-rate` at a fixed **3,000 req/s for 60 s**, a rate every version handles comfortably.
- **Throughput test (closed model):** API only (12 uvicorn processes), k6 ramping to 1,000 virtual users over 1m50s.
- **API CPU per request:** user + system CPU of all uvicorn processes from `/proc/<pid>/stat`, before and after, divided by requests served.
- **Delivery test:** 6 delivery workers draining the ingestion backlog into a local mock endpoint.
- **Redis cost:** `CONFIG RESETSTAT`, then main-thread CPU from `INFO cpu` before and after, divided by events; per-command costs from `INFO commandstats`.
- **Correctness check after every change:** 1,000 events in → 1,000 created, attempted, delivered, and received by the mock, with nothing left pending.

**Round 2 ladder: each change applied in order, same session, 3,000 req/s, 0 failed requests**

| Step | CPU per request | p50 / p95 |
|---|---|---|
| Start (end of Round 1) | 1.268 ms | 1.57 / 4.57 ms |
| + pure ASGI middleware | 0.864 ms | 1.04 / 2.00 ms |
| + no access log | 0.747 ms | 0.92 / 1.84 ms |
| + httptools | 0.621 ms | 0.78 / 1.58 ms |
| + uvloop | 0.575 ms | 0.74 / 1.52 ms |
| + endpoint cache | 0.436 ms | 0.65 / 1.26 ms |
| + token bucket | **0.428 ms** | **0.65 / 1.31 ms** |

Max throughput (closed test, same evening): **5,082 → 13,508 req/s**; p95 at 1,000 clients 259 → 128.5 ms.

**Per-command Redis cost**

| Command | µs per call |
|---|---|
| `GET` | ~1.0 |
| `SET` | ~2.3 |
| `XADD` | ~4.4 |
| Ingest Lua script (token bucket + idempotency + `XADD` + `SET`) | ~20.5 |

**Honest scope**
- Laptop numbers are used as **before/after ratios**, not absolute capacity claims. The load generator (k6) runs on the same laptop, which is why costs are compared at a fixed, comfortable rate rather than at saturation.
- ~50 µs of Redis per event covers the **API side**. Delivery workers add their own Redis work, so the full pipeline's ceiling is lower; measuring it end to end is Round 3.
- The delivery test uses a local endpoint (~2 ms). Real endpoints take ~100–500 ms, which makes delivery concurrency, not Redis or the API, the next bottleneck.

The full log of every change, measurement, and mistake is in [`docs/scaling-journal.md`](docs/scaling-journal.md).

---

## Guarantees and failure modes

| If this fails... | What happens |
|---|---|
| Redis fails during ingestion | the client gets a `5xx` and retries; a `202` is only sent after the event is in the stream |
| The ingest script fails halfway | at worst, a duplicate on retry, never a lost event |
| A tenant exceeds its rate limit | `429` with `Retry-After`; the bucket refills gradually instead of locking the tenant out |
| Many requests miss the endpoint cache at once | one database query; the rest wait for its result |
| A worker crashes before recording a result | the event stays pending and is reclaimed and redelivered |
| A worker crashes after the HTTP POST, before recording | redelivery (inherent to webhooks: receivers should deduplicate by event ID) |
| Redis hiccups in a worker loop | the error is logged, the worker continues |
| A consumer group falls behind | the trimmer waits for it; memory grows, nothing is lost |

---

## Known limitations and roadmap

I keep this list on purpose. The project is being scaled in rounds, each driven by a measured bottleneck.

| Round | Focus | Status |
|---|---|---|
| 1 | Redis cost per event | ✅ done |
| 2 | API CPU per request: profiling, pure-ASGI middleware, server tuning, the 40 ms stall, endpoint cache, token bucket, open-model load tests | ✅ done |
| 3 | Delivery concurrency: each worker currently sends one request at a time, so slow endpoints cap throughput (6 in flight; ~4,000 needed at 20k events/s with 200 ms endpoints) | **next** |
| 4 | Failure handling: failed deliveries aren't yet scheduled for retry; backoff with jitter, circuit breakers, dead-letter queue, a correct sweeper | planned |
| 5 | PostgreSQL at scale: partitioning, retention, connection pooling (PgBouncer) | planned |
| 6 | Observability (histograms, SLOs, tracing) and a cloud benchmark of `baseline` vs final | planned |
| 7 | Deployment, plus authentication on the admin routes | planned |

**Other known gaps**
- The HMAC signature is computed over canonical JSON rather than the exact bytes sent, and there's no timestamp in the signature for replay protection yet (Round 3).
- The endpoint cache isn't cleared when an endpoint is updated or deleted, so events can go to the old URL for up to 5 minutes.
- An endpoint ID used *before* it was created stays `404` for up to 60 s (negative cache).
- No per-request access log; each request's trace ID is still stored with the event.

---

## Running locally

**Prerequisites:** Docker, [uv](https://docs.astral.sh/uv/), and [k6](https://k6.io/) for load tests.

```bash
# 1. Start PostgreSQL and Redis
docker compose up -d

# 2. Configure (.env in the project root)
cat > .env << 'EOF'
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/webhooks
REDIS_URL=redis://localhost:6379/0
EOF

# 3. Install dependencies and create the schema
uv sync
uv run alembic upgrade head

# 4. Start a local test endpoint (a mock customer server) in its own terminal
uv run uvicorn loadtest.mock_destination:app --port 9000

# 5. Start the platform: API, delivery workers, sinks, housekeeper
./start_local.sh
```

The API runs as:

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 8 \
  --no-access-log --http httptools --loop uvloop
```

**Send your first webhook:**

```bash
# Create a tenant (returns its id and signing secret)
curl -s -X POST localhost:8000/tenants -H "Content-Type: application/json" \
  -d '{"name":"demo","rate_limit":1000}'

# Create an API key for it
curl -s -X POST localhost:8000/tenants/<TENANT_ID>/api-keys -H "Content-Type: application/json" \
  -d '{"name":"demo-key"}'

# Register an endpoint
curl -s -X POST localhost:8000/endpoints -H "Content-Type: application/json" \
  -H "API-Key: <API_KEY>" -d '{"url":"http://localhost:9000/webhook"}'

# Send an event
curl -s -X POST localhost:8000/events -H "Content-Type: application/json" \
  -H "API-Key: <API_KEY>" \
  -d '{"endpoint_id":"<ENDPOINT_ID>","idempotency_key":"order-42","payload":{"order_id":42}}'

# Check delivery
curl -s localhost:9000/stats
```

**Load tests** (the script creates its own tenant, key, and endpoint in `setup()`):

```bash
k6 run loadtest/loadtest_k6.js                                  # closed: ramp to 1,000 virtual users (max throughput)
k6 run -e MODE=open -e RATE=3000 loadtest/loadtest_k6.js        # open: fixed 3,000 req/s for 60 s (cost and latency)
k6 run --iterations 1000 --vus 50 loadtest/loadtest_k6.js       # correctness run
uv run python loadtest/parse_bench.py                           # h11 vs httptools parsing cost
curl -s "localhost:8000/metrics/summary?reset=false"
```

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tenants` | create a tenant (returns its signing secret) |
| `POST` | `/tenants/{id}/api-keys` | create an API key (shown once; stored as a SHA-256 hash) |
| `DELETE` | `/api-keys/{id}` | revoke a key (effective within ≤ 30 s) |
| `POST` | `/endpoints` | register a destination URL |
| `POST` | `/events` | submit an event (`202`; optional `idempotency_key`; `429` with `Retry-After` when over the limit) |
| `GET` | `/events/{id}/delivery_attempts` | delivery history for one event |
| `GET` | `/health/details` | Postgres, Redis, worker, and housekeeper status |
| `GET` | `/metrics/summary` | throughput, latency, and pipeline backlog |

Every response carries an `X-Trace-ID` header.

**Verifying a webhook (receiver side):** each request carries `X-Webhook-Signature: sha256=<hex>`, an HMAC-SHA256 of the payload (JSON with sorted keys and compact separators) using the tenant's signing secret. Compare with a constant-time function such as `hmac.compare_digest`.

---

## Project structure

```
app/
├── main.py                  # FastAPI app, pure ASGI trace middleware, startup
├── core/
│   ├── dependencies.py      # API-key auth, two-level tenant cache, rate limit + Retry-After
│   ├── redis_scripts.py     # token bucket + atomic ingest Lua script
│   ├── local_metrics.py     # in-process metrics with 1 s flush
│   └── ...
├── routers/                 # tenants, api-keys, endpoints, events (endpoint cache), observability
└── workers/
    ├── delivery_worker.py   # sign + deliver, result + ACK transaction
    ├── event_sink_worker.py # batched event persistence
    ├── result_sink_worker.py# batched attempt persistence
    └── retry_runner.py      # housekeeper: retry scheduler, sweeper, stream trimmer
loadtest/                    # k6 script (closed + open models), mock destination, parser benchmark
alembic/                     # database migrations
docs/scaling-journal.md      # every change, measurement, and decision
```

## Tech stack

Python 3.12 · FastAPI · uvicorn (httptools, uvloop) · asyncio · Redis 7 (Streams, Lua, transactions) · PostgreSQL 16 · SQLAlchemy 2 (async) · Alembic · httpx · structlog · k6 · py-spy · Docker Compose