# Webhook Delivery Platform

A multi-tenant webhook delivery service, in the style of the infrastructure behind Stripe or GitHub webhooks: clients submit events through an API, and the platform delivers each one as a signed HTTP request to the customer's endpoint, **at least once**, with idempotency, rate limiting, and full delivery history.

Built with **FastAPI · Redis Streams · PostgreSQL · asyncio**, and scaled one measured bottleneck at a time.

---

## Highlights

| | Original design | Current | Change |
|---|---|---|---|
| Ingestion throughput | 2,790 req/s | **4,868 req/s** | **+74%** |
| p95 latency (1,000 concurrent clients) | 434 ms | **267 ms** | −38% |
| Redis CPU per event | 120 µs | **50 µs** | **−58%** |
| Redis round trips per event (whole pipeline) | 14 | **~3** | −79% |
| Delivery throughput (6 workers, local endpoint) | 2,416/s | **2,874/s** | +19% |

*Same-session A/B on a 16-thread laptop: the original code (git tag `baseline`) vs the current code, back to back, same load test, Redis CPU measured exactly with `INFO cpu` and `INFO commandstats`. See [Performance](#performance) for the method.*

**Correctness fixes along the way:** a race that could silently lose an accepted event · a worker that died permanently on a Redis hiccup · revoked API keys that kept working for 5 minutes · event streams that grew in memory forever.

---

## What it does

1. A tenant registers an **endpoint** (a URL) and gets an **API key**.
2. The tenant sends `POST /events` with a JSON payload and an optional **idempotency key**.
3. The API answers `202 Accepted` in about a millisecond, without touching the database.
4. A **delivery worker** signs the payload with **HMAC-SHA256** and POSTs it to the endpoint.
5. Every event and every delivery attempt is persisted to PostgreSQL in batches, for history and auditing.

---

## Architecture

```mermaid
flowchart LR
    C["Client"] -->|"POST /events"| API["FastAPI API (N processes)"]
    API -->|"1 Lua call: rate limit + idempotency + XADD"| S[("Redis stream: webhook_events")]
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

- **No database on the write path.** The API authenticates from a two-level cache and enqueues to Redis. PostgreSQL is written asynchronously by sink workers in batches of 500.
- **Fat messages.** Each stream entry carries everything needed to deliver it (URL, payload, signing context), so delivery workers never query the database.
- **Consumer groups.** Delivery and persistence are separate groups on the same stream, so they scale and fail independently.
- **At-least-once delivery.** An event is ACKed only after its result is recorded. Every failure mode is designed to produce, at worst, a duplicate, never a loss.

### Life of one event

| Step | Where | Redis trips |
|---|---|---|
| Hash the API key, look up the tenant | L1 in-process cache (30 s), L2 Redis (5 min), then Postgres | ~0 (L1 hit rate 99.5% under load) |
| Validate the endpoint | in-process cache | 0 |
| Rate limit → idempotency check → enqueue → remember key | **one atomic Lua script** | 1 |
| Read event, sign, POST to customer | delivery worker | 1 |
| Record result + ACK the event | **one `MULTI`/`EXEC` transaction** | 1 |
| Persist event and result | sink workers, batched | ~0.01 |

---

## Engineering decisions

Each decision was made against measured numbers, and each has a known condition under which I would change it.

**1. Fewer Redis round trips, not just fewer commands.**
`redis-benchmark` showed ~4.1 µs per command sent one at a time, but ~0.4 µs when 16 are pipelined: about **90% of a round trip's cost is the "envelope"** (network read, parse, reply), not the command itself. Redis executes commands on a single core, so I optimized for trips: **14 → ~3 per event**.

**2. Metrics aggregated in memory, flushed once per second.**
Metrics were 5 of the 14 trips and moved no event forward. Each process now counts locally and flushes in one pipeline per second (a swap with no `await` in between, so no counts are lost). Trade-off: metrics lag ≤ 1 s and a crash loses ≤ 1 s of counts, which is acceptable for metrics and never acceptable for events.

**3. An atomic Lua ingest script, ordered so failures cause duplicates, not losses.**
Previously, idempotency (`SET NX`) and enqueue (`XADD`) were two trips: if the second failed, every client retry for 24 hours was told "already queued," and the event was **silently lost**. Lua scripts run in isolation but **don't roll back**, so command order decides the failure mode. The script reads the idempotency key, enqueues, and only then stores the key: a failure halfway can produce a duplicate, never a loss.

**4. `MULTI`/`EXEC` where no decision is needed, Lua only where one is.**
`commandstats` showed my Lua script costs ~23 µs of interpreter overhead per call, more than the four commands inside it. So the worker's "record result + ACK" uses a plain transaction (both run or neither does) instead of another script.

**5. A two-level tenant cache with bounded revocation.**
A per-process L1 (30 s) in front of a shared Redis L2 (5 min), invalidated on revocation. It removes ~25% of Redis trips at the target scale, and a revoked key stops working within **≤ 30 s** (previously up to 5 min). I chose bounded over instant revocation deliberately, the same trade-off short-lived JWTs make; pub/sub invalidation is the documented upgrade path.

**6. Stream trimming that can't lose events.**
ACKs don't delete stream entries, so memory grew forever (~860 GB/day at 20k events/s). A naive `MAXLEN` cap would delete unread events whenever the backlog exceeds it. The trimmer computes, for each consumer group, the oldest entry it hasn't finished, and trims only below the **slowest** group (`XTRIM MINID`). Verified: with delivery workers stopped, nothing was trimmed, and all 5,000 events were delivered once they restarted.

**7. Redis Streams over Kafka or RabbitMQ (for now).**
At ~3 trips per event, one Redis core handles roughly 20,000 ingested events/s, and I already run Redis for caching and rate limiting. Kafka would add durable replay and horizontal partitioning at a large operational cost. I'd revisit if I needed multi-day replay, or if one Redis became the bottleneck after optimization.

---

## Performance

**Method**
- **Same-session A/B:** the original code (`git checkout baseline`) and the current code, measured back to back, on the same machine, plugged in.
- **Ingestion test:** API only (12 uvicorn processes), k6 ramping to 1,000 virtual users over 1m50s.
- **Delivery test:** 6 delivery workers draining the ingestion backlog into a local mock endpoint.
- **Redis cost:** `CONFIG RESETSTAT`, then main-thread CPU from `INFO cpu` before and after, divided by events; per-command costs from `INFO commandstats`.
- **Correctness check after every change:** 1,000 events in → 1,000 created, attempted, delivered, and received by the mock, with nothing left pending.

**Per-command cost (current)**

| Command | µs per call |
|---|---|
| `GET` | ~1.0 |
| `INCR` | ~2.5 |
| `SET` | ~2.3 |
| `XADD` | ~4.4 |
| Ingest Lua script (includes the 4 commands above) | ~29 |

**Honest scope**
- Laptop numbers are used as **before/after ratios**, not absolute capacity claims.
- 50 µs per event covers the **API side**. Delivery workers add their own Redis work, so the full pipeline's ceiling is lower; measuring it end to end is on the roadmap.
- The delivery test uses a local endpoint (~2 ms). Real endpoints take ~100–500 ms, which makes delivery concurrency, not Redis, the next bottleneck (see the roadmap).

The full log of every change, measurement, and mistake is in [`docs/scaling-journal.md`](docs/scaling-journal.md).

---

## Guarantees and failure modes

| If this fails... | What happens |
|---|---|
| Redis fails during ingestion | the client gets a `5xx` and retries; a `202` is only sent after the event is in the stream |
| The ingest script fails halfway | at worst, a duplicate on retry, never a lost event |
| A worker crashes before recording a result | the event stays pending and is reclaimed and redelivered |
| A worker crashes after the HTTP POST, before recording | redelivery (inherent to webhooks: receivers should deduplicate by event ID) |
| Redis hiccups in a worker loop | the error is logged, the worker continues |
| A consumer group falls behind | the trimmer waits for it; memory grows, nothing is lost |

---

## Known limitations and roadmap

I keep this list on purpose. The project is being scaled in rounds, each driven by a measured bottleneck.

| Round | Focus | Status |
|---|---|---|
| 1 | Redis cost per event | ✅ done (results above) |
| 2 | API CPU per request: profiling, pure-ASGI middleware, token-bucket rate limiting, open-model load tests | next |
| 3 | Delivery concurrency: each worker currently sends one request at a time, so slow endpoints cap throughput | planned |
| 4 | Failure handling: failed deliveries aren't yet scheduled for retry; backoff with jitter, circuit breakers, dead-letter queue, a correct sweeper | planned |
| 5 | PostgreSQL at scale: partitioning and retention | planned |
| 6 | Observability (histograms, SLOs, tracing) and a cloud benchmark of `baseline` vs final | planned |
| 7 | Deployment, plus authentication on the admin routes | planned |

Other known gaps: the HMAC signature is computed over canonical JSON rather than the exact bytes sent, and there's no timestamp in the signature for replay protection yet (both Round 3).

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

**Load test:**

```bash
k6 run loadtest/loadtest_k6.js                              # ramp to 1,000 virtual users
k6 run --iterations 1000 --vus 50 loadtest/loadtest_k6.js   # correctness run
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
| `POST` | `/events` | submit an event (`202`; optional `idempotency_key`) |
| `GET` | `/events/{id}/delivery_attempts` | delivery history for one event |
| `GET` | `/health/details` | Postgres, Redis, worker, and housekeeper status |
| `GET` | `/metrics/summary` | throughput, latency, and pipeline backlog |

**Verifying a webhook (receiver side):** each request carries `X-Webhook-Signature: sha256=<hex>`, an HMAC-SHA256 of the payload (JSON with sorted keys and compact separators) using the tenant's signing secret. Compare with a constant-time function such as `hmac.compare_digest`.

---

## Project structure

```
app/
├── main.py                  # FastAPI app, middleware, startup
├── core/
│   ├── dependencies.py      # API-key auth, two-level tenant cache, rate limit
│   ├── redis_scripts.py     # atomic ingest Lua script
│   ├── local_metrics.py     # in-process metrics with 1 s flush
│   └── ...
├── routers/                 # tenants, api-keys, endpoints, events, observability
└── workers/
    ├── delivery_worker.py   # sign + deliver, result + ACK transaction
    ├── event_sink_worker.py # batched event persistence
    ├── result_sink_worker.py# batched attempt persistence
    └── retry_runner.py      # housekeeper: retry scheduler, sweeper, stream trimmer
loadtest/                    # k6 script + mock destination
alembic/                     # database migrations
docs/scaling-journal.md      # every change, measurement, and decision
```

## Tech stack

Python 3.12 · FastAPI · asyncio · Redis 7 (Streams, Lua, transactions) · PostgreSQL 16 · SQLAlchemy 2 (async) · Alembic · httpx · structlog · k6 · Docker Compose