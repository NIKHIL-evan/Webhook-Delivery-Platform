# Webhook Delivery Platform

A self-hostable webhook delivery platform built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker-based processing.

The platform accepts events, persists them durably, delivers them asynchronously, retries failures with exponential backoff, and provides operational visibility through logs, metrics, tracing, and health endpoints.

The architecture follows an **at-least-once delivery model**, similar to production webhook systems such as Stripe, GitHub, and Shopify.

---

# Current Status

## Phase 5 — Performance Analysis In Progress

Completed:

* FastAPI REST API
* PostgreSQL persistence
* Redis Streams
* Redis Consumer Groups
* Async worker-based delivery
* Retry scheduler
* Exponential backoff retries
* Dead-letter handling
* Delivery attempt tracking
* Idempotency keys
* API key authentication
* Tenant isolation
* HMAC webhook signing
* Per-tenant rate limiting
* Structured logging
* Trace IDs
* Health endpoints
* Metrics endpoints
* Operational diagnostics
* Public deployment on Render

---

# Architecture

```text
Client
   ↓
API Key Authentication
   ↓
Tenant Resolution
   ↓
Rate Limiting
   ↓
FastAPI API Server
   ↓
PostgreSQL (Source of Truth)
   ↓
Redis Streams
   ↓
Consumer Group Workers
   ↓
HMAC Signed Webhook Delivery
   ↓
Destination Endpoint
```

---

# Event Lifecycle

```text
Client creates event
      ↓
Event persisted in PostgreSQL
      ↓
Event published to Redis Stream
      ↓
Worker consumes message
      ↓
Webhook delivered
      ↓
Success
      ↓
ACK message
      ↓
Event marked delivered
```

Failure path:

```text
Delivery fails
      ↓
Delivery attempt persisted
      ↓
next_retry_at scheduled
      ↓
Retry scheduler
      ↓
Event re-enqueued
      ↓
Worker retries
      ↓
Maximum attempts reached
      ↓
Event marked dead
```

---

# Features

## Asynchronous Delivery

Webhook delivery occurs entirely in background workers.

API latency does not include webhook delivery time.

This prevents slow customer endpoints from impacting API responsiveness.

---

## Redis Consumer Groups

Consumer groups provide:

* worker coordination
* horizontal scalability
* message ownership
* pending message tracking
* fault recovery

---

## Delivery Attempt Tracking

Every delivery attempt is persisted.

Stored information includes:

* attempt number
* status
* response code
* timestamp

---

## Exponential Backoff Retries

Failed deliveries are retried automatically.

Current retry schedule:

```text
Attempt 1 → 2 seconds
Attempt 2 → 4 seconds
Attempt 3 → 8 seconds
Attempt 4 → 16 seconds
Attempt 5 → Dead
```

Jitter is added to reduce retry synchronization.

---

## Dead Letter Handling

Events exceeding maximum retry attempts are marked:

```text
dead
```

Dead events remain queryable for debugging and inspection.

---

## Idempotency

Clients may provide:

```json
{
  "idempotency_key": "client-generated-key"
}
```

Duplicate submissions return the original event rather than creating a new one.

Correctness is enforced through PostgreSQL uniqueness constraints.

---

# Security

## API Key Authentication

Each tenant receives API keys.

Keys are:

* securely generated
* hashed before storage
* validated on every request

---

## Tenant Isolation

Resources belong to a tenant.

A tenant cannot:

* access another tenant's endpoints
* create events for another tenant
* bypass ownership checks

---

## HMAC Webhook Signing

Outbound webhook deliveries are signed using:

```text
HMAC-SHA256
```

Every delivery contains:

```text
X-Webhook-Signature
```

Receivers can verify:

* authenticity
* integrity
* source trustworthiness

---

## Rate Limiting

Per-tenant rate limiting is enforced using Redis.

Current implementation:

```text
Fixed Window
60-second window
Tenant-scoped counters
```

Requests exceeding limits receive:

```http
429 Too Many Requests
```

---

# Observability

## Structured Logging

Workers emit structured JSON logs.

Example:

```json
{
  "worker":"worker-1",
  "trace_id":"...",
  "event_id":"...",
  "event":"delivery_successful"
}
```

---

## Distributed Tracing

Every event receives a trace identifier.

Trace IDs propagate through:

```text
Event Creation
      ↓
Redis Stream
      ↓
Worker Processing
      ↓
Webhook Delivery
      ↓
Retry Scheduling
```

This enables reconstruction of the complete event lifecycle.

---

## Health Endpoints

### GET /health

Liveness probe.

Used by:

* load balancers
* Render
* container platforms

Response:

```json
{
  "status":"healthy"
}
```

---

### GET /health/details

Operational diagnostics endpoint.

Reports:

* PostgreSQL status
* Redis status
* Worker status
* Retry scheduler status

Possible states:

```text
healthy
degraded
unhealthy
```

Health policy:

```text
PostgreSQL down → Unhealthy
Redis down      → Unhealthy
Workers down    → Degraded
Retry loop down → Degraded
```

---

## Metrics

### GET /metrics/summary

Current metrics:

* events_created
* events_delivered
* delivery_attempts
* events_failed
* events_dead
* pending_messages
* avg_api_latency_ms
* avg_queue_delay_ms
* max_queue_delay_ms

Operational metrics are stored in Redis and exposed through:

GET /metrics/summary

Metric Definitions

API Latency
    Time spent processing a request inside FastAPI.

Queue Delay
    Time between enqueueing an event into Redis Streams and a worker beginning processing.

Max Queue Delay
    Worst observed queue wait time.

These metrics allow independent measurement of:

- API performance
- Worker throughput
- Queue buildup
- System saturation

---

# API Endpoints

## POST /tenants

Create tenant.

---

## POST /tenants/{tenant_id}/api-keys

Generate API key.

---

## POST /endpoints

Register webhook endpoint.

Authentication required.

---

## POST /events

Create event and schedule delivery.

Authentication required.

---

## GET /events/{event_id}/delivery_attempts

Retrieve delivery history.

---

## GET /health

Liveness probe.

Used by:

- load balancers
- Render health checks
- container orchestration systems

Response:

```json
{
  "status": "healthy"
}
---

GET /health/details

Detailed readiness endpoint.

Reports:

PostgreSQL connectivity
Redis connectivity
Worker heartbeat status
Retry scheduler heartbeat status

Possible system states:

healthy
degraded
unhealthy

Returns:

{
  "status": "healthy",
  "postgres": "up",
  "redis": "up",
  "worker_loop": "up",
  "retry_loop": "up",
  "timestamp": "2026-06-05T20:21:26.248543Z"
}

Returns HTTP 503 when critical dependencies are unavailable.
---

GET /metrics/summary

Lightweight operational metrics endpoint.

Returns:

{
  "events_created": 1001,
  "events_delivered": 1001,
  "delivery_attempts": 1001,
  "events_failed": 0,
  "events_dead": 0,
  "pending_messages": 0,
  "timestamp": "2026-06-05T20:21:26.248543Z"
}
---

# Delivery Semantics

Current guarantee:

```text
At-Least-Once Delivery
```

Implications:

* events are not silently lost
* duplicate deliveries are possible

Example:

```text
Webhook sent
      ↓
Receiver gets request
      ↓
Worker crashes before ACK
      ↓
Message reprocessed
      ↓
Duplicate delivery
```

This is an intentional distributed-systems trade-off.

---

# Performance

## Phase 3 Baseline

Environment:

* Local PostgreSQL
* Local Redis
* Authentication enabled
* Tenant isolation enabled
* Rate limiting enabled

Load:

* 1000 requests
* 10 concurrent clients

| Metric       | Result    |
| ------------ | --------- |
| p50          | 83.93 ms  |
| p95          | 141.06 ms |
| p99          | 203.94 ms |
| Requests/sec | 109.05    |

---

## Phase 4 Baseline

Environment:

* Local PostgreSQL
* Local Redis Streams
* Async workers
* Retry scheduler
* API authentication
* HMAC signing
* Structured logging
* Tracing
* Metrics
* Health checks

Load:

* 1000 requests
* 10 concurrent clients

| Metric       | Result    |
| ------------ | --------- |
| p50          | 83.97 ms  |
| p95          | 130.35 ms |
| p99          | 187.96 ms |
| Requests/sec | 110.88    |

Operational Metrics:

```text
Events Created     : 1001
Events Delivered   : 1001
Delivery Attempts  : 1001
Failed Events      : 0
Dead Events        : 0
Pending Messages   : 0
```

Observations:

* 100% successful event delivery
* Worker throughput matched event creation rate
* No queue backlog accumulated
* No retries required
* Observability overhead had negligible impact on latency

---

# Failure Model

PostgreSQL is the durable source of truth.

Redis Streams provide:

* asynchronous processing
* worker coordination
* pending-entry tracking
* acknowledgment semantics

If worker processes fail:

* events remain persisted
* retry scheduler can recover failed deliveries

If Redis stream state is lost:

* events remain in PostgreSQL
* recovery remains possible through persisted state

---

# Known Limitations

Current limitations:

* duplicate deliveries remain possible
* dead-letter handling is database-state based
* retry scheduler uses polling
* worker and API execute in the same process
* Redis stream retention policy not implemented
* metrics stored in Redis rather than Prometheus
* tracing is application-level rather than OpenTelemetry-based
* replay tooling not implemented
* load testing performed only on local infrastructure

---

# Deployment

Deployed on Render using:

* FastAPI Web Service
* Managed PostgreSQL
* Managed Redis

Background processes:

```python
asyncio.create_task(worker_loop())
asyncio.create_task(retry_loop())
```

---

Phase 5 Scaling Analysis

Environment:

Local PostgreSQL
Local Redis
Consumer Groups
HMAC Signing
Structured Logging
Tracing
Metrics
Health Checks

Load:

10,000 requests
10 concurrent clients
Worker Scaling Results
Workers	Avg API Latency	p95 Latency	Requests/sec	Avg Queue Delay	Max Queue Delay
2	32.64 ms	127.83 ms	124.39	25.96 s	39.66 s
4	44.31 ms	139.88 ms	106.25	9.08 s	16.92 s
6	47.12 ms	112.02 ms	101.77	10.47 ms	84 ms
8	53.42 ms	123.97 ms	94.90	11.57 ms	86 ms
Findings
API Was Not The Bottleneck

API latency remained below:

55 ms

under sustained 10,000-event load.

Worker Throughput Was The Initial Bottleneck

With 2 workers:

Average Queue Delay: 25.96 seconds

Events accumulated faster than workers could process them.

Additional Workers Reduced Queue Buildup
2 workers → 25.96s queue delay
4 workers → 9.08s queue delay
6 workers → 10ms queue delay

At 6 workers the consumer throughput approximately matched event creation throughput.

Diminishing Returns After 6 Workers

Increasing workers:

6 → 8

produced no meaningful queue-delay improvement while increasing API latency and reducing request throughput.

Resource Contention Appeared

After queue buildup disappeared, additional workers began competing for:

PostgreSQL connections
Redis operations
CPU scheduling
Event-loop execution time
Conclusion

For the current machine and workload:

6 workers ≈ optimal operating point

Beyond that point, additional workers increase system overhead without improving delivery performance.