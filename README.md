# Webhook Delivery Platform

A self-hostable webhook delivery platform built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker-based processing.

Clients can:

* Register webhook endpoints
* Create events
* Deliver events asynchronously
* Retry failed deliveries with exponential backoff
* Track delivery attempts
* Use idempotency keys
* Authenticate using API keys
* Operate in isolated multi-tenant environments
* Receive HMAC-signed webhook requests

The platform follows an at-least-once delivery model similar to production webhook systems.

---

# Status

Phase 3 — Multi-Tenancy and Security Implemented

Implemented:

* FastAPI REST API
* PostgreSQL persistence
* Async SQLAlchemy integration
* Redis Streams
* Redis consumer groups
* Background worker processing
* Retry scheduler
* Exponential backoff retries
* Dead letter handling
* Delivery attempt tracking
* Idempotency key support
* API key authentication
* Tenant isolation
* HMAC webhook signing
* Per-tenant rate limiting
* Async webhook delivery
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

Retry flow:

```text
delivery fails
    ↓
delivery attempt persisted
    ↓
next_retry_at scheduled
    ↓
retry scheduler
    ↓
event re-enqueued
    ↓
worker retries
    ↓
max attempts reached
    ↓
event marked dead
```

---

# Features

## Async Delivery

Webhook delivery occurs asynchronously using Redis Streams and background workers.

API response latency does not include webhook delivery time.

---

## Consumer Groups

Redis consumer groups provide:

* worker coordination
* horizontal scalability
* pending message tracking
* message ownership

---

## Delivery Tracking

Every delivery attempt is persisted.

Stored information includes:

* attempt number
* response status code
* delivery status
* delivery timestamp

---

## Retry Scheduling

Failed deliveries are retried using exponential backoff.

Current behavior:

```text
attempt 1 → 2 seconds
attempt 2 → 4 seconds
attempt 3 → 8 seconds
attempt 4 → 16 seconds
...
```

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
  "idempotency_key": "client-key"
}
```

Duplicate submissions return the original event.

PostgreSQL unique constraints enforce correctness.

---

# Security Features

## API Keys

Each tenant receives API keys for authentication.

Keys are:

* generated securely
* hashed before storage
* validated on every request

---

## Tenant Isolation

Resources belong to a tenant.

A tenant cannot:

* access another tenant's endpoints
* create events against another tenant's resources
* bypass ownership checks

---

## HMAC Signing

Outbound webhook deliveries are signed using:

```text
HMAC-SHA256
```

Every webhook contains:

```text
X-Webhook-Signature
```

Receivers can verify authenticity and payload integrity.

---

## Rate Limiting

Rate limiting is enforced per tenant using Redis.

Current implementation:

```text
fixed window
60 second interval
per-tenant counters
```

Requests exceeding limits receive:

```http
429 Too Many Requests
```

---

# API Endpoints

## POST /tenants

Creates a tenant.

---

## POST /tenants/{tenant_id}/api-keys

Generates an API key for a tenant.

---

## POST /endpoints

Registers a webhook endpoint.

Authentication required.

---

## POST /events

Creates an event and schedules delivery.

Authentication required.

---

## GET /events/{event_id}/delivery_attempts

Returns delivery history for an event.

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
worker sends webhook
      ↓
destination receives webhook
      ↓
worker crashes before ACK
      ↓
message reprocessed
      ↓
duplicate delivery
```

This is an intentional distributed-systems trade-off.

---

# Performance

Phase 3 Benchmark

Environment:

* Local PostgreSQL
* Local Redis
* API authentication enabled
* Tenant isolation enabled
* Rate limiting enabled
* Worker processing active

Load test:
- 1000 requests
- 10 concurrent clients
- local PostgreSQL
- local Redis
- worker processing active

| Metric | Result |
|----------|----------|
| p50 | 83.93 ms |
| p95 | 141.06 ms |
| p99 | 203.94 ms |
| Requests/sec | 109.05 |

Success Rate:
100%

Failures:
0

Delivery Attempts:
1001

Delivered Events:
1001

Notes:

* webhook delivery remained asynchronous
* worker processing was active during benchmark
* benchmark includes authentication and rate-limiting overhead

---

# Failure Model

PostgreSQL is the durable source of truth.

Redis Streams provide:

* asynchronous processing
* worker coordination
* pending entry tracking
* delivery acknowledgment

If Redis stream state is lost:

* events remain persisted in PostgreSQL
* retry scheduler can recover pending work

---

# Known Limitations

Current limitations:

* duplicate deliveries remain possible
* dead letter queue is represented as database state
* retry scheduler uses polling
* worker and API run in the same process
* no observability stack yet
* no replay tooling yet
* no tracing implementation yet
* no metrics dashboard yet

---

# Deployment

Deployed on Render using:

* FastAPI Web Service
* Managed PostgreSQL
* Managed Redis

Background services:

```python
asyncio.create_task(worker_loop())
asyncio.create_task(retry_loop())
```

---

# Next Phase

Phase 4 — Observability

Planned additions:

* structured logging
* metrics
* tracing
* delivery dashboards
* operational visibility
* failure analytics
* performance monitoring
