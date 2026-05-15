# Webhook Delivery Platform

A self-hostable webhook delivery platform built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker-based processing. Clients can register destination webhook URLs, submit JSON events, and the system reliably delivers them as HTTP POST requests. The platform includes asynchronous processing, retry scheduling, dead letter handling, delivery attempt tracking, and idempotency protection.

---

# Status

Phase 2 — reliability and retry semantics implemented.

Implemented:
- REST API with FastAPI
- PostgreSQL schema design with foreign keys and UUID primary keys
- Async SQLAlchemy integration
- Redis Streams with consumer groups
- Background worker loop using asyncio
- Asynchronous webhook delivery
- Delivery attempt tracking
- Retry scheduling with exponential backoff
- Dead letter state handling
- Idempotency key support
- Failure tracking and retry persistence
- Public deployment on Render
- Basic load testing and performance measurement

---

# Architecture

```text
Client
   ↓
FastAPI API Server
   ↓
PostgreSQL (durable source of truth)
   ↓
Redis Stream (async delivery queue)
   ↓
Worker Loop
   ↓
Destination Webhook
```

Retry flow:

```text
delivery fails
    ↓
event marked failed
    ↓
next_retry_at scheduled
    ↓
retry scheduler re-enqueues event
    ↓
worker retries delivery
    ↓
max retries exceeded
    ↓
event marked dead
```

---

# Local Setup

## 1. Clone the repository

```bash
git clone <repo-url>
cd webhook-delivery-platform
```

## 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Create PostgreSQL database

```bash
sudo -u postgres createdb webhook_delivery
```

## 5. Configure environment variables

Create a `.env` file:

```env
DATABASE_URL=postgresql+asyncpg://postgres:<password>@localhost:5432/webhook_delivery
REDIS_URL=redis://localhost:6379
```

## 6. Run Redis locally

```bash
sudo apt install redis-server
sudo systemctl start redis-server
```

## 7. Run migrations

```bash
alembic upgrade head
```

## 8. Start the application

```bash
uvicorn app.main:app --reload
```

---

# API Endpoints

## POST /endpoints

Registers a destination webhook URL.

Request:

```json
{
  "url": "https://example.com/webhook"
}
```

---

## POST /events

Creates an event and asynchronously schedules webhook delivery.

Request:

```json
{
  "url_id": "<endpoint-uuid>",
  "payload": {
    "message": "hello world"
  },
  "idempotency_key": "optional-client-key"
}
```

Behavior:
- validates endpoint exists
- checks idempotency key if provided
- stores event in PostgreSQL
- writes delivery job into Redis Stream
- worker asynchronously delivers webhook
- failed deliveries are retried with backoff
- delivery attempts are persisted

---

## GET /events/{event_id}/delivery_attempts

Fetches all delivery attempts associated with an event.

---

# Reliability Features (Phase 2)

## Retry Scheduling

Failed deliveries are retried using exponential backoff.

Current retry flow:
- attempt 1 → retry after 2 seconds
- attempt 2 → retry after 4 seconds
- attempt 3 → retry after 8 seconds
- etc.

---

## Dead Letter Handling

Events stop retrying after reaching maximum retry attempts.

Dead events remain persisted in PostgreSQL for inspection and debugging.

---

## Idempotency

Clients may optionally send an `idempotency_key`.

If the same key is submitted multiple times:
- only one event is created
- duplicate requests return the original event

PostgreSQL unique constraints enforce correctness during concurrent requests.

---

# Performance (Phase 2)

Load test:
- 200 requests
- 10 concurrent clients
- warm Render instance

| Metric | Latency |
|---|---|
| p50 | 482ms |
| p95 | 1140ms |
| p99 | 1491ms |
| Requests/sec | 16.5 |

Average response wait:
- 0.5152 seconds

Notes:
- numbers reflect Render free tier limitations
- API latency no longer includes webhook delivery time
- webhook delivery is fully asynchronous

---

# Delivery Semantics

The system currently provides:

```text
at-least-once delivery
```

This means:
- events are not silently lost
- duplicate webhook deliveries are still possible under failure conditions

Example:
- worker delivers webhook successfully
- worker crashes before Redis ACK
- another worker retries same event

This is intentional and matches real-world distributed system trade-offs.

---

# Known Limitations

Current limitations:
- duplicate webhook deliveries are still possible
- dead letter queue is represented as database state, not separate Redis stream
- worker and API currently run inside same process
- retry scheduler uses polling
- no distributed rate limiting
- no webhook signing/authentication yet
- no tenant isolation
- no observability stack yet
- no replay tooling yet

---

# Failure Model

PostgreSQL acts as the durable source of truth.

Redis Streams are used for:
- asynchronous delivery coordination
- worker communication
- message acknowledgment tracking

If Redis loses transient stream state:
- events still exist durably in PostgreSQL

Retry scheduling state is currently stored in PostgreSQL using:
- `attempt_count`
- `next_retry_at`
- `status`

This keeps retry behavior durable across worker restarts and crashes.

---

# Deployment

Deployed publicly on Render using:
- FastAPI web service
- managed PostgreSQL
- managed Redis

The worker loop and retry scheduler currently run inside the same asyncio application process using:

```python
asyncio.create_task(worker_loop())
asyncio.create_task(retry_loop())
```
