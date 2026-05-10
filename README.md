# Webhook Delivery Platform

A self-hostable webhook delivery service built with FastAPI, PostgreSQL, Redis Streams, and asynchronous worker-based processing. Clients can register destination webhook URLs, submit JSON events, and the system delivers them asynchronously as HTTP POST requests. Delivery attempts and event states are persisted for observability and failure tracking.

---

## Status

Phase 1 — asynchronous webhook delivery implemented using Redis Streams and worker-based processing.

Implemented:
- REST API with FastAPI
- PostgreSQL schema design with foreign keys and UUID primary keys
- Async SQLAlchemy integration
- Redis Streams with consumer groups
- Background worker loop using asyncio
- Asynchronous webhook delivery
- Delivery attempt tracking
- Public deployment on Render
- Basic load testing and performance measurement

---

## Architecture

```text
Client
   ↓
FastAPI API Server
   ↓
PostgreSQL (source of truth)
   ↓
Redis Stream (delivery queue)
   ↓
Worker Loop
   ↓
Destination Webhook
```

---

## Local Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd webhook-delivery-platform
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create PostgreSQL database

```bash
sudo -u postgres createdb webhook_delivery
```

### 5. Configure environment variables

Create a `.env` file:

```env
DATABASE_URL=postgresql+asyncpg://postgres:<password>@localhost:5432/webhook_delivery
REDIS_URL=redis://localhost:6379
```

### 6. Run Redis locally

```bash
sudo apt install redis-server
sudo systemctl start redis-server
```

### 7. Run the application

```bash
uvicorn app.main:app --reload
```

---

## API Endpoints

### POST `/endpoints`

Registers a destination webhook URL.

#### Request

```json
{
  "url": "https://example.com/webhook"
}
```

---

### POST `/events`

Creates an event, stores it in PostgreSQL, and writes a delivery job to the Redis Stream.

#### Request

```json
{
  "url_id": "<endpoint-uuid>",
  "payload": {
    "message": "hello world"
  }
}
```

#### Behavior

- validates endpoint exists
- stores event in PostgreSQL
- writes job into Redis Stream
- worker asynchronously delivers webhook
- delivery attempts are stored in PostgreSQL

---

### GET `/events/{event_id}/delivery_attempts`

Fetches delivery attempts associated with an event.

---

## Performance (Phase 1)

Load test: 200 requests, 10 concurrent, warm Render instance.

| Metric | Latency |
|--------|---------|
| p50 | 482ms |
| p95 | 1140ms |
| p99 | 1491ms |
| Requests/sec | 16.5 |

resp wait: 0.5152 secs average

Note:
- numbers reflect Render free tier constraints
- API response time no longer includes webhook delivery latency
- webhook delivery is fully asynchronous

---

## Known Limitations — Phase 1

- At-least-once delivery may produce duplicate webhook deliveries
- No retry backoff strategy yet
- No dead-letter queue support
- Worker and API currently run inside same process
- Redis Stream recovery/reconciliation jobs not implemented yet
- No webhook signing/authentication
- No rate limiting or tenant isolation
- No observability stack yet

---

## Failure Model

The system uses at-least-once delivery semantics.

If a worker crashes after successfully delivering a webhook but before acknowledging the Redis Stream message, another worker may retry the same event. This prevents message loss but allows duplicate deliveries.

PostgreSQL acts as the durable source of truth. Redis Streams are used as the asynchronous delivery mechanism.

---

## Deployment

Deployed publicly on Render using:
- FastAPI web service
- managed PostgreSQL
- managed Redis

API and worker currently run concurrently inside the same asyncio event loop using:

```python
asyncio.create_task(worker_loop())
```
