# Webhook Delivery Platform

A self-hostable webhook delivery service built with FastAPI and PostgreSQL. Clients can register destination webhook URLs, submit events with JSON payloads, and the system delivers those events as HTTP POST requests to the registered endpoints. The platform also stores delivery attempts and basic delivery status information for observability and debugging.

---

## Status

Phase 0 — core synchronous delivery flow implemented.

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
```

### 6. Run the server

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

#### Response

Returns the created endpoint with generated UUID.

---

### POST `/events`

Creates an event and immediately attempts webhook delivery to the registered endpoint.

#### Request

```json
{
  "url_id": "<endpoint-uuid>",
  "payload": {
    "order_id": "123",
    "status": "paid"
  }
}
```

#### Behavior

- validates endpoint exists
- stores event in database
- sends HTTP POST to destination URL
- stores delivery attempt result

---

### GET `/events/{event_id}/delivery_attempts`

Fetches delivery attempts associated with an event.

---

## Known Limitations — Phase 0

- Webhook delivery is synchronous. The API request waits for delivery to complete before returning a response.
- No retry mechanism exists yet for failed deliveries.
- No background workers or queue system are implemented.
- Failed deliveries require manual resubmission.
- No webhook signing/authentication yet.
- No rate limiting or idempotency support yet.
