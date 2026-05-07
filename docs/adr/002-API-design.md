Context:

The webhook platform needs an API so developers can interact with the system.

The API should allow users to:

Register webhook URLs
Send events to those URLs
Check delivery attempt history

Decision:

I decided to use REST-style APIs with three endpoints.

Register an endpoint:

    POST /endpoints

    Used to register a webhook URL.

    Request body:

    {
    "url": "https://shipfast.com/webhooks"
    }

    Response:

    Response 201:  { "url_id": "...", "url": "...", "created_at": "..." }
    Response 422:  invalid request body

Send an event:

POST /events

    Used by applications to send events to a registered endpoint.

    Request body:

    {
    "url_id": "abc-123",
    "payload": {
        "order_id": "123",
        "status": "paid"
    }
    }

    Response:

    Response 201:  { "event_id": "...", "url_id": "...", "payload": {...}, "status": "pending", "created_at": "..." }
    Response 422:  invalid request body

Get delivery attempts:

    GET /events/{event_id}/delivery_attempts

    Used to check all delivery attempts for a specific event.

    Response:

    Response 200:  [
    {
        "attempt_id": "...",
        "event_id": "...",
        "attempt_number": 1,
        "status": "failed",
        "response_code": 503,
        "attempted_at": "..."
    },
    {
        "attempt_id": "...",
        "event_id": "...",
        "attempt_number": 2,
        "status": "delivered",
        "response_code": 200,
        "attempted_at": "..."
    }
]
Response 404:  event_id not found

Alternatives Considered:

I considered using:

GET /delivery_attempts?event_id=abc-123

This also works, but I chose:

GET /events/{event_id}/delivery_attempts

because delivery attempts always belong to an event. The nested path makes the relationship clearer and easier to understand.

I also considered putting event_id in the GET request body, but GET requests normally do not use request bodies in REST APIs.