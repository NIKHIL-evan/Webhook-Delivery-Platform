Context

This system needs a database because it has to remember things even after the server restarts.

The system needs to store:

Webhook URLs registered by users
Events sent by users
Delivery attempts and retry history

For example, if a webhook delivery fails, the system should remember to retry later. Users should also be able to check whether their events were delivered successfully or failed.

Decision

I decided to use three tables.

1. endpoints

    Stores webhook URLs registered by users.

    url_id
    url
    created_at

2. events

    Stores incoming events.

    event_id
    url_id (foreign key)
    payload (jsonb)
    status
    created_at

3. delivery_attempts

    Stores every delivery try for an event.

    attempt_id
    event_id (foreign key)
    attempt_number
    status
    response_code
    attempted_at

The payload is stored using PostgreSQL jsonb because event data can have different JSON structures and jsonb data type is faster then json data type because it doest require reparsing for querying and processing.

The events table also stores a status column so the system can quickly find pending events without checking all delivery attempts every time.

Alternatives Considered

I first thought about storing everything in one table, but that would repeat the same webhook URL many times and make retry tracking messy.

I also considered keeping status only inside delivery_attempts, but then the system would need extra queries and joins just to find pending, delivered or failed events.

Trade-offs Accepted

The event status is stored in two places:

events
delivery_attempts

This duplicates some data, but it improves performance and makes common queries simpler.