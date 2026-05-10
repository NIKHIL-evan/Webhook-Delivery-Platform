# ADR 003: Worker Architecture

## Context

Phase 1 introduced asynchronous webhook delivery using Redis Streams and worker-based processing. The system needed a continuously running worker loop capable of consuming messages from the Redis Stream and delivering webhooks independently of the API request lifecycle.

The original design intended the API service and worker service to run as separate processes. However, Render's free tier does not provide free background worker services, making separate deployment impractical for this phase.

## Decision

The worker loop is started inside the FastAPI application's lifespan using:

```python
asyncio.create_task(worker_loop())
```

This allows the API server and the Redis worker loop to run concurrently inside the same Python process and asyncio event loop.

The API continues handling HTTP requests while the worker asynchronously polls Redis Streams and processes delivery jobs in the background.

## Alternatives Considered

### Separate Background Worker Service

A separate worker deployment was considered as the cleaner production architecture. This would isolate API traffic from background processing and allow workers to scale independently.

However, this approach requires an additional paid Render background worker service and was therefore rejected for Phase 1.

## Trade-offs Accepted

Running both the API and worker in the same process introduces tighter coupling between request handling and background processing.

If the process crashes:
- both API handling and worker execution stop simultaneously
- worker scaling cannot happen independently
- CPU-intensive worker logic could affect API responsiveness

This architecture is acceptable for Phase 1 because webhook delivery workloads are still small and primarily intended for learning asynchronous delivery patterns and Redis Streams mechanics.

Future phases may separate the worker into an independently deployable service.

## Additional Alternatives Considered

### Simple Redis List Queue

A simpler queue implementation using Redis Lists (`LPUSH` / `BRPOP`) was considered instead of Redis Streams.

Redis Lists would have been sufficient for basic producer-consumer behavior, but they do not provide native support for:
- consumer groups
- pending message tracking
- acknowledgments
- automatic recovery of unacknowledged messages

Using Redis Lists would require manually implementing worker coordination, retry handling, and crash recovery logic.

Redis Streams were selected because they provide these distributed delivery semantics natively and better align with the Phase 1 learning goals around:
- at-least-once delivery
- acknowledgments
- consumer groups
- worker crash recovery