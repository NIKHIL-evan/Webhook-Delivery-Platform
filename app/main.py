import asyncio
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.local_metrics import flush_forever
from app.core.redis_client import redis_client
from app.core.telemetry import request_trace_id
from app.routers import endpoints, events, attempts, tenants, generate_key, observability


class TraceMiddleware:
    """Pure ASGI middleware: gives every HTTP request a trace ID.

    Same behavior as the old BaseHTTPMiddleware version (contextvar + X-Trace-ID
    response header), but the app runs in the SAME task: no extra tasks, no
    internal response pipe, no repacking. Round 2 / Task 3.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app  # the next layer, remembered once at startup

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Startup/shutdown ("lifespan") and websockets pass through untouched.
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        trace_id = str(uuid.uuid4())
        token = request_trace_id.set(trace_id)
        trace_header = (b"x-trace-id", trace_id.encode("latin-1"))

        # Closure: remembers `send` and `trace_header`; adds the header
        # to the first response piece (status + headers) as it goes out.
        async def send_with_trace(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append(trace_header)
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace)
        finally:
            request_trace_id.reset(token)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # create redis stream and worker group
    try:
        await redis_client.xgroup_create(
            name="webhook_events",
            groupname="delivery_workers",
            id=0,
            mkstream=True
        )
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

    app.state.metrics_task = asyncio.create_task(flush_forever())

    yield

    app.state.metrics_task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(TraceMiddleware)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(attempts.router)
app.include_router(tenants.router)
app.include_router(generate_key.router)
app.include_router(observability.router)