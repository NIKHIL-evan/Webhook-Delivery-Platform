import uuid
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager

from app.telemetry import request_trace_id 
from app.routers import endpoints, events, attempts, tenants, generate_key, observability
from app.redis_client import redis_client
from app.middleware import metrics_middleware

class TraceMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 1. Generate the ID
        trace_id = str(uuid.uuid4())
        
        # 2. Bind to local execution context
        token = request_trace_id.set(trace_id)
        
        try:
            # 3. Pass control to the router
            response = await call_next(request)
            
            # 4. Attach to outbound response
            response.headers["X-Trace-ID"] = trace_id
            return response
        finally:
            # 5. Clean up memory
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

    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(TraceMiddleware)
app.middleware("http")(metrics_middleware)
app.include_router(endpoints.router)
app.include_router(events.router)
app.include_router(attempts.router)
app.include_router(tenants.router)
app.include_router(generate_key.router)
app.include_router(observability.router)