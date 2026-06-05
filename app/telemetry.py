from contextvars import ContextVar

request_trace_id: ContextVar[str] = ContextVar("request_trace_id", default="unknown")