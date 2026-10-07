"""Compare h11 (pure Python) vs httptools (C) on the same HTTP request.

Does what uvicorn does for every request on a kept-alive connection:
  1. parse the request bytes k6 sends
  2. build the response bytes (202 + small JSON body)
Run from the project root:  uv run python loadtest/parse_bench.py
"""
import time

import h11
import httptools

N = 100_000

BODY = (
    b'{"endpoint_id":"6f1c2c1e-3b7a-4c55-9a43-1d2e3f4a5b6c",'
    b'"idempotency_key":"1759850000000-k3j4h5",'
    b'"payload":{"order_id":123456,"amount":42.5}}'
)
RAW_REQUEST = (
    b"POST /events HTTP/1.1\r\n"
    b"Host: localhost:8000\r\n"
    b"User-Agent: k6/0.50.0 (https://k6.io/)\r\n"
    b"Content-Type: application/json\r\n"
    b"Api-Key: wh_live_0123456789abcdef0123456789abcdef\r\n"
    b"Content-Length: " + str(len(BODY)).encode() + b"\r\n"
    b"\r\n" + BODY
)
RESPONSE_HEADERS = [
    (b"date", b"Wed, 07 Oct 2026 16:00:00 GMT"),
    (b"server", b"uvicorn"),
    (b"content-type", b"application/json"),
    (b"content-length", b"2"),
    (b"x-trace-id", b"3f2a9c1e-7b44-4d0a-9e51-2c8f6a1b0d93"),
]
RESPONSE_BODY = b"{}"


# ---------- h11: everything in Python ----------
h11_conn = h11.Connection(h11.SERVER)


def one_request_h11():
    h11_conn.receive_data(RAW_REQUEST)
    request = h11_conn.next_event()       # h11.Request object
    data = h11_conn.next_event()          # h11.Data object (the body)
    h11_conn.next_event()                 # h11.EndOfMessage object
    out = h11_conn.send(h11.Response(status_code=202, headers=RESPONSE_HEADERS))
    out += h11_conn.send(h11.Data(data=RESPONSE_BODY))
    h11_conn.send(h11.EndOfMessage())
    h11_conn.start_next_cycle()           # ready for the next request
    return request.method, data.data, out


# ---------- httptools: parsing in C, small Python callbacks ----------
class Protocol:
    """Like uvicorn's callbacks: C calls these with ready-made pieces."""

    def on_message_begin(self):
        self.headers = []
        self.body = b""

    def on_url(self, url):
        self.url = url

    def on_header(self, name, value):
        self.headers.append((name.lower(), value))

    def on_body(self, body):
        self.body += body

    def on_message_complete(self):
        pass


proto = Protocol()
ht_parser = httptools.HttpRequestParser(proto)
STATUS_LINE = b"HTTP/1.1 202 Accepted\r\n"


def one_request_httptools():
    ht_parser.feed_data(RAW_REQUEST)      # C scans + validates everything
    parts = [STATUS_LINE]                 # uvicorn builds the response itself
    for name, value in RESPONSE_HEADERS:
        parts.extend([name, b": ", value, b"\r\n"])
    parts.append(b"\r\n")
    parts.append(RESPONSE_BODY)
    return ht_parser.get_method(), proto.body, b"".join(parts)


def bench(fn):
    for _ in range(1_000):                # warm up
        fn()
    start = time.process_time()           # CPU time, not wall time
    for _ in range(N):
        fn()
    return (time.process_time() - start) / N * 1_000_000   # µs per request


if __name__ == "__main__":
    # sanity: both understood the same request
    m1, b1, _ = one_request_h11()
    m2, b2, _ = one_request_httptools()
    assert m1 == m2 == b"POST" and bytes(b1) == b2 == BODY, "parsers disagree"

    t_h11 = bench(one_request_h11)
    t_ht = bench(one_request_httptools)
    print(f"h11       : {t_h11:6.2f} µs CPU per request")
    print(f"httptools : {t_ht:6.2f} µs CPU per request")
    print(f"h11 costs {t_h11 / t_ht:.1f}x more, {t_h11 - t_ht:.2f} µs extra per request")