import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = 'http://localhost:8000';
const MOCK_DESTINATION_URL = 'http://localhost:9000/webhook';

// Pick the load model per run:
//   k6 run loadtest/loadtest_k6.js                              -> closed (default, Test B)
//   k6 run -e MODE=open loadtest/loadtest_k6.js                 -> open, 3,000 req/s for 60 s
//   k6 run -e MODE=open -e RATE=4000 -e DURATION=30s loadtest/loadtest_k6.js
const MODE = __ENV.MODE || 'closed';
const RATE = parseInt(__ENV.RATE || '3000');
const DURATION = __ENV.DURATION || '60s';

const SCENARIOS = {
  // CLOSED: each VU waits for its reply before sending again (same ramp as before).
  closed: {
    executor: 'ramping-vus',
    startVUs: 1,
    stages: [
      { duration: '10s', target: 200 },
      { duration: '20s', target: 200 },
      { duration: '10s', target: 500 },
      { duration: '20s', target: 500 },
      { duration: '10s', target: 1000 },
      { duration: '40s', target: 1000 },
    ],
  },
  // OPEN: start RATE new requests every second, no matter how slow the server is.
  open: {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 500,   // ready from the start (Little's Law: ~135 busy at 3,000/s)
    maxVUs: 2000,           // extra room if the server slows down
  },
};

if (!SCENARIOS[MODE]) {
  throw new Error(`MODE must be "closed" or "open", got "${MODE}"`);
}

export const options = {
  scenarios: { [MODE]: SCENARIOS[MODE] },
};

function randomId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

// Runs ONCE, before any VU starts. Creates ONE tenant + key + endpoint.
// Whatever it returns is copied into every VU as `data`.
export function setup() {
  // ALL traffic now shares ONE tenant, so ONE rate-limit counter.
  // 1 billion per 60 s window = effectively unlimited for load tests.
  const tenantRes = http.post(
    `${BASE_URL}/tenants`,
    JSON.stringify({ name: `loadtest-${randomId()}`, rate_limit: 1000000000 }),
    JSON_HEADERS
  );
  if (tenantRes.status !== 200) {
    throw new Error(`setup: create tenant failed (${tenantRes.status}): ${tenantRes.body}`);
  }
  const tenantId = tenantRes.json('id');

  const keyRes = http.post(
    `${BASE_URL}/tenants/${tenantId}/api-keys`,
    JSON.stringify({ name: 'loadtest-key' }),
    JSON_HEADERS
  );
  if (keyRes.status !== 200) {
    throw new Error(`setup: create API key failed (${keyRes.status}): ${keyRes.body}`);
  }
  const apiKey = keyRes.json('api_key');

  const endpointRes = http.post(
    `${BASE_URL}/endpoints`,
    JSON.stringify({ url: MOCK_DESTINATION_URL }),
    { headers: { 'Content-Type': 'application/json', 'API-Key': apiKey } }
  );
  if (endpointRes.status !== 200) {
    throw new Error(`setup: create endpoint failed (${endpointRes.status}): ${endpointRes.body}`);
  }

  return { apiKey: apiKey, endpointId: endpointRes.json('endpoint_id') };
}

// Runs on every iteration of every VU: ONLY POST /events is measured.
export default function (data) {
  const payload = JSON.stringify({
    endpoint_id: data.endpointId,
    idempotency_key: randomId(),
    payload: { order_id: Math.floor(Math.random() * 1000000), amount: Math.random() * 500 },
  });

  const res = http.post(`${BASE_URL}/events`, payload, {
    headers: { 'Content-Type': 'application/json', 'API-Key': data.apiKey },
  });

  check(res, { '202': (r) => r.status === 202 });
}