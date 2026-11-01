import http from 'k6/http';
import { check } from 'k6';

// ---------------------------------------------------------
// SETTINGS. Override any of them on the command line: -e NAME=value
// ---------------------------------------------------------
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';   // the API
const MOCK_URL = __ENV.MOCK_URL || 'http://localhost:9000';   // the mock receiver
const MODE     = __ENV.MODE || 'closed';                      // closed | open | count
const DELAY_MS = parseInt(__ENV.DELAY_MS || '0', 10);         // mock delay per webhook
const RATE     = parseInt(__ENV.RATE || '3000', 10);          // open: requests per second
const DURATION = __ENV.DURATION || '60s';                     // open: how long
const COUNT    = parseInt(__ENV.COUNT || '1000', 10);         // count: exact number of events

// ---------------------------------------------------------
// THE THREE TESTS
// ---------------------------------------------------------
const SCENARIOS = {
  // Test B: max throughput. 0 → 1,000 users over 1m50s; each user sends as fast as it can.
  closed: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { duration: '10s', target: 200 },
      { duration: '20s', target: 200 },
      { duration: '10s', target: 500 },
      { duration: '20s', target: 500 },
      { duration: '10s', target: 1000 },
      { duration: '40s', target: 1000 },
    ],
  },
  // Test D: cost. A fixed RATE per second, no matter how fast the server answers.
  open: {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 200,
    maxVUs: 2000,
  },
  // Test A: correctness. Exactly COUNT events, shared by 50 users.
  count: {
    executor: 'shared-iterations',
    vus: 50,
    iterations: COUNT,
    maxDuration: '5m',
  },
};

if (!SCENARIOS[MODE]) {
  throw new Error(`Unknown MODE "${MODE}". Use closed, open or count.`);
}

export const options = {
  scenarios: { main: SCENARIOS[MODE] },
};

// ---------------------------------------------------------
// Helper: POST some JSON; stop the whole test if the status isn't the expected one.
// ---------------------------------------------------------
function mustPost(url, bodyObject, headers, expectedStatus) {
  const res = http.post(url, JSON.stringify(bodyObject), { headers: headers });
  if (res.status !== expectedStatus) {
    // status 0 means "could not connect at all" (e.g. the mock isn't running)
    throw new Error(`setup: POST ${url} returned ${res.status}: ${res.body}`);
  }
  return res;
}

// ---------------------------------------------------------
// setup(): runs ONCE, before any user starts.
// Whatever it returns is handed to every call of the default function below.
// ---------------------------------------------------------
export function setup() {
  const jsonHeaders = { 'Content-Type': 'application/json' };

  // 1. Create a tenant. The API generates the signing secret and returns it here.
  const tenantRes = mustPost(
    `${BASE_URL}/tenants`,
    { name: `loadtest-${Date.now()}`, rate_limit: 1000000 },
    jsonHeaders,
    200
  );
  const tenantId = tenantRes.json('id');
  const signingSecret = tenantRes.json('signing_secret');

  // 2. Hand the secret (and the delay) to the mock, then clear its counters.
  mustPost(`${MOCK_URL}/config`, { secret: signingSecret, delay_ms: DELAY_MS }, jsonHeaders, 200);
  mustPost(`${MOCK_URL}/stats/reset`, {}, jsonHeaders, 200);

  // 3. Create an API key for the tenant.
  const keyRes = mustPost(
    `${BASE_URL}/tenants/${tenantId}/api-keys`,
    { name: 'loadtest-key' },
    jsonHeaders,
    200
  );
  const apiKey = keyRes.json('api_key');

  // 4. Register the mock as this tenant's webhook endpoint.
  const endpointRes = mustPost(
    `${BASE_URL}/endpoints`,
    { url: `${MOCK_URL}/webhook` },
    { 'Content-Type': 'application/json', 'API-Key': apiKey },
    200
  );

  return { apiKey: apiKey, endpointId: endpointRes.json('endpoint_id') };
}

// ---------------------------------------------------------
// default function: runs once per iteration (= one event), on every user.
// `data` is exactly what setup() returned.
// ---------------------------------------------------------
export default function (data) {
  const body = JSON.stringify({
    endpoint_id: data.endpointId,
    // __VU = this user's number, __ITER = how many times this user has run.
    // Together they're unique within a run, and each run has a fresh tenant.
    idempotency_key: `${__VU}-${__ITER}`,
    payload: { order_id: Math.floor(Math.random() * 1000000), amount: Math.random() * 500 },
  });

  const res = http.post(`${BASE_URL}/events`, body, {
    headers: { 'Content-Type': 'application/json', 'API-Key': data.apiKey },
  });

  check(res, { '202': (r) => r.status === 202 });
}