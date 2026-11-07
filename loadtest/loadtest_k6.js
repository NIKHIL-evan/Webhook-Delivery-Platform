import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const MODE     = __ENV.MODE || 'closed';
const DELAY_MS = parseInt(__ENV.DELAY_MS || '0', 10);
const RATE     = parseInt(__ENV.RATE || '3000', 10);
const DURATION = __ENV.DURATION || '60s';
const COUNT    = parseInt(__ENV.COUNT || '1000', 10);

const SCENARIOS = {
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
  open: {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: 200,
    maxVUs: 2000,
  },
  count: {
    executor: 'shared-iterations',
    vus: 50,
    iterations: COUNT,
    maxDuration: '5m',
  },
};

if (!SCENARIOS[MODE]) throw new Error(`Unknown MODE "${MODE}"`);
export const options = { scenarios: { main: SCENARIOS[MODE] } };

function mustPost(url, bodyObject, headers, expectedStatus) {
  const res = http.post(url, JSON.stringify(bodyObject), { headers: headers });
  if (res.status !== expectedStatus) throw new Error(`setup: POST ${url} returned ${res.status}`);
  return res;
}

export function setup() {
  const jsonHeaders = { 'Content-Type': 'application/json' };

  const tenantRes = mustPost(`${BASE_URL}/tenants`, { name: `loadtest-${Date.now()}`, rate_limit: 1000000 }, jsonHeaders, 200);
  const tenantId = tenantRes.json('id');
  const signingSecret = tenantRes.json('signing_secret');
  
  const keyRes = mustPost(`${BASE_URL}/tenants/${tenantId}/api-keys`, { name: 'loadtest-key' }, jsonHeaders, 200);
  const apiKey = keyRes.json('api_key');

  // Configure port 9000 (mock_config.json syncs it to the other 7 workers)
  mustPost(`http://localhost:9000/config`, { secret: signingSecret, delay_ms: DELAY_MS }, jsonHeaders, 200);

  let endpointIds = [];
  for (let p = 9000; p <= 9001; p++) {
    mustPost(`http://localhost:${p}/stats/reset`, {}, jsonHeaders, 200);
    const epRes = mustPost(
      `${BASE_URL}/endpoints`,
      { url: `http://localhost:${p}/webhook` },
      { 'Content-Type': 'application/json', 'API-Key': apiKey },
      200
    );
    endpointIds.push(epRes.json('endpoint_id'));
  }

  return { apiKey: apiKey, endpointIds: endpointIds };
}

export default function (data) {
  // Distribute events evenly across the 2 mock servers
  const epId = data.endpointIds[__ITER % 2];
  
  const body = JSON.stringify({
    endpoint_id: epId,
    idempotency_key: `${__VU}-${__ITER}`,
    payload: { order_id: Math.floor(Math.random() * 1000000), amount: Math.random() * 500 },
  });

  const res = http.post(`${BASE_URL}/events`, body, {
    headers: { 'Content-Type': 'application/json', 'API-Key': data.apiKey },
  });

  check(res, { '202': (r) => r.status === 202 });
}