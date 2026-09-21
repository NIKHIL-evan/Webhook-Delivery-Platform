import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = 'http://localhost:8000';
const MOCK_DESTINATION_URL = 'http://localhost:9000/webhook';

export const options = {
  stages: [
    { duration: '10s', target: 200 },
    { duration: '20s', target: 200 },
    { duration: '10s', target: 500 },
    { duration: '20s', target: 500 },
    { duration: '10s', target: 1000 },
    { duration: '40s', target: 1000 },
  ],
};

function randomId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

let apiKey = null;
let endpointId = null;

function setupTenant() {
  const tenantRes = http.post(
    `${BASE_URL}/tenants`,
    JSON.stringify({ name: `loadtest-${randomId()}`, rate_limit: 1000000 }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  const tenantId = tenantRes.json('id');

  const keyRes = http.post(
    `${BASE_URL}/tenants/${tenantId}/api-keys`,
    JSON.stringify({ name: 'loadtest-key' }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  apiKey = keyRes.json('api_key');

  const endpointRes = http.post(
    `${BASE_URL}/endpoints`,
    JSON.stringify({ url: MOCK_DESTINATION_URL }),
    { headers: { 'Content-Type': 'application/json', 'API-Key': apiKey } }
  );
  endpointId = endpointRes.json('endpoint_id');
}

export default function () {
  if (!apiKey) setupTenant();

  const payload = JSON.stringify({
    endpoint_id: endpointId,
    idempotency_key: randomId(),
    payload: { order_id: Math.floor(Math.random() * 1000000), amount: Math.random() * 500 },
  });

  const res = http.post(`${BASE_URL}/events`, payload, {
    headers: { 'Content-Type': 'application/json', 'API-Key': apiKey },
  });

  check(res, { '202': (r) => r.status === 202 });
}