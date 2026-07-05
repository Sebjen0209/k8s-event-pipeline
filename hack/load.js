// k6 load test for the ingest path. Thresholds make this a *gate*, not a
// demo: CI fails if p95 crosses 300ms or more than 1% of requests fail.
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 25 }, // ramp up
        { duration: '45s', target: 25 }, // hold
        { duration: '10s', target: 0 },  // drain
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<300'],
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:18080';
const TYPES = ['page_view', 'click', 'purchase', 'signup'];

export default function () {
  const body = JSON.stringify({
    type: TYPES[Math.floor(Math.random() * TYPES.length)],
    source: 'k6',
    payload: { vu: __VU, iter: __ITER },
  });
  const res = http.post(`${BASE}/v1/events`, body, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
  sleep(0.05 + Math.random() * 0.1);
}
