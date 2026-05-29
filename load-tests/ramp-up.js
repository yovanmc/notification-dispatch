import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5100';

const successRate = new Rate('success_rate');
const enqueueDuration = new Trend('enqueue_duration', true);

export const options = {
    stages: [
        { duration: '30s', target: 10 },   // ramp up to 10 VUs
        { duration: '1m', target: 10 },    // hold 10 VUs
        { duration: '30s', target: 50 },   // ramp up to 50 VUs
        { duration: '1m', target: 50 },    // hold 50 VUs
        { duration: '30s', target: 0 },    // ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],  // 95th percentile under 500ms
        success_rate: ['rate>0.99'],       // 99% success rate
    },
};

export default function () {
    const idempotencyKey = uuidv4();
    const payload = JSON.stringify({
        channel: 'email',
        recipient: `loadtest-${idempotencyKey}@example.com`,
        body: 'Load test notification',
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': idempotencyKey,
        },
    };

    const start = Date.now();
    const res = http.post(`${BASE_URL}/notifications`, payload, params);
    enqueueDuration.add(Date.now() - start);

    const passed = check(res, {
        'status is 202': (r) => r.status === 202,
        'has jobId': (r) => JSON.parse(r.body).jobId !== undefined,
    });

    successRate.add(passed);
    sleep(0.1);
}
