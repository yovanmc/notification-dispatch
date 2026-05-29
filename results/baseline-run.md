# Baseline Load Test Results

**Status:** Not yet run. Start the system with `docker compose up`, then:

```bash
k6 run load-tests/ramp-up.js
```

Record results here after a verified local run including:
- Date and environment details (machine specs, Docker resource limits)
- k6 summary output
- p50, p95, p99 latency
- Total requests and throughput
- Success rate
- Any errors or dropped requests
