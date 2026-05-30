# Baseline Load Test Results

> **Scope:** API enqueue baseline — measures `POST /notifications` submission throughput and latency only. Worker delivery, stream lag, and DLQ behaviour are not captured here.

## Test Summary

**Date:** May 30, 2026  
**Status:** Local baseline completed. Not a production benchmark.  
**Environment:** Local MacBook (Docker Desktop)  
**Docker Resource Limits:** Default Docker Desktop configuration

## Scenario Configuration

- **Script:** `load-tests/ramp-up.js`
- **Max VUs:** 50
- **Duration:** 3m30s (5 stages, graceful ramp-down 30s, graceful stop 30s)
- **Total Iterations:** 51,032

## Key Metrics

### Throughput & Success
- **HTTP Requests:** 51,032 total | 242.89 req/s average
- **Success Rate:** 100% (all requests returned 202 Accepted)
- **HTTP Request Failed:** 0%

### Latency (HTTP Response Time)
- **p50 (median):** 3.02 ms
- **p90:** 11.73 ms
- **p95:** 13.74 ms
- **p99:** Not explicitly reported, but max observed 122.95 ms
- **Average:** 4.98 ms
- **Min:** 337 microseconds
- **Max:** 122.95 ms

### Custom Metrics (API Enqueue)
- **enqueue_duration (avg):** 5.04 ms
- **enqueue_duration (p90):** 12 ms
- **enqueue_duration (p95):** 14 ms

### Execution Profile
- **Iteration Duration (avg):** 105.84 ms (includes think time between requests)
- **Iteration Duration (p95):** 114.73 ms
- **Total Test Duration:** 3m30s

### Network I/O
- **Data Received:** 15 MB (72 kB/s)
- **Data Sent:** 16 MB (77 kB/s)

## Threshold Results

All configured thresholds passed:
- ✓ `http_req_duration` p(95) < 500ms: **13.74 ms** (PASS)
- ✓ `success_rate` > 99%: **100%** (PASS)

## Observations

- API enqueue endpoint maintained stable p95 latency of ~13-14 ms across the full load test
- Zero errors or dropped requests with 50 concurrent virtual users
- The API enqueue endpoint handled the sustained ~243 req/s load without issues
- Graceful ramp-down phase executed cleanly

## To Rerun Locally

```bash
docker compose up -d
sleep 8
k6 run load-tests/ramp-up.js
docker compose down
```
