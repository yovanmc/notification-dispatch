# notification-dispatch

A production-patterned notification dispatch service demonstrating durable job queuing via Redis Streams, idempotent submission, and multi-channel routing with automatic retry and dead-lettering.

## Architecture

```
Client
  │
  ▼
┌─────────────────────────────────┐
│  NotificationDispatch.Api       │
│  POST /notifications            │
│    Lua: idempotency check       │
│      └─ XADD notifications:jobs │
│  GET  /notifications/{id}       │
│  GET  /notifications/dlq        │
│  POST /notifications/dlq/{id}/replay │
└────────────┬────────────────────┘
             │ Redis Stream
             ▼
┌────────────────────────────────────────┐
│  notifications:jobs (Redis Stream)     │
│  consumer group: notification-workers  │
└────────────┬───────────────────────────┘
             │ XREADGROUP / XAUTOCLAIM (30s)
             ▼
┌─────────────────────────────────────────────────┐
│  NotificationDispatch.Worker                    │
│                                                 │
│  JobConsumer                                    │
│    ├─ EmailSender   (logged/fake)               │
│    ├─ SmsSender     (logged/fake)               │
│    └─ WebhookSender (real HTTP POST)            │
│                                                 │
│  Retry: attempt 1 → 1s → attempt 2 → 2s →      │
│         attempt 3 → 4s → XACK + DLQ            │
└────────┬────────────────────┬───────────────────┘
         │                    │
         ▼                    ▼
notification:status:{jobId}  notifications:dlq
(24h TTL)                    (dead-letter stream)
```

**Shared models** (`NotificationDispatch.Core`): `NotificationRequest`, `NotificationJob`, `NotificationStatus`, `DeliveryState`.

## Quick Start

Requires Docker and Docker Compose.

```bash
docker compose up
```

API is available at `http://localhost:5100`.

Submit a notification:

```bash
curl -X POST http://localhost:5100/notifications \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"channel":"email","recipient":"you@example.com","body":"Hello"}'
# 202 {"jobId":"<uuid>"}
```

Check job status:

```bash
curl http://localhost:5100/notifications/<jobId>
```

## API Reference

### POST /notifications

Submit a notification job.

**Headers**

| Header | Required | Description |
|---|---|---|
| `Idempotency-Key` | yes | Client-generated UUID; duplicate keys return the existing job |

**Body**

```json
{"channel":"email","recipient":"user@example.com","body":"Hello"}
```

Supported channels: `email`, `sms`, `webhook`.

**Responses**

- `202` — new job accepted: `{"jobId":"<uuid>"}`
- `200` — duplicate key, returns existing job: `{"jobId":"<uuid>","status":{...}}`

---

### GET /notifications/{id}

Poll job status.

**Responses**

- `200` — `{"state":"Queued|Processing|Delivered|DeadLettered","attempts":1,...}`
- `404` — job not found (never submitted or TTL expired)

---

### GET /notifications/dlq?count=20

List recently dead-lettered jobs. `count` max is 100.

**Response**

- `200` — array of job objects

---

### POST /notifications/dlq/{jobId}/replay

Re-enqueue a dead-lettered job under a new job ID.

**Response**

- `202` — `{"originalJobId":"<uuid>","newJobId":"<uuid>"}`

## Design Decisions

**Redis Streams over in-memory queue** — Streams are durable and survive worker restarts. Consumer groups allow multiple worker instances to process jobs concurrently without duplicates. `XAUTOCLAIM` reclaims stalled entries if a worker crashes mid-job.

**Lua script for idempotency** — The check-set-enqueue sequence runs atomically in a single round trip. Without a Lua script, a window between checking the key and writing the stream entry creates a TOCTOU race under concurrent duplicate submissions.

**In-process retry with exponential backoff** — Three attempts (backoff 1s / 2s / 4s) happen inside the worker before a job is dead-lettered. This keeps failure handling co-located with the sender logic and avoids the complexity of a separate retry queue for a single-worker deployment.

**Fake email and SMS senders** — The scope of this service is demonstrating the dispatch architecture (routing, retry, idempotency, DLQ). Real SMTP or Twilio integration is a thin swap at the sender layer. Webhook delivery is real (HTTP POST) to show the pattern end-to-end.

## Failure Modes

| Failure | Behaviour |
|---|---|
| Redis down | API returns `503`; Worker halts stream polling and logs errors; status store unavailable |
| Worker crash mid-processing | `XAUTOCLAIM` reclaims the pending entry after 30 s; no message loss |
| Webhook target down | Retried 3 times with exponential backoff, then dead-lettered; recoverable via `/dlq/{jobId}/replay` |

## Running Tests

Requires Docker (integration tests use Testcontainers to spin up `redis:7-alpine` and WireMock).

```bash
dotnet test
```

This runs all unit and integration tests.

## Load Testing

Requires [k6](https://k6.io/docs/get-started/installation/) and the stack running.

```bash
docker compose up -d
k6 run load-tests/ramp-up.js
```

Results are written to `results/`.
