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
│  consumer group: worker-group          │
└────────────┬───────────────────────────┘
             │ XREADGROUP / XAUTOCLAIM (30s idle)
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
│         attempt 3 → XACK + DLQ                  │
└────────┬────────────────────┬───────────────────┘
         │                    │
         ▼                    ▼
notification:status:{jobId}  notifications:dlq
(24h TTL)                    (dead-letter stream)
```

**Shared models** (`NotificationDispatch.Core`): `NotificationRequest`, `NotificationJob`, `NotificationStatus`, `DeliveryState`.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose (for integration tests and local stack)
- [k6](https://k6.io/docs/get-started/installation/) (optional — only for load tests)

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
- `400` — `Idempotency-Key` header missing, blank, or >256 chars; or invalid channel/recipient/body

---

### GET /notifications/{id}

Poll job status.

**Responses**

- `200` — `{"state":"Queued|Processing|Delivered|DeadLettered","attempts":1,...}`
- `404` — job not found (never submitted or TTL expired)

---

### GET /notifications/dlq?count=20

List recently dead-lettered jobs. `count` must be between 1 and 100 (default 20).

**Response**

- `200` — array of job objects
- `400` — `count` out of range

---

### POST /notifications/dlq/{jobId}/replay

Re-enqueue a dead-lettered job under a new job ID.

**Response**

- `202` — `{"originalJobId":"<uuid>","newJobId":"<uuid>"}`
- `404` — no DLQ entry found for `jobId`
- `422` — DLQ entry exists but payload field is missing (data corruption)

## Design Decisions

**Redis Streams over in-memory queue** — Streams are durable and survive worker restarts. Consumer groups allow multiple worker instances to process jobs concurrently. Delivery is **at-least-once**: if a worker crashes after sending but before ACKing, `XAUTOCLAIM` will redeliver the entry. Callers should treat webhook endpoints as idempotent; the worker forwards `X-Idempotency-Key: {jobId}` on every attempt to help downstream systems deduplicate. `XAUTOCLAIM` reclaims stalled entries if a worker crashes mid-job.

**Lua script for idempotency** — The check-set-enqueue sequence runs atomically in a single round trip. Without a Lua script, a window between checking the key and writing the stream entry creates a TOCTOU race under concurrent duplicate submissions.

**In-process retry with exponential backoff** — Three attempts (backoff 1s / 2s between attempts) happen inside the worker before a job is dead-lettered. This keeps failure handling co-located with the sender logic and avoids the complexity of a separate retry queue for a single-worker deployment.

**Fake email and SMS senders** — The scope of this service is demonstrating the dispatch architecture (routing, retry, idempotency, DLQ). Real SMTP or Twilio integration is a thin swap at the sender layer. Webhook delivery is real (HTTP POST) to show the pattern end-to-end.

## Failure Modes

| Failure | Behaviour |
|---|---|
| Redis down | API returns `503`; Worker halts stream polling and logs errors; status store unavailable |
| Worker crash mid-processing | `XAUTOCLAIM` reclaims the pending entry after 30 s; **at-least-once** — if crash occurs after send but before ACK, the webhook may fire again |
| Webhook target down | Retried 3 times with exponential backoff, then dead-lettered; recoverable via `/dlq/{jobId}/replay` |

## Limitations

**Delivery semantics:** At-least-once. A job can be sent more than once if the worker crashes after delivery but before the stream ACK. Webhook endpoints should be idempotent. Email and SMS channels are logged stubs; real SMTP/Twilio delivery would inherit the same at-least-once guarantee.

**SSRF:** Webhook delivery accepts arbitrary `http://` or `https://` URLs. Loopback, private IP ranges, link-local, and cloud metadata endpoints (e.g. `169.254.169.254`) are reachable. **Do not expose this service publicly without an IP allowlist.** This is a local demo.

**Stream retention:** The job stream has no retention policy — ACKed entries remain indefinitely. Status records expire after 24 hours. Both will be addressed in a future change.

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
