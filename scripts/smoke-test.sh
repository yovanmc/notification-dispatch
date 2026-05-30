#!/usr/bin/env bash
# Smoke test: validates the critical user journey against a running docker compose stack.
# Usage: ./scripts/smoke-test.sh [API_BASE_URL]
# Defaults to http://localhost:5100

set -euo pipefail

API="${1:-http://localhost:5100}"
MAX_WAIT=30  # seconds to wait for each state transition

poll_state() {
    local job_id="$1"
    local expected="$2"
    local waited=0
    while [ $waited -lt $MAX_WAIT ]; do
        local state
        state=$(curl -sf "$API/notifications/$job_id" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])" 2>/dev/null || echo "")
        if [ "$state" = "$expected" ]; then
            echo "  ✓ Job $job_id reached state $expected"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "  ✗ Job $job_id did not reach $expected after ${MAX_WAIT}s (last state: $state)"
    return 1
}

echo "=== Smoke test against $API ==="

# 1. Health check
echo
echo "1. Health check..."
curl -sf "$API/health" > /dev/null
echo "  ✓ /health returned 200"

# 2. Submit email job
echo
echo "2. Submit email notification..."
KEY=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
RESPONSE=$(curl -sf -X POST "$API/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"channel":"email","recipient":"smoke@example.com","body":"Smoke test"}')
JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['jobId'])")
echo "  ✓ Accepted job $JOB_ID"

# 3. Poll until Delivered (worker processes email jobs)
echo
echo "3. Wait for email job to be Delivered..."
poll_state "$JOB_ID" "Delivered"

# 4. Idempotency — same key returns same job
echo
echo "4. Idempotency check..."
RESPONSE2=$(curl -sf -X POST "$API/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"channel":"email","recipient":"smoke@example.com","body":"Smoke test"}')
JOB_ID2=$(echo "$RESPONSE2" | python3 -c "import sys,json; print(json.load(sys.stdin)['jobId'])")
if [ "$JOB_ID" = "$JOB_ID2" ]; then
    echo "  ✓ Duplicate key returned same jobId"
else
    echo "  ✗ Expected same jobId ($JOB_ID) but got $JOB_ID2"
    exit 1
fi

echo
echo "=== Smoke test PASSED ==="
