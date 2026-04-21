#!/usr/bin/env bash
#
# Docker smoke test for vexobj.
#
# Runs a just-built vexobj image end-to-end: boots the container, waits for
# /health, captures the admin key from stdout, then walks the minimum
# roundtrip an operator expects to work on day one — create bucket, PUT
# object, GET it back and byte-compare. Any failure prints the container
# logs before exiting so CI output shows what the server saw.
#
# Usage:
#   .github/scripts/docker-smoke.sh [IMAGE]
#
# IMAGE defaults to vexobj-smoke:ci. PORT can be overridden with $PORT
# (default 18000 so a running local vexobj on 8000 doesn't collide).

set -euo pipefail

IMAGE="${1:-vexobj-smoke:ci}"
CONTAINER="vexobj-smoke-$$"
PORT="${PORT:-18000}"

log() { printf '>>> %s\n' "$*"; }

cleanup() {
    local code=$?
    if [ $code -ne 0 ]; then
        echo "--- container logs (last 200 lines) ---"
        docker logs --tail 200 "$CONTAINER" 2>&1 || true
        echo "--- end container logs ---"
    fi
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    exit $code
}
trap cleanup EXIT

log "starting $IMAGE as $CONTAINER on :$PORT"
docker run -d --name "$CONTAINER" \
    -p "$PORT:8000" \
    -e VEXOBJ_BIND=0.0.0.0:8000 \
    -e VEXOBJ_DATA_DIR=/data \
    -e VEXOBJ_AUTH_ENABLED=true \
    "$IMAGE" >/dev/null

log "waiting for /health"
health_ok=0
for i in $(seq 1 60); do
    if curl -sfo /dev/null "http://localhost:$PORT/health"; then
        log "health OK after ${i}s"
        health_ok=1
        break
    fi
    sleep 1
done
if [ "$health_ok" -ne 1 ]; then
    log "FAIL: /health never responded within 60s"
    exit 1
fi

log "extracting admin key from container logs"
KEY=$(docker logs "$CONTAINER" 2>&1 | grep -oE 'vex_[A-Za-z0-9_-]+' | head -1 || true)
if [ -z "$KEY" ]; then
    log "FAIL: no admin key found in container logs"
    exit 1
fi
log "admin key captured"

# ── Create bucket (native API, 201) ────────────────────────────────
log "POST /v1/buckets"
code=$(curl -sw '%{http_code}' -o /tmp/resp.$$ \
    -X POST "http://localhost:$PORT/v1/buckets" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/json" \
    -d '{"name":"smoke","public":false}')
if [ "$code" != "201" ]; then
    log "FAIL create bucket: HTTP $code"
    cat /tmp/resp.$$ || true
    exit 1
fi

# ── PUT object (native API, 201) ───────────────────────────────────
PAYLOAD='hello smoke test, from vexobj docker-smoke.sh'
log "PUT /v1/objects/smoke/hello.txt"
code=$(curl -sw '%{http_code}' -o /dev/null \
    -X PUT "http://localhost:$PORT/v1/objects/smoke/hello.txt" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: text/plain" \
    --data-binary "$PAYLOAD")
if [ "$code" != "201" ]; then
    log "FAIL PUT object: HTTP $code"
    exit 1
fi

# ── GET object and byte-compare ─────────────────────────────────────
log "GET /v1/objects/smoke/hello.txt"
got=$(curl -sf "http://localhost:$PORT/v1/objects/smoke/hello.txt" \
    -H "Authorization: Bearer $KEY")
if [ "$got" != "$PAYLOAD" ]; then
    log "FAIL GET: byte mismatch"
    log "  want: $PAYLOAD"
    log "  got:  $got"
    exit 1
fi

# ── S3 listing (verify the S3 mux is also up) ───────────────────────
log "GET /s3 (S3 list buckets, Bearer shortcut)"
code=$(curl -sw '%{http_code}' -o /tmp/resp.$$ \
    "http://localhost:$PORT/s3" \
    -H "Authorization: Bearer $KEY")
if [ "$code" != "200" ]; then
    log "FAIL S3 list: HTTP $code"
    cat /tmp/resp.$$ || true
    exit 1
fi
if ! grep -q '<ListAllMyBucketsResult' /tmp/resp.$$; then
    log "FAIL S3 list: no ListAllMyBucketsResult in body"
    cat /tmp/resp.$$
    exit 1
fi

rm -f /tmp/resp.$$
log "smoke test passed"
