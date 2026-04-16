#!/usr/bin/env bash
# VaultFS Load Test Suite
# Usage: ./bench.sh <base_url> <api_key> [concurrency] [requests]
#
# Prerequisites: curl, bash
# Optional: wrk (for sustained throughput tests)

set -euo pipefail

BASE="${1:?Usage: bench.sh <base_url> <api_key> [concurrency] [requests]}"
KEY="${2:?Missing api_key}"
CONCURRENCY="${3:-10}"
REQUESTS="${4:-100}"

BUCKET="bench-$(date +%s)"
REPORT_FILE="/tmp/vaultfs-bench-$(date +%Y%m%d-%H%M%S).json"

echo "=== VaultFS Benchmark Suite ==="
echo "Target:      $BASE"
echo "Concurrency: $CONCURRENCY"
echo "Requests:    $REQUESTS"
echo ""

# ── Setup ────────────────────────────────────────────────
echo "[1/7] Setting up benchmark bucket..."
curl -sf -X POST "$BASE/v1/buckets" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"$BUCKET\"}" > /dev/null

# Generate test files
TMPDIR=$(mktemp -d)
dd if=/dev/urandom of="$TMPDIR/1kb.bin" bs=1K count=1 2>/dev/null
dd if=/dev/urandom of="$TMPDIR/100kb.bin" bs=1K count=100 2>/dev/null
dd if=/dev/urandom of="$TMPDIR/1mb.bin" bs=1M count=1 2>/dev/null
dd if=/dev/urandom of="$TMPDIR/10mb.bin" bs=1M count=10 2>/dev/null

# ── Benchmark functions ──────────────────────────────────
bench_upload() {
    local label="$1" file="$2" count="$3" concurrency="$4"
    local size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file")

    echo -n "  $label (${size} bytes, ${count} req, ${concurrency} conc)... "

    local start=$(date +%s%N)
    seq 1 "$count" | xargs -P "$concurrency" -I{} \
        curl -sf -X PUT "$BASE/v1/objects/$BUCKET/bench-{}-$(basename $file)" \
            -H "Authorization: Bearer $KEY" \
            -H "Content-Type: application/octet-stream" \
            --data-binary "@$file" -o /dev/null -w ""
    local end=$(date +%s%N)

    local elapsed=$(( (end - start) / 1000000 ))
    local rps=$(echo "scale=1; $count * 1000 / $elapsed" | bc 2>/dev/null || echo "?")
    local throughput=$(echo "scale=1; $count * $size / 1048576 * 1000 / $elapsed" | bc 2>/dev/null || echo "?")

    echo "${elapsed}ms (${rps} req/s, ${throughput} MB/s)"
    echo "\"upload_${label}\": {\"elapsed_ms\": $elapsed, \"requests\": $count, \"file_size\": $size}," >> "$REPORT_FILE.tmp"
}

bench_download() {
    local label="$1" key="$2" count="$3" concurrency="$4"

    echo -n "  $label (${count} req, ${concurrency} conc)... "

    local start=$(date +%s%N)
    seq 1 "$count" | xargs -P "$concurrency" -I{} \
        curl -sf "$BASE/v1/objects/$BUCKET/$key" \
            -H "Authorization: Bearer $KEY" -o /dev/null -w ""
    local end=$(date +%s%N)

    local elapsed=$(( (end - start) / 1000000 ))
    local rps=$(echo "scale=1; $count * 1000 / $elapsed" | bc 2>/dev/null || echo "?")

    echo "${elapsed}ms (${rps} req/s)"
    echo "\"download_${label}\": {\"elapsed_ms\": $elapsed, \"requests\": $count}," >> "$REPORT_FILE.tmp"
}

# ── Upload Benchmarks ────────────────────────────────────
echo ""
echo "[2/7] Upload benchmarks..."
echo "{" > "$REPORT_FILE.tmp"

bench_upload "1KB"   "$TMPDIR/1kb.bin"   "$REQUESTS" "$CONCURRENCY"
bench_upload "100KB" "$TMPDIR/100kb.bin"  "$REQUESTS" "$CONCURRENCY"
bench_upload "1MB"   "$TMPDIR/1mb.bin"    "$((REQUESTS/2))" "$CONCURRENCY"
bench_upload "10MB"  "$TMPDIR/10mb.bin"   "$((REQUESTS/10))" "$((CONCURRENCY/2))"

# ── Seed some objects for download tests ─────────────────
echo ""
echo "[3/7] Seeding download test data..."
curl -sf -X PUT "$BASE/v1/objects/$BUCKET/dl-1kb.bin" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$TMPDIR/1kb.bin" -o /dev/null
curl -sf -X PUT "$BASE/v1/objects/$BUCKET/dl-1mb.bin" \
    -H "Authorization: Bearer $KEY" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$TMPDIR/1mb.bin" -o /dev/null

# ── Download Benchmarks ──────────────────────────────────
echo ""
echo "[4/7] Download benchmarks..."
bench_download "1KB"  "dl-1kb.bin" "$REQUESTS" "$CONCURRENCY"
bench_download "1MB"  "dl-1mb.bin" "$REQUESTS" "$CONCURRENCY"

# ── List Benchmarks ──────────────────────────────────────
echo ""
echo "[5/7] List objects benchmark..."
echo -n "  List (${REQUESTS} req, ${CONCURRENCY} conc)... "
start=$(date +%s%N)
seq 1 "$REQUESTS" | xargs -P "$CONCURRENCY" -I{} \
    curl -sf "$BASE/v1/objects/$BUCKET?max_keys=100" \
        -H "Authorization: Bearer $KEY" -o /dev/null -w ""
end=$(date +%s%N)
elapsed=$(( (end - start) / 1000000 ))
rps=$(echo "scale=1; $REQUESTS * 1000 / $elapsed" | bc 2>/dev/null || echo "?")
echo "${elapsed}ms (${rps} req/s)"
echo "\"list\": {\"elapsed_ms\": $elapsed, \"requests\": $REQUESTS}," >> "$REPORT_FILE.tmp"

# ── Health Check Latency ─────────────────────────────────
echo ""
echo "[6/7] Health check latency (baseline)..."
echo -n "  Health (${REQUESTS} req, ${CONCURRENCY} conc)... "
start=$(date +%s%N)
seq 1 "$REQUESTS" | xargs -P "$CONCURRENCY" -I{} \
    curl -sf "$BASE/health" -o /dev/null -w ""
end=$(date +%s%N)
elapsed=$(( (end - start) / 1000000 ))
rps=$(echo "scale=1; $REQUESTS * 1000 / $elapsed" | bc 2>/dev/null || echo "?")
echo "${elapsed}ms (${rps} req/s)"
echo "\"health\": {\"elapsed_ms\": $elapsed, \"requests\": $REQUESTS}" >> "$REPORT_FILE.tmp"

# ── Finalize Report ──────────────────────────────────────
echo "}" >> "$REPORT_FILE.tmp"
# Fix trailing commas (simplified)
sed 's/},}/}}/' "$REPORT_FILE.tmp" > "$REPORT_FILE" 2>/dev/null || cp "$REPORT_FILE.tmp" "$REPORT_FILE"
rm -f "$REPORT_FILE.tmp"

echo ""
echo "[7/7] Cleanup..."
rm -rf "$TMPDIR"

echo ""
echo "=== Benchmark Complete ==="
echo "Report: $REPORT_FILE"
echo ""
echo "To delete bench bucket: curl -X DELETE '$BASE/v1/buckets/$BUCKET' -H 'Authorization: Bearer $KEY'"
