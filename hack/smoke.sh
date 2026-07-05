#!/usr/bin/env bash
# End-to-end smoke test: exercises the full ingest -> stream -> aggregate ->
# query path through a deployed instance. Used both locally and in CI.
set -euo pipefail

BASE="${1:-http://localhost:8080}"

echo "==> posting 10 page_view + 1 purchase to $BASE"
for i in $(seq 1 10); do
  curl -fsS -X POST "$BASE/v1/events" \
    -H 'Content-Type: application/json' \
    -d "{\"type\":\"page_view\",\"source\":\"smoke\",\"payload\":{\"n\":$i}}" > /dev/null
done
curl -fsS -X POST "$BASE/v1/events" \
  -H 'Content-Type: application/json' \
  -d '{"type":"purchase","source":"smoke"}' > /dev/null

echo "==> invalid event must be rejected with 400"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/v1/events" \
  -H 'Content-Type: application/json' -d '{"source":"smoke"}')
if [ "$code" != "400" ]; then
  echo "FAIL: expected 400, got $code"
  exit 1
fi

echo "==> waiting for the worker to aggregate"
for _ in $(seq 1 30); do
  total=$(curl -fsS "$BASE/v1/stats" | jq -r '.total // 0')
  if [ "$total" -ge 11 ]; then
    echo "==> stats:"
    curl -fsS "$BASE/v1/stats" | jq .
    pv=$(curl -fsS "$BASE/v1/stats" | jq -r '.byType.page_view // 0')
    if [ "$pv" -lt 10 ]; then
      echo "FAIL: byType.page_view=$pv, want >=10"
      exit 1
    fi
    echo "SMOKE OK (total=$total)"
    exit 0
  fi
  sleep 2
done

echo "FAIL: timed out waiting for aggregation (last total=$total)"
exit 1
