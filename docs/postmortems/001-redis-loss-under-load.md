# Postmortem 001 — Redis killed under load

**Date:** 2026-07-05 · **Type:** planned chaos experiment · **Status:** resolved
**Environment:** local kind cluster, chart defaults, k6 load (25 VUs, ~190 req/s)

## TL;DR

Killing the Redis pod under load surfaced exactly what the demo posture
promises (fast fail, ~8s self-healing, total state loss on emptyDir) — **and
one genuine bug the tests had missed**: the worker never recreates its
consumer group after Redis comes back empty, so the pipeline kept accepting
events while silently aggregating nothing. Fixed in the same commit, with a
regression test.

## Timeline (local)

| Time | Event |
|---|---|
| 23:55:21 | k6 starts: ramp to 25 VUs (~190 req/s sustained) |
| 23:55:51 | `kubectl delete pod -l app.kubernetes.io/name=redis`. 5,404 events aggregated at this point |
| 23:55:51–23:55:58 | Ingest returns **503** (`event store unavailable`) — fail-fast, no hangs, no timeouts |
| 23:55:59 | Replacement Redis pod ready; ingest returns **202** again. Recovery: **~8 seconds**, zero human intervention |
| 23:55:59 → end | `GET /v1/stats` reports `total=0` — and **stays at 0**: events are accepted but never aggregated |
| 23:56:31 | k6 done: 13,250 requests, 7.93% failed (all inside the 8s window), p95 for successful requests 1.68ms |

## Impact

- **Availability:** 7.93% of requests failed during the ~8s outage window; all
  failures were clean 503s (clients can retry), not hangs.
- **Data:** all pre-incident state lost — 5,404 aggregated events plus the
  queued stream. Expected: single Redis on `emptyDir` is the documented demo
  trade-off.
- **The real finding:** post-recovery, ingestion resumed (202) but
  **aggregation silently stopped**. `/v1/stats` stayed at 0 while thousands of
  new events queued.

## Root cause of the silent stall

The worker creates its consumer group once, at startup
(`XGROUP CREATE ... MKSTREAM`). When Redis restarted empty, the group no
longer existed, and every subsequent read failed with:

```
NOGROUP No such key 'events' or consumer group 'aggregators' in XREADGROUP
```

The read loop treated this like any transient error — log, sleep 1s, retry —
which can never succeed: nothing recreates the group until the worker pod
itself restarts. Liveness probes don't help; the process is healthy, it's the
assumption that died.

## What worked

- **Readiness vs liveness split:** API pods failed readiness (Redis ping) and
  returned 503 immediately instead of hanging; they were correctly *not*
  restarted for a dependency problem they couldn't fix.
- **Kubernetes self-healing:** the Deployment replaced the Redis pod in ~8s.
- **The alert rules would have caught it:** `EventPipelineWorkerStalled`
  fires exactly on this signature (`worker_stream_length > 0` while
  `rate(worker_events_processed_total) == 0`).

## What didn't

- **NOGROUP was an unhandled failure mode** — a state-loss scenario unit
  tests with a well-behaved Redis never exercise.

## Action items

| # | Action | Status |
|---|---|---|
| 1 | Worker recreates the consumer group on NOGROUP and resumes | ✅ fixed (`internal/worker`) |
| 2 | Regression test simulating Redis state loss (`FlushAll` mid-run) | ✅ `TestWorkerRecreatesGroupAfterRedisStateLoss` |
| 3 | Alert on the signature (`EventPipelineWorkerStalled`) | ✅ shipped in `rules/alerts.yaml` |
| 4 | Durable Redis (AOF/managed) for anything beyond metrics-grade data | documented trade-off, README |

## Lesson

The interesting failure wasn't the crash — Kubernetes handled that in eight
seconds. It was the *assumption that survived the crash*: "the consumer group
I created at startup still exists." Chaos testing exists to kill exactly that
kind of assumption while it's cheap.
