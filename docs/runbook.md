# Runbook

One section per alert. Every command is copy-pasteable against the namespace
the chart is installed in.

Useful context for all of them:

```sh
kubectl get pods -l app.kubernetes.io/part-of=event-pipeline -o wide
kubectl logs -l app.kubernetes.io/name=worker --tail=50
kubectl exec deploy/redis -- redis-cli xlen events
kubectl exec deploy/redis -- redis-cli xpending events aggregators
```

## EventPipelineHighErrorRate

> More than 5% of ingest requests return 5xx.

The ingest API only returns 5xx when Redis writes fail, so this is almost
never an API problem.

1. Is Redis up? `kubectl get pods -l app.kubernetes.io/name=redis` — if it is
   restarting, see [EventPipelineRedisDown](#eventpipelineredisdown).
2. Check API logs for the actual error:
   `kubectl logs -l app.kubernetes.io/name=ingest-api --tail=30 | grep enqueue`
3. If Redis is up but slow (OOM, CPU-throttled): `kubectl top pod`, check the
   redis container's resource limits against its usage.

## EventPipelineSlowIngest

> p95 latency for POST /v1/events above 250ms.

1. Check whether the HPA is already at max: `kubectl get hpa ingest-api`.
   At max + high CPU → raise `ingestApi.hpa.maxReplicas`.
2. Check Redis latency: `kubectl exec deploy/redis -- redis-cli --latency-history -i 5`
   (one-off sample; anything over a few ms inside a cluster is suspicious).
3. Look at the trace waterfall (when OTLP is wired) for where time goes:
   handler vs XADD.

## EventPipelineWorkerStalled

> The stream has entries but nothing is being processed.

1. Are worker pods running? `kubectl get pods -l app.kubernetes.io/name=worker`
2. Crash-looping → `kubectl logs -l app.kubernetes.io/name=worker --previous`
3. Running but idle → check the consumer group actually exists:
   `kubectl exec deploy/redis -- redis-cli xinfo groups events`
4. Restarting the worker is safe at any time: processing is at-least-once and
   unacked messages are reclaimed via XAUTOCLAIM.
   `kubectl rollout restart deploy/worker`

## EventPipelineBacklogHigh

> More than 50k entries in the stream — producers are outrunning consumers.

1. Scale consumers: `kubectl scale deploy/worker --replicas=3`. Safe: replicas
   share the consumer group and aggregate updates are atomic increments.
2. If growth continues, check whether one message poisons the loop (repeated
   `record failed` for the same ID in worker logs).
3. Note the stream is capped (XADD MAXLEN ~): sustained overrun eventually
   trims *unread* events — that is data loss by backpressure policy. Decide:
   scale up, or accept the trim.

## EventPipelinePendingStuck

> Messages were delivered to a consumer but never acked.

1. `kubectl exec deploy/redis -- redis-cli xpending events aggregators` — the
   consumer names are pod names; do those pods still exist?
2. Dead pod holding messages → nothing to do: a live worker reclaims them
   after 30s idle (XAUTOCLAIM). If they never drain, the *live* workers are
   failing on record/ack — read their logs.

## EventPipelineRedisDown

> Workers cannot PING Redis.

1. `kubectl get pods -l app.kubernetes.io/name=redis` and
   `kubectl describe pod -l app.kubernetes.io/name=redis` (OOMKilled? evicted?)
2. Remember the demo posture: single replica on emptyDir — a pod restart
   loses the stream and the aggregates (see postmortem 001). The system
   self-heals structurally: the API's readiness gate fails fast, the worker
   recreates the consumer group on reconnect.
3. If the node is the problem, delete the pod and let the scheduler move it:
   `kubectl delete pod -l app.kubernetes.io/name=redis`
