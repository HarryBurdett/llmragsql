# Health Check Contract

Every application container exposes the same health endpoints.
SAM (and docker-compose) use these for readiness probes,
load-balancer health checks, and dependency-ordering.

## Endpoints

### `GET /healthz`

**Always available**, regardless of `INSTALLED_APPS`. Returns
200 if the process is up and the FastAPI app has registered.

Response:
```json
{
  "status": "ok",
  "app_name": "bank-reconcile",
  "installed_apps": ["bank_reconcile"]
}
```

When `INSTALLED_APPS` is unset, `installed_apps` is the string
`"all"`.

### `GET /api/health` (existing)

The legacy detailed health endpoint exposes Opera SQL connectivity,
IMAP status, etc. Kept for backwards compatibility with the
existing UI dashboard.

## Probe configuration

### docker-compose

Already configured in [`Dockerfile`](../../Dockerfile):

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1
```

### Kubernetes / SAM

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /healthz
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 2
```

## What `/healthz` does NOT check

- Does NOT verify Opera SQL connectivity (that's
  `/api/health/opera`)
- Does NOT verify IMAP reachability (that's `/api/health/email`)
- Does NOT verify Gemini API key validity

This is intentional. `/healthz` answers "is this process alive
and serving HTTP?" — used to decide whether to send traffic.
External-dependency checks live behind `/api/health/*` so a
broken Opera SQL connection doesn't take the container out of
load-balancer rotation (the app might still be useful for
read-only operations).

## Startup ordering

`docker-compose.yml` uses `depends_on` with `service_healthy`
condition for ordering. Apps that consume `core-email` wait for
it to report healthy before starting:

```yaml
bank-reconcile:
  depends_on:
    core-email:
      condition: service_healthy
```

SAM's equivalent depends on the platform — most service meshes
handle dependency ordering via init containers or readiness gates.

## Failure semantics

If `/healthz` returns non-200 for `failureThreshold` consecutive
probes:
- **livenessProbe failure**: container is killed and restarted
- **readinessProbe failure**: container is removed from
  load-balancer rotation but kept running (so debuggers can attach)

Recommended: set `failureThreshold` higher on liveness than
readiness so transient hiccups don't trigger restarts.
