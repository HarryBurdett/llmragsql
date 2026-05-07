# Deployment Shapes

The same images support multiple deployment topologies. Pick the
shape that matches your customer.

## Shape 1 — Monolith (current production)

One container with all apps registered. Same as today's
`run_dev.sh`, just packaged.

```
┌────────────────────────┐
│   sqlrag/monolith      │
│  INSTALLED_APPS=all    │
│   port 8000            │
└────────────────────────┘
```

**Build:**
```bash
docker build -t sqlrag/monolith --target monolith .
```

**Run:**
```bash
docker run -p 8000:8000 --env-file .env \
    -v sqlrag-data:/app/data \
    sqlrag/monolith
```

**Use when:**
- Single-tenant on-prem deployment
- Customer wants the simplest possible install
- Resource-constrained environment

**Trade-off:** Apps share a process. Updating one app means
restarting all. Debugging one app's crash takes the rest down.

## Shape 2 — Per-app containers (Phase B + onwards)

Each app in its own container. The pre-SAM target architecture.

```
┌─────────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐
│bank-reconci-│ │gocardless│ │suppliers │ │balance-check│
│   le        │ │          │ │          │ │             │
└─────────────┘ └──────────┘ └──────────┘ └─────────────┘
        │             │             │             │
        └─────────────┴──────┬──────┴─────────────┘
                             │
                      ┌──────────┐
                      │core-email│
                      └──────────┘
                             │
                      ┌──────────┐
                      │  nginx   │
                      └──────────┘
                             │
                      ┌──────────┐
                      │ frontend │
                      └──────────┘
```

**Run:**
```bash
docker compose up
```

**Use when:**
- Multi-tenant SaaS (one deployment per tenant)
- Customer wants to license individual apps
- Production deployments where update isolation matters
- SAM-platform-bound deployments

**Trade-off:** More containers to operate. Phase B work needed
to fully separate (each app's own database etc.).

## Shape 3 — Single-app container (per-customer SKU)

Customer licenses only one app — ship them just that container.

```
┌──────────────────────────┐
│ sqlrag/bank-reconcile    │
│ INSTALLED_APPS=          │
│   bank_reconcile         │
└──────────────────────────┘
```

**Build:**
```bash
docker build --target bank-reconcile -t sqlrag/bank-reconcile .
```

**Run:**
```bash
docker run -p 8000:8000 --env-file .env \
    -v sqlrag-bank-data:/app/data \
    sqlrag/bank-reconcile
```

**Use when:**
- Customer bought "Bank Statement Reconciliation only"
- You want to limit the route surface for compliance reasons
- Cost-sensitive deployment

**Trade-off:** Customer can't add another app later without a
new deployment. Workaround: ship the monolith with feature flags
(this is the current default).

## Shape 4 — SAM-hosted (post-migration)

Apps run inside SAM. Topology dictated by SAM, but conceptually
the same as Shape 2 with SAM providing:

- Service discovery (replaces docker-compose's network)
- Ingress (replaces nginx-gateway)
- Secrets (replaces `.env` file)
- Auth (potentially replaces `core-auth`)
- Observability (logs, metrics, traces)

**Build:**
```bash
docker build --target bank-reconcile -t sqlrag/bank-reconcile:v1.2.3 .
docker build --target gocardless     -t sqlrag/gocardless:v1.2.3 .
# ... per app
docker push sqlrag/<app>:v1.2.3
```

**Deploy:** via SAM's deployment manifest (Helm chart, Kustomize,
or whatever SAM ingests).

**Use when:**
- Customer is on the SAM platform (final production state)

## Choosing a shape

| Customer scenario | Shape |
|---|---|
| Single Opera customer, single Windows server, low traffic | 1 |
| You hosting SaaS for multiple customers | 2 |
| Customer who only bought GoCardless | 3 |
| SAM platform tenant | 4 |
| Demo / sales | 1 (single command, fast spin-up) |

## Cross-shape compatibility

All four shapes use the same Python source code and the same
configuration contract (env vars). Switching between shapes only
requires:

- Different Dockerfile target (or none, for monolith)
- Different `INSTALLED_APPS` env var
- Different network / volume layout

You can demo the customer their target shape (Shape 4) with a
Shape 2 docker-compose deployment because they're functionally
identical.
