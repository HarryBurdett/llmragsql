# Quickstart — Run the Apps in Docker

**5-minute setup** to get all the applications running in containers
on your laptop. Pre-SAM Docker stack — same shape SAM will host
later, just with docker-compose providing env vars instead of SAM.

## Prerequisites

- Docker Desktop (macOS / Windows) or Docker Engine + Compose plugin (Linux)
- Access to an Opera SQL Server instance (or use SQLite-in-memory
  for a smoke test — see below)
- IMAP credentials for an inbox containing bank statements / GoCardless
  payouts / supplier statements
- A Gemini API key

## Step 1 — Configure

```bash
cp .env.example .env
```

Edit `.env` with your values. The minimum to get the stack running:

```bash
DATABASE_SERVER=10.10.100.50
DATABASE_DATABASE=opera_company_01
DATABASE_USERNAME=sa
DATABASE_PASSWORD=...

EMAIL_IMAP_SERVER=imap.example.com
EMAIL_IMAP_USERNAME=...
EMAIL_IMAP_PASSWORD=...

GEMINI_API_KEY=...
```

For Opera 3 deployments add:
```bash
OPERA_VERSION=3
OPERA3_DATA_PATH=//windows-server/opera3-share/COMPANY01
OPERA3_WRITE_AGENT_URL=http://windows-server:9000
```

For GoCardless add (sandbox token in dev — NEVER use a live token
for testing):
```bash
GOCARDLESS_ACCESS_TOKEN=sandbox_...
GOCARDLESS_ENVIRONMENT=sandbox
```

## Step 2 — Build and run

```bash
docker compose up --build
```

This builds the Python image, pulls nginx + node images, and starts
all 5 app containers + gateway + frontend.

First build takes ~5 minutes (installs ODBC drivers + Python deps).
Subsequent rebuilds are seconds.

## Step 3 — Open the UI

- **Frontend:** http://localhost:5173
- **API gateway:** http://localhost:8080 (this is what the
  frontend talks to in production)
- **Direct app debug ports:**
  - http://localhost:8001 — bank-reconcile
  - http://localhost:8002 — gocardless
  - http://localhost:8003 — suppliers
  - http://localhost:8004 — balance-check
  - http://localhost:8005 — core-email

Each app exposes `/healthz` for sanity checks:
```bash
curl http://localhost:8001/healthz
curl http://localhost:8002/healthz
curl http://localhost:8080/healthz   # gateway aggregator
```

## Step 4 — Smoke test

```bash
# Frontend reachable
curl http://localhost:5173

# Gateway routing works (should return JSON, not 404)
curl http://localhost:8080/api/health

# Each app is up
for port in 8001 8002 8003 8004 8005; do
    echo -n "Port $port: "
    curl -s http://localhost:$port/healthz | jq .app_name
done
```

Expected output:
```
Port 8001: "bank-reconcile"
Port 8002: "gocardless"
Port 8003: "suppliers"
Port 8004: "balance-check"
Port 8005: "core-email"
```

## Step 5 — Stop / restart / logs

```bash
docker compose stop                # Stop everything
docker compose down                # Stop + remove containers (volumes preserved)
docker compose down -v             # Stop + remove containers AND volumes (data loss!)
docker compose logs -f             # Tail all logs
docker compose logs -f bank-reconcile   # Tail one app
docker compose restart bank-reconcile   # Restart one app
docker compose ps                  # See running containers
```

## Common issues

**"Cannot connect to Opera SQL"**
Check the `DATABASE_SERVER` is reachable from inside the container:
```bash
docker compose exec bank-reconcile bash -c "nc -zv $DATABASE_SERVER $DATABASE_PORT"
```
On macOS, `host.docker.internal` works for "the Mac itself"; from
the container, Opera SQL on the same Mac is reachable as
`host.docker.internal`.

**"ODBC Driver 18 for SQL Server" not loading**
The Dockerfile installs `msodbcsql18`. If the Opera server requires
TLS 1.0 (legacy SQL Server), set `DATABASE_TRUST_SERVER_CERTIFICATE=true`.

**Volumes are confusing**
Each app has its own named volume for SQLite state:
```bash
docker volume ls | grep sqlrag
# llmragsql_bank-reconcile-data
# llmragsql_gocardless-data
# llmragsql_suppliers-data
# ...
docker volume inspect llmragsql_bank-reconcile-data
```

**Hot reload during dev**
The frontend image runs Vite in dev mode with HMR. The Python apps
use uvicorn without `--reload` because reload doesn't play well
with the multi-stage build. For Python iteration, mount the source:
```yaml
# Add to docker-compose.yml under any app's `volumes:`
- ./apps:/app/apps
- ./api:/app/api
- ./sql_rag:/app/sql_rag
```
Then `docker compose restart bank-reconcile` to pick up changes.

## Next steps

- Read [`deployment-shapes.md`](./deployment-shapes.md) for the
  four supported deployment topologies
- Read [`env-var-contract.md`](./env-var-contract.md) for the full
  env-var reference
- Read [`migration-checklist.md`](./migration-checklist.md) when
  you're ready to migrate to SAM
