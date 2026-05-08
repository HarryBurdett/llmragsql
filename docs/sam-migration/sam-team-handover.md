# SAM Team Handover — Merging the SQL RAG Applications

**Audience:** SAM platform engineering team
**Purpose:** Step-by-step instructions for merging the SQL RAG application
suite (Bank Reconciliation, GoCardless, Suppliers, Balance Check) into the
SAM platform.
**Pre-requisite:** SAM platform is operational with auth, tenants, secrets,
and deployment infrastructure available.

This document is self-contained for the merge work. It links to deeper
references where helpful but you should not need to read every linked
document to do the work.

---

## TL;DR

We have four containerised business applications + a frontend that
share a common architecture (env-var-driven config, ports/adapters
behind every external dependency). Phase A and Phase B of our work
were specifically designed for SAM-merge: every config value, every
external service, every shared resource is already abstracted behind
a swap point.

**Confirmed scope (revised):** SAM provides email (inbox / attachment
storage / send), auth, secrets, and the Opera 3 Agent. Our previous
`core-email` and `core-opera3` services are therefore **not** part of
the merge bundle — SAM replaces them. Our four workflow apps consume
SAM's services through Phase B's adapter ports.

**Deployment context:** SAM and our apps run on the same platform
technology, in different locations. Same image format, same
manifest conventions, same observability stack — apps reach SAM via
URL across locations.

### Step-by-step at a glance

| # | Step | Owner | Effort |
|---|---|---|---|
| 1 | Confirm SAM conventions (auth, secrets, email API, etc.) — answer the questions in §3 | SAM team | Half a day |
| 2 | Write SAM adapter files (~6 small files: auth, email, secrets, opera-sql credentials) | Us | 3-5 days |
| 3 | Build container images per app and push to your registry | Us | 1 day |
| 4 | Create deployment manifests (5 deployments: 4 apps + frontend) | SAM team | 1-2 days |
| 5 | Provision per-tenant persistent volumes for each app's data (see Phase 3a) | SAM team | Per-tenant |
| 6 | Populate per-tenant secrets per the env-var contract (see Phase 4) | SAM team | Per-tenant |
| 7 | Deploy apps, run `/healthz` + per-app data-integrity health check | Joint | 1 hour per customer |
| 8 | Cut traffic from old to new deployment | SAM team | Per-tenant |

**Total: ~1 week our side; deployment + per-tenant work on yours.**

**No application code changes are required for the merge** — only the
adapter layer and your deployment manifests.

### Considerations split: who handles what

**SAM-side considerations:**
- Email service (inbox poller, attachment storage, send) — SAM provides; replaces our `core-email`
- Opera 3 Agent — SAM hosts; our apps consume via `OPERA3_AGENT_URL`
- Auth (JWT issuance + validation) — SAM owns; our apps validate inbound JWTs against `AUTH_JWT_PUBLIC_KEY`
- Secrets store — SAM owns; populated per tenant
- Persistent volumes per tenant per app for SQLite operational data
- Ingress / routing — SAM's gateway replaces our nginx
- Logging / metrics conventions — SAM specifies; we adapt

**Our-side considerations:**
- App business logic (bank rec, GoCardless, suppliers, balance check) — unchanged, no code edits for the merge
- Adapter implementations (one per SAM service we consume — auth, email, secrets)
- Image build + push pipeline
- Health-check + smoke-test scripts
- Per-app data integrity checks (validates SQLite state against the tenant's Opera)
- Frontend bundle build + deploy

**Joint considerations (need both sides to agree):**
- Cutover runbook per customer (parallel run, switch ingress, monitor, retire old)
- Data migration of existing operational SQLite (rsync into SAM volume)
- Tenant onboarding workflow (who provisions what, in what order)
- Rollback path (SAM-side ingress flip back to legacy deployment)

Estimated effort once SAM specifics are known: **~1 week of focused
work on the application side**, deployment + per-tenant configuration
on yours.

---

## §1 What's being merged

### Application catalogue (4 apps + 1 frontend = 5 SAM deployments)

| App | Purpose | Owns | Reads from |
|---|---|---|---|
| `bank-reconcile` | Bank statement scan + reconcile + Opera posting | `bank_aliases.db`, `bank_patterns.db`, statement-tracking SQLite | Opera SQL, **SAM email service**, Gemini |
| `gocardless` | Direct Debit payout import | `gocardless_payments.db` | Opera SQL, **SAM email service**, GoCardless API, Gemini |
| `suppliers` | Supplier statement reconciliation | `supplier_extraction_cache.db`, `supplier_statements.db` | Opera SQL, **SAM email service**, Gemini |
| `balance-check` | Internal Opera balance reconciliation | (read-only, no own state) | Opera SQL |
| `frontend` | Single React SPA serving all four apps behind your ingress | (stateless, static bundle) | Gateway URL only |

### Not in the merge bundle

| ~~Service~~ | Reason |
|---|---|
| ~~`core-email`~~ | **Replaced by SAM.** SAM provides inbox / attachment storage / send. Our apps consume it via `SAM_EMAIL_URL`. |
| ~~`core-opera3`~~ | **Replaced by SAM.** SAM hosts the expanded Opera 3 Agent (handles both reads and writes). |
| ~~`core-opera-se`~~ | Optional shared SQL gateway; not required for initial merge. Apps talk to Opera SQL directly today. |
| ~~`core-auth`~~ | If SAM provides JWT-based auth, we drop our internal auth too; otherwise we keep a minimal auth surface. See §3 Q3. |
| ~~`nginx-gateway`~~ | Replaced by SAM's ingress / service mesh. |

### External dependencies

These exist outside SAM's control and your apps still consume them:

| External | What | Stays as-is? |
|---|---|---|
| Opera SQL Server | Customer's accounting database (Windows host) | Yes — connection details supplied per tenant |
| Opera 3 Agent | **SAM-hosted** service handling all Opera 3 reads + writes (expanded from the original write-only Windows agent). Reads FoxPro DBFs and posts transactions on behalf of our containers. | **Hosted by SAM** — no longer customer-deployed |
| Email (inbox + attachments + send) | **SAM email service** — SAM owns the connection to the customer's mailbox (MS Graph / IMAP) and the send pipeline. Our apps no longer hold mailbox credentials. | **Hosted by SAM** |
| Gemini API | Google AI for PDF extraction | Yes — API key per deployment or per tenant |
| GoCardless API | Direct Debit platform | Yes — token per tenant |

**Architecture update:** The Opera 3 Agent has been **expanded by SAM
to handle both reads and writes**, replacing two earlier integrations:
the direct DBF file-share access (for reads) and the customer-deployed
Windows Write Agent (for writes). Our containers no longer need an SMB
mount or direct file access — every Opera 3 operation goes over HTTP to
SAM's Opera 3 Agent.

Implications:
- ✅ No SMB / CIFS configuration needed in our containers
- ✅ No `OPERA3_DATA_PATH` env var (deprecated)
- ✅ Single integration point for all Opera 3 access
- ✅ SAM's agent handles authentication, locking, multi-tenant isolation
- ✅ Our `Opera3WriterPort` and `Opera3ReaderPort` adapters both
  become HTTP clients pointing at the same agent URL

### See also

- [`README.md`](./README.md) — application catalogue overview
- [`dependency-graph.md`](./dependency-graph.md) — full dependency map
- [`apps/`](./apps/) — per-app deep-dive (bank-reconcile, gocardless, suppliers)

---

## §2 What we provide as part of the handover

The following are ready and pushed to the repository. Use them as inputs
to your merge.

### Code artifacts

- **Container images** (multi-stage `Dockerfile` at repo root)
  - `--target bank-reconcile` — single-app image
  - `--target gocardless` — single-app image
  - `--target suppliers` — single-app image
  - `--target balance-check` — single-app image
  - `--target core-email` — shared IMAP poller image
  - `--target monolith` (default) — all apps in one image (current production)

- **Image metadata**
  - Listens on port `8000` for HTTP
  - Exposes `/healthz` for liveness/readiness
  - Runs as non-root user `sqlrag` (uid 1001)
  - Reads config from environment variables only (no baked-in config)

- **Frontend image** (`frontend/Dockerfile`)
  - `--target prod` — nginx serving the React bundle
  - Listens on port `80`
  - Uses `VITE_API_BASE_URL` build-time env to point at the API gateway

### Documentation

- [`env-var-contract.md`](./env-var-contract.md) — every environment variable each app consumes, organised by purpose, with per-app required/conditional table. **Most important reference.**
- [`sam-integration-pattern.md`](./sam-integration-pattern.md) — architectural pattern + code skeletons for the SAM adapter
- [`dependency-graph.md`](./dependency-graph.md) — app-to-app + external dependency map, gateway routing summary
- [`deployment-shapes.md`](./deployment-shapes.md) — four supported topologies (monolith, per-app, single-app, SAM-hosted)
- [`health-checks.md`](./health-checks.md) — `/healthz` contract + Kubernetes-shaped probe spec
- [`migration-checklist.md`](./migration-checklist.md) — per-app post-merge checklist

### Built-in tooling for SAM merge

- **`/api/system/connection-info`** endpoint on every app — returns the centralised parameters that app is wired up to (sanitised: no secrets), used by the System Connection panel each operator can see
- **`/api/{app}/health-check`** endpoints — verify the app's local data references valid Opera codes (run after deployment to confirm tenant config is good)
- **System Connection panel** in each app's Settings — read-only display of what backends are configured

---

## §3 What SAM must provide / decide

These are the conventions we need from you before writing the SAM adapter.
None require us to make changes to apps; they configure the adapter.

### Confirmed deployment context (✅ already known)

**SAM and our apps run on the same platform technology, in different
locations.**

What this means:
- ✅ Same image format and deployment manifest conventions
- ✅ Same observability stack (logs / metrics / traces use one toolchain)
- ✅ Same container registry conventions
- ✅ Same operational tooling (rolling upgrades, secret store, etc.)
- ⚠ Different network locations → SAM is reachable by URL only (no
  local-host or sidecar shortcut); explicit DNS / service-discovery
  setup needed
- ⚠ Inter-location latency is real (~10-100ms typical) → SAM-config
  responses must be cached aggressively in our adapter (TTL or
  webhook-based invalidation)

**Implications for the SAM adapter:**
- The `SAMConfigClient` already designed has aggressive caching built
  in. We bias toward longer TTL (default 5 min) given cross-location
  latency.
- Service-to-service auth between our apps and SAM may need an explicit
  bootstrap (service token / mTLS cert) since cross-location workload
  identity is platform-dependent. Confirm with SAM team how this works
  on your shared platform.

### Critical (block adapter work)

1. **Where SAM hosts our containers** ✅ same platform as SAM, different
   location. Confirm specifically:
   - Same Kubernetes cluster (different namespace), or different cluster?
   - Service-mesh available across locations, or HTTP-over-internal-DNS?
   - Latency budget between our apps and SAM (informs caching strategy)

2. **How SAM injects secrets into our containers**
   - Environment variables (most common; what we expect today)
   - Mounted files (Kubernetes Secrets style)
   - Runtime config API the app fetches at startup

   **Note:** Whatever you pick, our apps already read from `os.environ`. If
   you mount files or use a config API, an init step at container start
   reads them into env vars. One adapter handles this.

3. **Authentication token format**
   - JWT? OAuth2? Custom session cookie?
   - For JWT: what's the public key distribution mechanism, what claims
     does the token carry (`tenant_id`, `roles`, `system_type`, others)?

4. **Tenant identification per request**
   - Tenant ID in the JWT? In a header (`X-Tenant-ID`)? In the URL path?

5. **Service-to-service auth**
   - Our apps will call SAM's secret/config API. How do they authenticate?
   - mTLS? Service token in env? Workload identity?

6. **SAM email service API contract** — *new, replaces core-email*
   - **Endpoint shape:** REST? GraphQL? SAM SDK? Confirm base URL pattern
     (we propose `SAM_EMAIL_URL=https://sam.example.com/email/{tenant}/`).
   - **Capabilities required by our apps:**
     - List emails by mailbox / since-date / search filter
     - Fetch a single message body
     - Download an attachment by message-id + filename (returns bytes)
     - Send an email (SMTP-equivalent — used by gocardless + suppliers
       remittance, suppliers contact email)
     - Mark as read / move to folder (suppliers archives processed
       statements)
   - **Per-app mailbox routing — who decides?**
     - Option A: each app passes a `mailbox` query parameter (e.g.
       `?mailbox=banking@customer.com`) — SAM uses it to route
     - Option B: SAM maps "this service token = this mailbox" centrally;
       our apps just call `/email/list` and get the right inbox
     - This determines whether `EMAIL_MAILBOX` stays as our env var or
       gets dropped entirely.
   - **Attachment delivery:** inline base64 in the email response, or
     a separate attachment-fetch endpoint with byte-stream response?
   - **Attachment caching:** does SAM cache attachments, or do we still
     need our app-private cache? (We currently dedupe-by-hash to save
     re-extraction cost.)

### Operational (refines, not blocking)

6. **Logging conventions**
   - Plain text or structured JSON?
   - Required fields (`tenant_id`, `app_name`, `request_id`, etc.)?
   - Log level/severity field name?

7. **Metrics / observability**
   - Prometheus, Datadog, or something else?
   - Required labels (tenant, app, environment)?
   - Distributed tracing format (OpenTelemetry?)

8. **Container registry**
   - Where do we push images? (Docker Hub, ECR, GCR, private registry)
   - Naming convention, version tag policy

9. **Health/readiness probe expectations**
   - We expose `/healthz`. Does SAM use this path?
   - Probe interval, timeout, failure threshold?

10. **Network egress policy**
    - Can our apps still reach the customer's Opera SQL, IMAP, Gemini API
      directly, or does egress go through SAM's gateway?
    - If through gateway: what's the proxy mechanism?

### Tenant lifecycle

11. **Onboarding new customers**
    - Admin UI in SAM where their Opera SQL host/user/password is entered?
    - API/config-as-code declaration?

12. **Secret rotation**
    - Push-notification webhook to invalidate cached values in our apps?
    - TTL-based re-fetch (e.g. cache for 5 minutes)?
    - Expected rotation frequency?

---

## §4 Step-by-step merge procedure

### Phase 0 — Pre-merge alignment (1 day, jointly)

**SAM team:**
- Answer questions 1-5 above (the critical block)
- Answer questions 6-10 if available

**Us:**
- Confirm we have a Docker registry account on your registry
- Confirm we can push test images
- Sanity-check the [env-var-contract](./env-var-contract.md) against your
  capabilities

Output: a one-page agreed-upon "wire format" for auth, secrets, tenant ID.

### Phase 1 — SAM adapter implementation (3-5 days, us)

For each port in our system, we write a SAM-specific adapter. The
[sam-integration-pattern.md](./sam-integration-pattern.md) document has
copy-paste skeletons for these. Files involved:

- `apps/core/adapters/sam/config_client.py` — HTTP client to SAM's
  secrets/config service (cached, TTL-aware)
- `apps/core/adapters/sam/auth.py` — token validation
- `apps/core/adapters/sam/company_context.py` — read tenant from token
- `apps/core/adapters/sam/opera_sql.py` — per-tenant Opera SQL
  connection lookup
- `apps/core/adapters/sam/email_storage.py` — only if SAM provides
  email service; otherwise our existing core-email is used unchanged
- `apps/core/adapters/sam/opera3_reader.py` — per-tenant Opera 3 path
  (only relevant for Opera 3 customers)

Plus one middleware update (~15 lines) in `api/main.py` to validate
SAM-issued tokens when `SAM_ENABLED=true`.

The factory in `apps/core/adapters/factory.py` already has SAM-branch
placeholders — we wire them up.

### Phase 2 — Build + push images (1 day, us)

- Tag and push each app image to your registry
- Verify the push works from CI/CD
- Document the image SHA per app for SAM deployment manifests

### Phase 3 — Deployment manifest (1-2 days, SAM team)

You create the deployment configuration in your platform's format:

- **5 SAM deployments total: 4 app containers + 1 frontend.**
  bank-reconcile, gocardless, suppliers, balance-check + frontend SPA.
  No `core-email` (SAM provides it). No `core-opera3` (SAM hosts the
  agent). No `nginx-gateway` (your ingress replaces it).
- Health probes pointing at `/healthz`
- Resource requests/limits (we'll suggest baselines)
- Environment variables populated from your secrets store
- One persistent volume per app per tenant (see Storage strategy below)

Required env vars per app: see [env-var-contract.md](./env-var-contract.md).
Common to all:
```
DATABASE_*           Opera SQL connection (per tenant)
OPERA_VERSION        SE | 3 (per tenant — comes from SAM token claim
                     or SAM-provided central config)
COMPANY_DATA_BASE_PATH  /app/data (mounted volume root)
SYSTEM_LOG_LEVEL     INFO
SAM_ENABLED          true
SAM_EMAIL_URL        your email service base URL
SAM_AUTH_TOKEN       short-lived service token issued by SAM
SAM_*_URL            other service URLs (registry, secrets, etc.)
AUTH_JWT_PUBLIC_KEY  for inbound token validation
```

### Phase 3a — Storage strategy (decision recorded for the merge)

**Recommendation: keep SQLite per-tenant per-app, mounted on
SAM-managed persistent volumes. Do NOT move to a central Postgres for
the initial merge.**

**What lives on disk per tenant:**
```
/app/data/{tenant}/
├── bank_reconcile/   # aliases, pattern-learning, PDF cache, locks
├── gocardless/       # mandate registry, payment-request log
├── suppliers/        # statement extraction history, automation rules
└── core/             # (only if any auth state remains; otherwise dropped)
```

Sizes: typically <100 MB per tenant, dominated by extraction caches.
50 tenants ≈ 5 GB total — well within a single volume mount.

**Why this is the right choice for the merge:**

| Concern | Why SQLite-on-SAM-volume is right |
|---|---|
| **Tenant isolation** | File-level isolation by directory layout. Impossible to accidentally read another tenant's data. With central Postgres every query needs `WHERE tenant_id = ?` — one missed filter leaks finance data. |
| **Backup / DR** | SAM volume snapshot ≡ database snapshot. Same recovery story, no extra tier. |
| **Tenant offboarding** | `rm -rf /app/data/{tenant}/` — single atomic operation. With central DB it's a delete script that has to be foolproof. |
| **Schema evolution** | Each app evolves its schema independently. No central DBA approval path. |
| **Performance** | In-process SQLite (microseconds) vs network Postgres (milliseconds with auth overhead). Reads are tiny but frequent. |
| **Failure blast radius** | One tenant's IO doesn't affect others. Central DB slow = all tenants affected. |
| **Migration cost** | Today's code is SQLite-flavoured. Moving to Postgres is months of work for benefits we don't currently need. |

**When to revisit (triggers for moving to central Postgres later):**

1. Regulatory requirement for centralised, queryable, sealed
   audit-time records across tenants
2. Per-tenant data outgrows SQLite (tens of millions of rows in a
   single app — not close to this)
3. A concrete cross-tenant product feature is built (e.g. cross-customer
   benchmarking). For analytics, a downstream warehouse is the right
   home anyway, not the operational store.

**SAM-side action:** provision per-tenant persistent volumes with
backup/snapshot policy applied. Mount each at `/app/data/{tenant}/`
inside the relevant app container.

**Our-side action:** none — already file-based, already isolated by
directory.

### Phase 4 — Per-tenant configuration (variable, SAM team per customer)

**SAM owns the email credentials, the Opera 3 Agent, and auth.** Our
apps never see mailbox passwords or Graph secrets — they call SAM's
services and SAM brokers the connection to the customer's mailbox.

For each customer being migrated, SAM admin populates their secret
slot with:

**Opera SQL (always required)**
- `DATABASE_SERVER`, `DATABASE_PORT`, `DATABASE_DATABASE`,
  `DATABASE_USERNAME`, `DATABASE_PASSWORD` — Opera SQL credentials.
  Per tenant.

**Opera version routing (always required)**
- `OPERA_VERSION` — `SE` or `3`. Determines which code path runs.
- `OPERA3_AGENT_URL` — only when `OPERA_VERSION=3`. URL of SAM's Opera
  3 Agent for this tenant; agent handles both reads and writes.

**Email — SAM-provided (always required)**
- `SAM_EMAIL_URL` — base URL of SAM's email service, scoped to this
  tenant (e.g. `https://sam.example.com/email/{tenant}/`).
- `SAM_AUTH_TOKEN` — service token our apps use to authenticate to
  SAM's email service (and other SAM services).

**Per-app mailbox identity (set per app, not per customer)**
- `EMAIL_MAILBOX` — the inbox this specific app reads from / sends
  as. Examples:
  - Single shared mailbox: every app gets `accounts@customer.com`
  - Per-workflow mailboxes:
    - bank-reconcile → `banking@customer.com`
    - gocardless → `payments@customer.com`
    - suppliers → `ap@customer.com`

  This is the only value that differs across apps for the same
  customer. SAM uses it to decide which mailbox to expose when the
  app calls `SAM_EMAIL_URL`. *(See §3 Q6 — exact mechanism depends
  on SAM's email API contract; either passed as a parameter or
  encoded in `SAM_AUTH_TOKEN`'s scope.)*

**AI extraction**
- `GEMINI_API_KEY` — Google Gemini key. Per deployment or per
  tenant; either works.

**GoCardless (only for customers using DD)**
- `GOCARDLESS_ACCESS_TOKEN` — per tenant
- `GOCARDLESS_WEBHOOK_SECRET` — per tenant

⚠️ **`GOCARDLESS_ENVIRONMENT`** must be set per-deployment, not
per-tenant (sandbox in dev, live in prod). Never per-tenant — it
would risk live API calls during testing.

**No longer needed (compared to the previous handover):**
- ~~`EMAIL_PROVIDER`~~, ~~`EMAIL_MICROSOFT_*`~~, ~~`EMAIL_IMAP_*`~~,
  ~~`EMAIL_SMTP_*`~~, ~~`EMAIL_FROM_ADDRESS`~~ — SAM owns all of
  these. Our apps never see mailbox credentials.

### Phase 5 — Smoke test per app (1 hour per customer, us + SAM team)

For each customer migrated:

1. **Container reachable**
   ```bash
   curl https://<sam-ingress>/<tenant>/<app>/healthz
   ```
   Expect 200 with `{"status": "ok", "app_name": "<app>"}`.

2. **Auth works**
   ```bash
   # With a valid SAM-issued JWT for this tenant
   curl -H "Authorization: Bearer $JWT" \
     https://<sam-ingress>/<tenant>/<app>/api/health
   ```
   Expect 200.

3. **Connection info correct**
   ```bash
   curl -H "Authorization: Bearer $JWT" \
     https://<sam-ingress>/<tenant>/<app>/api/system/connection-info
   ```
   Expect to see this tenant's Opera SQL host, IMAP creds (sanitised),
   etc. — confirms SAM injected the right secrets.

4. **Health check per app** (the killer test)
   ```bash
   curl -H "Authorization: Bearer $JWT" \
     https://<sam-ingress>/<tenant>/<app>/api/<app>/health-check
   ```
   Expect `healthy: true`. If false, the app's local data references
   Opera codes that don't exist in this tenant's Opera — usually means
   the wrong Opera credentials were configured.

5. **One end-to-end workflow**
   - bank-reconcile: scan inbox → preview → no posting needed for smoke test
   - gocardless: list payouts via API → confirm token works
   - suppliers: list suppliers from Opera
   - balance-check: load creditors page

### Phase 6 — Cutover (1 day per customer)

For each customer:

1. Confirm Phase 5 smoke tests all pass
2. Pause the customer's standalone deployment (their on-prem app)
3. Switch DNS / SAM ingress to route this customer to SAM-hosted apps
4. Monitor for 24 hours
5. Decommission standalone deployment

**Rollback:** revert the DNS/ingress switch. Their old standalone
deployment kept its data volumes during cutover, so no data loss.

### Phase 7 — Post-cutover hardening (ongoing)

- Watch logs for any unexpected errors
- Confirm log/metrics ingestion working
- Document any per-tenant quirks for support team
- Run Health Check periodically (monthly) on each tenant for
  data-integrity audit

---

## §5 Verification checklist (per app, per customer)

Use this checklist for each tenant-app pair after deployment.

- [ ] `GET /healthz` returns 200
- [ ] `GET /api/health` returns 200 with valid JWT
- [ ] `GET /api/system/connection-info` shows correct tenant's Opera/email
- [ ] `GET /api/<app>/health-check` returns `healthy: true`
- [ ] Login via SAM works, JWT carries `tenant_id` + `system_type`
- [ ] `bool(opera_sql_adapter)` is true (Opera reachable)
- [ ] `bool(email_storage_adapter)` is true (email reachable, only for
       apps that need email: bank-reconcile, gocardless, suppliers)
- [ ] One workflow per app completes end-to-end
- [ ] Logs flowing to SAM observability stack
- [ ] Metrics visible in SAM dashboard
- [ ] Tenant isolation verified (a request with tenant A's JWT can't see
       tenant B's data — easiest test: switch tenants and confirm
       different data)

---

## §6 What to expect post-merge

### What you operate

- App container deployments (one per app, one core-email, one frontend)
- Per-tenant secret slots in SAM
- Auth/login flow for users
- Routing/ingress to the right tenant-app
- Logs, metrics, traces
- Backups of per-app SQLite data volumes (per tenant)

### What we operate

- App releases (we push new images; SAM triggers rolling upgrades)
- Bug fixes and feature work
- Adapter maintenance if SAM conventions change
- Health-check additions as data integrity questions evolve

### Customer-facing changes

- Single login to SAM instead of per-app login
- Same in-app UI as before (no UX change apart from login flow)
- Health Check button in each app's Settings menu (already there)
- Per-app Settings + Cleardown menus (already there)
- System Connection panel showing what backends are wired up (already
  there — values automatically reflect SAM-provided config)

### Customer-facing **non**-changes

- Opera 3 Agent now hosted by SAM (expanded from the legacy
  Windows-only write-only agent to handle both reads and writes;
  apps' adapters call it over HTTP)
- Per-app data (bank aliases, learned patterns, supplier history) stays
  with the customer's tenant, ported across to SAM-hosted volumes
- Workflow is identical (bank rec, gocardless import, supplier reconcile,
  balance check)

---

## §7 Reference quick links

| Need | Document |
|---|---|
| Every env var the apps consume | [env-var-contract.md](./env-var-contract.md) |
| How SAM adapters integrate (with code) | [sam-integration-pattern.md](./sam-integration-pattern.md) |
| Per-app dependency map | [dependency-graph.md](./dependency-graph.md) |
| Deployment topology options | [deployment-shapes.md](./deployment-shapes.md) |
| `/healthz` contract + k8s probe spec | [health-checks.md](./health-checks.md) |
| Per-app SAM-merge checklist | [migration-checklist.md](./migration-checklist.md) |
| Local dev quickstart (for SAM team to validate) | [QUICKSTART.md](./QUICKSTART.md) |
| Phase B status (ports/adapters built) | [phase-b-status.md](./phase-b-status.md) |
| Per-app deep-dive | [`apps/`](./apps/) directory |

---

## §8 Common pitfalls

These are issues we expect to hit during merge — none are
architectural blockers, but worth knowing in advance.

### "SAM uses different env-var names"
Solution: add aliases in `apps/core/env_config.py`. One file, ~10 lines.
Apps don't change.

### "Tenant has Opera 3 but SAM forgets to set OPERA3_AGENT_URL"
Symptom: bank-reconcile / gocardless return 503 from Opera 3 routes
(or HTTP 404/connection-refused from the Opera 3 Agent client).
Fix: set the env var in that tenant's secrets in SAM, pointing at
SAM's expanded Opera 3 Agent endpoint for that tenant. Health Check
catches this — its "Opera connection" sub-check fails with a clear
error message.

### "Customer changes Opera SQL password — apps stop working"
Today: customer updates `.env`, restarts the app.
Post-SAM: SAM admin updates the secret. Either propagates via TTL
(default 5 min in our adapter cache) or via webhook if SAM supports it.

### "Per-tenant SQLite volumes are huge"
Each customer's `data/{tenant_id}/` accumulates audit history. Plan
disk capacity ~500MB-2GB per active tenant per year. Consider monthly
cleardown via `/api/<app>/cleardown` endpoints (already built — see
each app's Settings menu).

### "Audit row shows Opera 3 stuff after upgrade to SE"
Expected. `bank_statement_imports` rows from the Opera 3 era have
`target_system='opera3'`. They stay as historical record. New imports
post-upgrade carry `target_system='opera_se'`. Both kinds participate
in dedup. **Don't delete the old rows** — that's the upgrade-time
deduplication chain.

---

## §9 Contact

For questions during merge:

- **Application owner:** charlieb@intsysuk.com
- **Architecture / adapter implementation:** see commit history on
  `main` for `apps/core/adapters/`, `apps/core/ports/`,
  `apps/core/env_config.py`
- **Repository:** https://github.com/HarryBurdett/llmragsql

For each phase of the merge, propose a brief sync to align on the
specifics. Most issues are resolved in 5 minutes when the right
person is in the room.

---

**End of handover document.** Sign-off when both teams have read this
and the env-var contract.
