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

We have four containerised business applications + a small core that
share a common architecture (env-var-driven config, ports/adapters
behind every external dependency). Phase A and Phase B of our work
were specifically designed for SAM-merge: every config value, every
external service, every shared resource is already abstracted behind
a swap point.

**Deployment context:** SAM and our apps run on the same platform
technology, in different locations. Same image format, same
manifest conventions, same observability stack — apps reach SAM via
URL across locations.

Your job is roughly:

1. Provide your conventions (auth, secrets) — see §3
2. We write ~6 small SAM adapter files matching those conventions
3. You configure per-tenant secrets in SAM
4. Deploy our images alongside SAM, run the included Health Check per app
5. Cut over

Estimated effort once SAM specifics are known: **~1 week of focused work**
on the application side, deployment + per-tenant configuration on yours.

**No application code changes are required for the merge** — only the
adapter layer and your deployment manifests.

---

## §1 What's being merged

### Application catalogue

| App | Purpose | Owns | Reads from |
|---|---|---|---|
| `bank-reconcile` | Bank statement scan + reconcile + Opera posting | `bank_aliases.db`, `bank_patterns.db`, statement-tracking SQLite | Opera SQL, IMAP, Gemini |
| `gocardless` | Direct Debit payout import | `gocardless_payments.db` | Opera SQL, IMAP, GoCardless API, Gemini |
| `suppliers` | Supplier statement reconciliation | `supplier_extraction_cache.db`, `supplier_statements.db` | Opera SQL, IMAP, SMTP, Gemini |
| `balance-check` | Internal Opera balance reconciliation | (read-only, no own state) | Opera SQL |

Plus a shared `core-email` IMAP poller (Phase B; can be replaced by SAM's
equivalent if SAM provides one).

### External dependencies

These exist outside SAM's control and your apps still consume them:

| External | What | Stays as-is? |
|---|---|---|
| Opera SQL Server | Customer's accounting database (Windows host) | Yes — connection details supplied per tenant |
| Opera 3 Agent | **SAM-hosted** service handling all Opera 3 reads + writes (expanded from the original write-only Windows agent). Reads FoxPro DBFs and posts transactions on behalf of our containers. | **Hosted by SAM** — no longer customer-deployed |
| Email IMAP/SMTP | Customer's email server | Yes — credentials per tenant |
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

- One deployment per app (5 deployments total: bank-reconcile, gocardless,
  suppliers, balance-check, core-email)
- Plus the frontend image (1 deployment) behind your ingress
- Health probes pointing at `/healthz`
- Resource requests/limits (we'll suggest baselines)
- Environment variables populated from your secrets store

Required env vars per app: see [env-var-contract.md](./env-var-contract.md).
Common to all:
```
DATABASE_*           Opera SQL connection
OPERA_VERSION        SE | 3 (per tenant — comes from SAM token claim)
COMPANY_DATA_BASE_PATH  /app/data (mounted volume)
SYSTEM_LOG_LEVEL     INFO
SAM_ENABLED          true
SAM_*_URL            your service URLs
AUTH_JWT_PUBLIC_KEY  for token validation
```

### Phase 4 — Per-tenant configuration (variable, SAM team per customer)

For each customer being migrated, SAM admin populates their secret
slot with:

- `DATABASE_SERVER`, `DATABASE_PORT`, `DATABASE_DATABASE`,
  `DATABASE_USERNAME`, `DATABASE_PASSWORD` — Opera SQL credentials
- **Email — central per customer:**
  - `EMAIL_PROVIDER` — `microsoft` (MS Graph, preferred) or `imap`
  - If `microsoft`: `EMAIL_MICROSOFT_TENANT_ID`,
    `EMAIL_MICROSOFT_CLIENT_ID`, `EMAIL_MICROSOFT_CLIENT_SECRET`
  - If `imap`: `EMAIL_IMAP_SERVER`, `EMAIL_IMAP_USERNAME`,
    `EMAIL_IMAP_PASSWORD`
  - `EMAIL_SMTP_SERVER`, `EMAIL_SMTP_USERNAME`, `EMAIL_SMTP_PASSWORD`
    (suppliers + gocardless send remittance emails)
- **Email — per-app mailbox identity** (set per app/container, not per
  customer):
  - `EMAIL_MAILBOX` — the inbox this app reads from / sends as.
    A customer may have one inbox for everything (set the same value
    for every app) or separate inboxes per workflow (e.g.
    `banking@customer.com` for bank-reconcile,
    `payments@customer.com` for gocardless,
    `ap@customer.com` for suppliers). The credentials above are
    shared; only `EMAIL_MAILBOX` differs per app.
  - `EMAIL_FROM_ADDRESS` — optional; defaults to `EMAIL_MAILBOX`
- `GEMINI_API_KEY` — AI extraction
- For GoCardless customers: `GOCARDLESS_ACCESS_TOKEN`,
  `GOCARDLESS_WEBHOOK_SECRET`
- For Opera 3 customers: `OPERA3_AGENT_URL` (per-tenant URL of SAM's
  Opera 3 Agent — handles both reads and writes; SAM populates this
  per tenant)

⚠️ **`GOCARDLESS_ENVIRONMENT`** must be set per-deployment, not per-tenant
(sandbox in dev, live in prod). Never per-tenant — it would risk live API
calls during testing.

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
