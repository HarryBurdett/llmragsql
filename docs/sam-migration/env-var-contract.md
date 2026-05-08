# Env-Var Contract

Every environment variable any application consumes, organised by
purpose. This is the **canonical source of truth** for what SAM
needs to provide.

## Convention

`[section] key = value` in `config.ini` maps to env var `SECTION_KEY`
(uppercased). Apps read from `os.environ` via the
`apps/core/env_config.py` loader, which transparently merges env
vars and (in development) `config.ini`.

Resolution order (highest priority first):
1. Environment variable
2. `config.ini` at the repo root (development convenience only)
3. Caller-supplied fallback in `cfg.get(..., fallback=X)`

## Required for every app

| Env var | Type | Default | Description |
|---|---|---|---|
| `DATABASE_SERVER` | string | — | Opera SQL Server hostname/IP |
| `DATABASE_PORT` | int | 1433 | Opera SQL port |
| `DATABASE_DATABASE` | string | — | Opera database name |
| `DATABASE_USE_WINDOWS_AUTH` | bool | false | Windows integrated auth |
| `DATABASE_USERNAME` | string | — | Opera SQL login (required if use_windows_auth=false) |
| `DATABASE_PASSWORD` | string | — | Opera SQL password |
| `DATABASE_POOL_SIZE` | int | 5 | Connection pool size |
| `DATABASE_MAX_OVERFLOW` | int | 10 | Pool overflow allowance |
| `DATABASE_POOL_TIMEOUT` | int | 30 | Seconds to wait for a connection |
| `DATABASE_POOL_RECYCLE` | int | 3600 | Recycle connections after N seconds |
| `DATABASE_CONNECTION_TIMEOUT` | int | 30 | Connection-establishment timeout |
| `DATABASE_COMMAND_TIMEOUT` | int | 60 | Per-query timeout |
| `DATABASE_SSL` | bool | false | Use TLS to Opera SQL |
| `DATABASE_TRUST_SERVER_CERTIFICATE` | bool | true | Skip cert validation |
| `OPERA_VERSION` | string | SE | `SE` or `3` — selects integration path |
| `SYSTEM_LOG_LEVEL` | string | INFO | Python logging level |
| `COMPANY_DATA_BASE_PATH` | path | ./data | Per-company SQLite root |

## Required for AI extraction (bank-reconcile, suppliers)

| Env var | Type | Default | Description |
|---|---|---|---|
| `GEMINI_API_KEY` | string | — | Google Gemini API key |
| `GEMINI_MODEL` | string | gemini-2.0-flash | Gemini model ID |
| `MODELS_PROVIDER` | string | gemini | gemini / openai / anthropic / groq / local |
| `MODELS_EMBEDDING_MODEL` | string | all-MiniLM-L6-v2 | Sentence-transformer model |

Optional alternative AI providers (set the matching key + change `MODELS_PROVIDER`):
- `OPENAI_API_KEY`, `OPENAI_MODEL`
- `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`
- `GROQ_API_KEY`, `GROQ_MODEL`

## Required for email-using apps (bank-reconcile, gocardless, suppliers)

**Architecture (post-SAM):** SAM owns the connection to the customer's
mailbox — MS Graph credentials, IMAP credentials, SMTP send pipeline.
Our apps **never see mailbox passwords**. They call SAM's email service
via `SAM_EMAIL_URL` and identify their target mailbox via
`EMAIL_MAILBOX`.

### Per-app mailbox identity (required for every email-using app)

Each app may need a **different inbox**. Common scenario: bank
statements land in `banking@customer.com`, supplier statements in
`ap@customer.com`, GoCardless payouts in `payments@customer.com`.
Equally common: a customer uses **one** inbox for everything — in
which case set the same value for every app.

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_MAILBOX` | string | — | The mailbox identity this app reads from / sends as. Set **per app** (per container). SAM uses this to route the app to the right mailbox when it calls `SAM_EMAIL_URL`. |

Examples:
```bash
# Single shared mailbox
bank-reconcile:    EMAIL_MAILBOX=accounts@customer.com
gocardless:        EMAIL_MAILBOX=accounts@customer.com
suppliers:         EMAIL_MAILBOX=accounts@customer.com

# Separate mailboxes per workflow
bank-reconcile:    EMAIL_MAILBOX=banking@customer.com
gocardless:        EMAIL_MAILBOX=payments@customer.com
suppliers:         EMAIL_MAILBOX=ap@customer.com
```

### SAM email service (post-SAM, central per tenant)

| Env var | Type | Default | Description |
|---|---|---|---|
| `SAM_EMAIL_URL` | URL | — | Base URL of SAM's email service for this tenant (e.g. `https://sam.example.com/email/{tenant}/`). Provides inbox listing, message fetch, attachment download, send. |
| `SAM_AUTH_TOKEN` | string | — | Service token our apps use to authenticate to SAM's email service (and other SAM services). |

**SAM-side responsibilities:**
- Holds MS Graph / IMAP / SMTP credentials per customer
- Connects to the customer's mailbox on our apps' behalf
- Exposes inbox / attachments / send via HTTP
- Routes per-app calls to the right mailbox using `EMAIL_MAILBOX`

**Our-side responsibilities:**
- Implement HTTP adapter against SAM's email API
  (`apps/core/adapters/sam/email_storage.py`)
- Pass `EMAIL_MAILBOX` when listing or fetching messages

### Pre-SAM email config (for development / standalone deployments only)

These env vars are **only used when running outside SAM** (e.g. local
dev, on-prem standalone, or pre-merge testing). SAM-hosted deployments
don't set these — SAM's email service replaces them.

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_PROVIDER` | string | imap | `imap`, `microsoft` (MS Graph), or `gmail` |
| `EMAIL_MICROSOFT_TENANT_ID` | string | — | Entra ID tenant — Microsoft 365 deployments |
| `EMAIL_MICROSOFT_CLIENT_ID` | string | — | App registration client ID |
| `EMAIL_MICROSOFT_CLIENT_SECRET` | string | — | App registration secret |
| `EMAIL_IMAP_SERVER` | string | — | IMAP hostname (classic) |
| `EMAIL_IMAP_PORT` | int | 993 | |
| `EMAIL_IMAP_USE_SSL` | bool | true | |
| `EMAIL_IMAP_USERNAME` | string | — | IMAP login |
| `EMAIL_IMAP_PASSWORD` | string | — | IMAP password |
| `EMAIL_SMTP_SERVER` | string | — | SMTP hostname |
| `EMAIL_SMTP_PORT` | int | 587 | |
| `EMAIL_SMTP_USERNAME` | string | — | |
| `EMAIL_SMTP_PASSWORD` | string | — | |
| `EMAIL_FROM_ADDRESS` | string | (falls back to `EMAIL_MAILBOX`) | From: header for sends |

When `SAM_ENABLED=true`, the adapter factory ignores all of the above
and uses the SAM email adapter instead.

## Required for Opera 3 deployments

| Env var | Type | Default | Description |
|---|---|---|---|
| `OPERA3_AGENT_URL` | URL | — | HTTP URL of SAM's expanded Opera 3 Agent — handles BOTH reads (FoxPro DBF queries) AND writes (postings). Per-tenant; SAM populates this. |
| `OPERA3_WRITE_AGENT_URL` | URL | — | **Legacy / backwards-compat.** When `OPERA3_AGENT_URL` is unset, the local writer falls back to this for write operations only. New deployments should use `OPERA3_AGENT_URL`. |

**Architecture update (post-SAM expansion):** The Opera 3 Agent has
been **expanded to handle both reads and writes** and is now hosted
by SAM. Our containers no longer need direct DBF file-share access
(no SMB mount required). All Opera 3 access — read and write — flows
through the agent over HTTP.

Older standalone deployments (pre-SAM) still work via the legacy
`OPERA3_WRITE_AGENT_URL` env var pointing at a customer-deployed
Windows Write Agent for writes only, with reads going through direct
DBF access. SAM-hosted deployments use the single `OPERA3_AGENT_URL`
endpoint for everything.

## Required for GoCardless app

| Env var | Type | Default | Description |
|---|---|---|---|
| `GOCARDLESS_ACCESS_TOKEN` | string | — | API token (sandbox or live) |
| `GOCARDLESS_ENVIRONMENT` | string | sandbox | sandbox / live |
| `GOCARDLESS_WEBHOOK_SECRET` | string | — | Webhook signature verification |

⚠️ Use sandbox tokens in development per
[CLAUDE.md](/CLAUDE.md). Never make live API requests against the
production GoCardless endpoint while testing.

## SAM platform integration (post-migration)

| Env var | Type | Default | Description |
|---|---|---|---|
| `SAM_ENABLED` | bool | false | Enables SAM-specific adapters across the board |
| `SAM_AUTH_URL` | URL | — | SAM auth service endpoint |
| `SAM_SECRETS_URL` | URL | — | SAM secrets service endpoint |
| `SAM_SERVICE_REGISTRY_URL` | URL | — | SAM service discovery |
| `SAM_EMAIL_URL` | URL | — | SAM email service base URL (per tenant) |
| `SAM_AUTH_TOKEN` | string | — | Short-lived service token for inter-service auth to SAM |
| `AUTH_JWT_PUBLIC_KEY` | string | — | Public key to validate inbound SAM-issued JWTs |

When `SAM_ENABLED=true`, apps switch to SAM-aware adapters that:
- Validate inbound JWTs against `AUTH_JWT_PUBLIC_KEY`
- Fetch secrets from SAM rather than env vars where applicable
- Route service calls through SAM's service registry

When `SAM_ENABLED=false` (pre-SAM Docker deployment), the apps use
local adapters with values from env vars / docker-compose.

## Inter-service URLs (Phase B and beyond)

When apps are split into separate containers (Phase B), each app
needs to know where the others live. These are populated by
docker-compose / SAM, not by users.

| Env var | Type | Default | Description |
|---|---|---|---|
| `CORE_EMAIL_URL` | URL | — | core-email service base URL |
| `CORE_OPERA_SE_URL` | URL | — | core-opera-se gateway URL |
| `CORE_OPERA3_URL` | URL | — | core-opera3 gateway URL |
| `CORE_AUTH_URL` | URL | — | core-auth service URL |
| `BANK_RECONCILE_URL` | URL | — | bank-reconcile app URL |
| `GOCARDLESS_URL` | URL | — | gocardless app URL |
| `SUPPLIERS_URL` | URL | — | suppliers app URL |
| `BALANCE_CHECK_URL` | URL | — | balance-check app URL |

Apps only need URLs for services they actually call — see each app's
detail page for its specific dependencies.

## Per-app required-env summary (SAM-hosted)

When `SAM_ENABLED=true`:

| App | Always required | Conditional |
|---|---|---|
| bank-reconcile | `DATABASE_*`, `OPERA_VERSION`, `EMAIL_MAILBOX`, `SAM_EMAIL_URL`, `SAM_AUTH_TOKEN`, `GEMINI_API_KEY` | `OPERA3_AGENT_URL` if Opera 3 |
| gocardless | `DATABASE_*`, `OPERA_VERSION`, `EMAIL_MAILBOX`, `SAM_EMAIL_URL`, `SAM_AUTH_TOKEN`, `GEMINI_API_KEY`, `GOCARDLESS_ACCESS_TOKEN` | `OPERA3_AGENT_URL` if Opera 3 |
| suppliers | `DATABASE_*`, `OPERA_VERSION`, `EMAIL_MAILBOX`, `SAM_EMAIL_URL`, `SAM_AUTH_TOKEN`, `GEMINI_API_KEY` | `OPERA3_AGENT_URL` if Opera 3 |
| balance-check | `DATABASE_*`, `OPERA_VERSION` | `OPERA3_AGENT_URL` if Opera 3 |
| ~~core-email~~ | *(replaced by SAM's email service)* | — |
| ~~core-opera3~~ | *(SAM hosts the Opera 3 Agent)* | — |

**Notes:**
- `EMAIL_MAILBOX` is the only per-app email env var. Everything else
  about email (credentials, connection to MS Graph / IMAP, send
  pipeline) lives in SAM.
- `SAM_EMAIL_URL` and `SAM_AUTH_TOKEN` are typically the same value
  across all apps for a given tenant — SAM populates them centrally.
- For local dev / standalone (no SAM), use the "Pre-SAM email config"
  block above instead of `SAM_EMAIL_URL`.

## Migration to SAM

When the apps move into SAM, the env-var names should remain the
same. SAM-side migration just changes **what populates them**:
- Today (docker-compose): `.env` file or compose `environment:` block
- Tomorrow (SAM): SAM-managed secrets store, projected as env vars
  or mounted files that get read into env vars at startup

If SAM uses different naming conventions (e.g. `database_server` vs
`DATABASE_SERVER`), add a startup adapter in `apps/core/env_config.py`
that aliases SAM names to the application names. This avoids
touching every app.
