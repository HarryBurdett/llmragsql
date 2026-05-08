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

### Per-app mailbox identity (NEW — required for every email-using app)

Each app may need a **different inbox**. Common scenario: bank statements
land in `accounts@customer.com`, supplier statements in
`ap@customer.com`, GoCardless payouts in `payments@customer.com`.
Equally common: a customer just uses **one** inbox for everything — in
which case set the same value for every app.

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_MAILBOX` | string | (falls back to `EMAIL_IMAP_USERNAME`) | The mailbox identity this app instance reads from / sends as. Set **per app** (per container). On Microsoft Graph this is the `userPrincipalName` (e.g. `payments@customer.com`); on classic IMAP/SMTP this matches the login. |

The MS Graph / IMAP / SMTP **credentials** are centralised — one set
per customer (see below). The mailbox identity is the only per-app
difference.

### Microsoft Graph (preferred for Microsoft 365 customers — central)

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_PROVIDER` | string | imap | `imap` (default), `microsoft` (MS Graph), or `gmail` |
| `EMAIL_MICROSOFT_TENANT_ID` | string | — | Azure AD / Entra ID tenant ID — **central**, one per customer |
| `EMAIL_MICROSOFT_CLIENT_ID` | string | — | Application (client) ID of the registered app — **central** |
| `EMAIL_MICROSOFT_CLIENT_SECRET` | string | — | Client secret — **central**; SAM populates per customer |

When `EMAIL_PROVIDER=microsoft`, the app uses the central Graph
credentials to authenticate, then accesses `EMAIL_MAILBOX` via the
`/users/{mailbox}/...` Graph endpoint. The app-registration must be
granted `Mail.Read`, `Mail.Send`, and (for suppliers) `Mail.ReadWrite`
on the `Application` permission set, with admin consent.

### IMAP (classic — receiving)

Used when `EMAIL_PROVIDER=imap` (or unset).

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_IMAP_ENABLED` | bool | true | Master switch for IMAP polling |
| `EMAIL_IMAP_SERVER` | string | — | IMAP hostname |
| `EMAIL_IMAP_PORT` | int | 993 | IMAP port |
| `EMAIL_IMAP_USE_SSL` | bool | true | Use TLS |
| `EMAIL_IMAP_USERNAME` | string | — | IMAP login (used as `EMAIL_MAILBOX` fallback) |
| `EMAIL_IMAP_PASSWORD` | string | — | IMAP password / app password |

### SMTP (sending — gocardless remittance, suppliers remittance)

| Env var | Type | Default | Description |
|---|---|---|---|
| `EMAIL_SMTP_SERVER` | string | — | SMTP hostname |
| `EMAIL_SMTP_PORT` | int | 587 | SMTP port |
| `EMAIL_SMTP_USERNAME` | string | — | SMTP login |
| `EMAIL_SMTP_PASSWORD` | string | — | SMTP password |
| `EMAIL_FROM_ADDRESS` | string | (falls back to `EMAIL_MAILBOX`) | Default From: header. Most installs leave this unset and let it default to the per-app mailbox. |

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
| `SAM_ENABLED` | bool | false | Enables SAM-specific adapters |
| `SAM_AUTH_URL` | URL | — | SAM auth service endpoint |
| `SAM_SECRETS_URL` | URL | — | SAM secrets service endpoint |
| `SAM_SERVICE_REGISTRY_URL` | URL | — | SAM service discovery |
| `AUTH_JWT_PUBLIC_KEY` | string | — | Public key to validate SAM-issued JWTs |

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

## Per-app required-env summary

| App | Always required | Conditional |
|---|---|---|
| bank-reconcile | `DATABASE_*`, `EMAIL_MAILBOX`, `EMAIL_*` (provider-specific), `GEMINI_API_KEY`, `OPERA_VERSION` | `OPERA3_AGENT_URL` if Opera 3 |
| gocardless | `DATABASE_*`, `EMAIL_MAILBOX`, `EMAIL_*` (provider-specific), `EMAIL_SMTP_*`, `GEMINI_API_KEY`, `GOCARDLESS_ACCESS_TOKEN` | `OPERA3_AGENT_URL` if Opera 3 |
| suppliers | `DATABASE_*`, `EMAIL_MAILBOX`, `EMAIL_*` (provider-specific), `EMAIL_SMTP_*`, `GEMINI_API_KEY` | `OPERA3_AGENT_URL` if Opera 3 |
| balance-check | `DATABASE_*` | `OPERA3_AGENT_URL` if Opera 3 |
| core-email | `EMAIL_MAILBOX`, `EMAIL_*` (provider-specific) | — |
| core-opera-se | `DATABASE_*` | — |
| ~~core-opera3~~ | *(no longer needed — SAM hosts the Opera 3 Agent)* | — |

**`EMAIL_*` (provider-specific)** means: when `EMAIL_PROVIDER=microsoft`,
the central Graph creds (`EMAIL_MICROSOFT_TENANT_ID/CLIENT_ID/CLIENT_SECRET`).
When `EMAIL_PROVIDER=imap` (default), the classic creds
(`EMAIL_IMAP_SERVER/PORT/USERNAME/PASSWORD`).

**Per-app mailbox examples**:
```bash
# Single shared mailbox — same value everywhere
bank-reconcile:    EMAIL_MAILBOX=accounts@customer.com
gocardless:        EMAIL_MAILBOX=accounts@customer.com
suppliers:         EMAIL_MAILBOX=accounts@customer.com

# Separate mailboxes per workflow
bank-reconcile:    EMAIL_MAILBOX=banking@customer.com
gocardless:        EMAIL_MAILBOX=payments@customer.com
suppliers:         EMAIL_MAILBOX=ap@customer.com
```

The credentials block (`EMAIL_MICROSOFT_*` or `EMAIL_IMAP_PASSWORD`) is
identical across all three apps for the same customer — only
`EMAIL_MAILBOX` differs.

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
