# Dependency Graph

Which application depends on which other application, and on which
external systems. Read this alongside [`env-var-contract.md`](./env-var-contract.md)
to understand what to plug in where.

## App-to-app dependencies (post-SAM merge)

```
                    ┌──────────────┐
                    │   frontend   │
                    │  (React UI)  │
                    └──────┬───────┘
                           │ HTTPS
                           ▼
                    ┌──────────────┐
                    │ SAM ingress  │
                    │  (replaces   │
                    │   nginx)     │
                    └──┬─┬─┬─┬─────┘
                       │ │ │ │
        ┌──────────────┘ │ │ └────────────────┐
        │                │ │                  │
        ▼                ▼ ▼                  ▼
 ┌─────────────┐  ┌──────────┐  ┌────────────┐  ┌───────────────┐
 │bank-recon-  │  │gocardless│  │ suppliers  │  │balance-check  │
 │   cile      │  │          │  │            │  │   (read-only) │
 └─────┬───────┘  └────┬─────┘  └────┬───────┘  └───────┬───────┘
       │               │             │                  │
       │               │             │                  │
       │  HTTPS via SAM_EMAIL_URL    │                  │
       └───────┬───────┴─────────────┘                  │
               ▼                                        │
       ┌────────────────────────┐                       │
       │  SAM email service     │                       │
       │  - Inbox / list / fetch│                       │
       │  - Attachments         │                       │
       │  - Send (SMTP equiv)   │                       │
       │  Per-app routing via   │                       │
       │  EMAIL_MAILBOX         │                       │
       └─────────┬──────────────┘                       │
                 │                                      │
                 ▼                                      │
       ┌──────────────────────────────────┐             │
       │ External: customer's mailbox     │             │
       │  (MS 365 / IMAP — connection     │             │
       │   owned by SAM, not our apps)    │             │
       └──────────────────────────────────┘             │
                                                        │
       ┌──────────────────────────────────────┬─────────┘
       │                                      │
       ▼                                      │
 ┌───────────────────┐                        │
 │ External:         │                        │
 │ Opera SQL Server  │◄───────────────────────┤
 │ (per tenant)      │                        │
 └───────────────────┘                        │
                                              │
 ┌─────────────────────────────────┐
 │ SAM-hosted:                     │
 │ Opera 3 Agent                   │
 │ — handles BOTH reads + writes   │
 │ (expanded from legacy           │
 │  Windows-only write agent)      │
 └─────────────────────────────────┘
                  ▲
                  │ HTTPS (read DBF + write DBF)
                  │
          ┌───────┴───────────────────┐
          │ Opera 3 tenants only      │
          │ (OPERA_VERSION=3)         │
          └───────────────────────────┘
```

**Key changes vs pre-SAM Docker stack:**
- ~~core-email~~ replaced by SAM email service
- ~~nginx-gateway~~ replaced by SAM ingress
- Opera 3 Agent now SAM-hosted (was customer-deployed Windows agent)
- Mailbox credentials now held by SAM, not in our env vars
- Each app calls `SAM_EMAIL_URL` and identifies its mailbox via `EMAIL_MAILBOX`

## Per-app dependencies

### bank-reconcile

**Depends on (HTTP):**
- **SAM email service** (`SAM_EMAIL_URL`) — fetches PDF attachments from inbox identified by `EMAIL_MAILBOX`
- (Optional: `core-opera-se` shared SQL gateway, not in initial merge)

**Depends on (external):**
- Opera SQL Server (writes via SQLAlchemy + pyodbc)
- SAM Opera 3 Agent (when `OPERA_VERSION=3`) — single HTTP endpoint for reads + writes
- Gemini API (PDF extraction)

**Provides (HTTP):**
- `/api/bank-import/*` — scan, preview, import endpoints
- `/api/reconcile/bank/{bank_code}/*` — reconcile UI endpoints
- `/api/opera3/bank-import/*` — Opera 3 mirrors

### gocardless

**Depends on (HTTP):**
- **SAM email service** (`SAM_EMAIL_URL`) — fetches GoCardless payout emails + sends remittance via the same service
- `bank-reconcile` (optional) — bank-aliases lookup if needed

**Depends on (external):**
- Opera SQL Server
- SAM Opera 3 Agent (when `OPERA_VERSION=3`) — single HTTP endpoint for reads + writes
- GoCardless API (payment platform)
- Gemini API

**Provides (HTTP):**
- `/api/gocardless/*` — settings, scan, import, remittance
- `/api/opera3/gocardless/*` — Opera 3 mirrors

### suppliers

**Depends on (HTTP):**
- **SAM email service** (`SAM_EMAIL_URL`) — supplier statement attachments, remittance email send, contact email send

**Depends on (external):**
- Opera SQL Server
- Gemini API (PDF extraction)

**Provides (HTTP):**
- `/api/suppliers/*` — statement reconciliation
- `/api/supplier-contacts/*` — contact management
- `/api/supplier-onboarding/*` — onboarding flow
- `/api/supplier-remittance/*` — remittance email
- `/api/supplier-aged/*` — aged debt analysis

### balance-check

**Depends on (HTTP):**
- (none — no inter-app HTTP calls)

**Depends on (external):**
- Opera SQL Server (read-only)

**Provides (HTTP):**
- `/api/reconcile/creditors`
- `/api/reconcile/debtors`
- `/api/reconcile/vat`
- `/api/reconcile/cashbook` (variance)

### ~~core-email~~ — replaced by SAM email service

The `core-email` container is **no longer part of the SAM merge bundle**.
SAM's email service replaces it — same capabilities (inbox poll,
attachment storage, send), but hosted by SAM and credentialed
centrally per customer.

If SAM doesn't take over auth/login, a slim `core-auth` may still be
needed for that — see §3 Q3 in the handover document.

## Routing summary (gateway)

| URL prefix | Backend | Purpose |
|---|---|---|
| `/api/bank-import/*` | bank-reconcile | Statement scan + import |
| `/api/reconcile/bank/{code}/*` | bank-reconcile | Reconcile UI |
| `/api/opera3/bank-import/*` | bank-reconcile | Opera 3 mirrors |
| `/api/repeat-entries/*` | bank-reconcile | Repeat entry processing |
| `/api/gocardless/*` | gocardless | GoCardless workflow |
| `/api/opera3/gocardless/*` | gocardless | Opera 3 mirror |
| `/api/suppliers/*`, `/api/supplier-*` | suppliers | Supplier workflow |
| `/api/reconcile/creditors\|debtors\|vat\|cashbook` | balance-check | Balance checks |
| `/api/email/*` | core-email | Email storage + search |
| `/api/system/*` | core-email | Auth + system |
| `/api/auth/*` | core-email | Login |
| `/api/companies/*` | core-email | Company management |
| `/api/installations/*` | core-email | Installation switching |
| `/healthz` | gateway | Aggregator health |

## SAM-side migration notes

When apps move into SAM, the dependency graph stays the same. SAM
just becomes the platform that:

1. Provides the network between containers (replaces `sqlrag-net`)
2. Provides the routing (replaces `nginx-gateway`)
3. Provides config + secrets (replaces `.env` + `env_file:`)
4. Optionally replaces `core-email`, `core-auth` with SAM equivalents

If SAM provides equivalents for `core-email` (an inbox / document
ingestion service) or `core-auth`, the Phase B adapters in each app
swap from "call our core-email" to "call SAM's email service" via
a one-config change. The apps don't know the difference.
