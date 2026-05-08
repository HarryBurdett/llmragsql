# Dependency Graph

Which application depends on which other application, and on which
external systems. Read this alongside [`env-var-contract.md`](./env-var-contract.md)
to understand what to plug in where.

## App-to-app dependencies (HTTP)

```
                    ┌──────────────┐
                    │   frontend   │
                    │  (React UI)  │
                    └──────┬───────┘
                           │ HTTP
                           ▼
                    ┌──────────────┐
                    │   gateway    │
                    │   (nginx)    │
                    └──┬─┬─┬─┬─┬───┘
                       │ │ │ │ │
        ┌──────────────┘ │ │ │ └────────────────┐
        │                │ │ │                  │
        ▼                ▼ │ ▼                  ▼
 ┌─────────────┐  ┌──────────┐  ┌────────────┐  ┌───────────────┐
 │bank-recon-  │  │gocardless│  │ suppliers  │  │balance-check  │
 │   cile      │  │          │  │            │  │   (read-only) │
 └─────┬───────┘  └────┬─────┘  └────┬───────┘  └───────┬───────┘
       │               │             │                  │
       │               │             │                  │
       │  HTTP         │             │                  │
       └───────┬───────┴─────────────┘                  │
               ▼                                        │
        ┌─────────────┐                                 │
        │ core-email  │                                 │
        │ (IMAP +     │                                 │
        │  storage)   │                                 │
        └─────┬───────┘                                 │
              │                                         │
              ▼                                         │
       ┌──────────────────────────────────┐             │
       │  External: IMAP server           │             │
       └──────────────────────────────────┘             │
                                                        │
       ┌──────────────────────────────────────┬─────────┘
       │                                      │
       ▼                                      │
 ┌───────────────────┐                        │
 │ External:         │                        │
 │ Opera SQL Server  │◄───────────────────────┤
 │ (Windows)         │                        │
 └───────────────────┘                        │
                                              │
 ┌─────────────────────────────────┐
 │ SAM (different location):       │
 │ Opera 3 Agent                   │
 │ — handles BOTH reads + writes   │
 │ (expanded from legacy           │
 │  Windows-only write agent)      │
 └─────────────────────────────────┘
                  ▲
                  │ HTTP (read DBF + write DBF)
                  │
          ┌───────┴───────────────────┐
          │ All apps when             │
          │ OPERA_VERSION=3           │
          └───────────────────────────┘
```

## Per-app dependencies

### bank-reconcile

**Depends on (HTTP):**
- `core-email` — fetches PDF attachments from inbox
- (Phase B: also `core-opera-se` via HTTP. Phase A: direct pyodbc.)

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
- `core-email` — fetches GoCardless payout emails
- `bank-reconcile` (Phase B) — bank-aliases lookup if needed

**Depends on (external):**
- Opera SQL Server
- SAM Opera 3 Agent (when `OPERA_VERSION=3`) — single HTTP endpoint for reads + writes
- GoCardless API (payment platform)
- IMAP / SMTP (remittance)
- Gemini API

**Provides (HTTP):**
- `/api/gocardless/*` — settings, scan, import, remittance
- `/api/opera3/gocardless/*` — Opera 3 mirrors

### suppliers

**Depends on (HTTP):**
- `core-email` — supplier statement attachments

**Depends on (external):**
- Opera SQL Server
- IMAP / SMTP (remittance, contact email)
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

### core-email

**Depends on (HTTP):**
- (none)

**Depends on (external):**
- IMAP server (poll for new mail)
- SMTP server (send remittance — actually owned by sender apps,
  but core-email exposes a send helper)

**Provides (HTTP):**
- `/api/email/*` — list, search, attachment download
- `/api/system/*` — auth, company switching (during Phase A)
- IMAP poller runs as a background task in the same process

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
