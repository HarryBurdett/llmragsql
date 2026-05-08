# suppliers

Supplier statement reconciliation, contact management, remittance.
AI-powered extraction from PDF/email statements; matching against
the Purchase Ledger; remittance email generation.

## Image target

`suppliers` (in `Dockerfile`).

```
INSTALLED_APPS=suppliers
APP_NAME=suppliers
```

## Required env vars

**Always (SAM-hosted):**
- `DATABASE_*` — Opera SQL connection (per tenant)
- `OPERA_VERSION` — `SE` or `3`
- `EMAIL_MAILBOX` — **the inbox supplier statements arrive in**
  (e.g. `ap@customer.com`, or the shared `accounts@customer.com` if
  the customer uses one inbox for everything)
- `SAM_EMAIL_URL` — SAM's email service base URL for this tenant
  (used for inbox + remittance + contact email — SAM handles inbound
  and outbound)
- `SAM_AUTH_TOKEN` — service token for calls to SAM
- `GEMINI_API_KEY` — AI extraction

**Local dev / standalone (not SAM):**
- Replace SAM email vars with `EMAIL_PROVIDER`, `EMAIL_IMAP_*` or
  `EMAIL_MICROSOFT_*`, plus `EMAIL_SMTP_*` for outbound mail.

**If `OPERA_VERSION=3`:**
- `OPERA3_AGENT_URL` — SAM's expanded Opera 3 Agent (handles both
  reads and writes; replaces the legacy `OPERA3_DATA_PATH` /
  `OPERA3_WRITE_AGENT_URL` pair from the standalone era)

**Phase B:**
- `CORE_EMAIL_URL`

## Owns

- `supplier_extraction_cache.db` — Gemini extraction cache for
  supplier statements (separate from bank-rec's PDF cache because
  the prompts and field schema differ)
- `supplier_statements.db` — per-supplier sync schedule, extraction
  history, remittance log

## Provides (HTTP)

| Endpoint prefix | Purpose |
|---|---|
| `/api/suppliers/scan` | Scan inbox + folder for statements |
| `/api/suppliers/preview` | Preview reconciliation result |
| `/api/suppliers/reconcile` | Run reconciliation |
| `/api/suppliers/variance` | Variance report |
| `/api/supplier-contacts/*` | Contact management |
| `/api/supplier-onboarding/*` | New supplier onboarding flow |
| `/api/supplier-remittance/*` | Generate + send remittance |
| `/api/supplier-aged/*` | Aged debt analysis |
| `/healthz` | Container health |

## Consumes (HTTP)

- `core-email` — supplier statement attachments

## External dependencies

- Opera SQL Server (read pname, ptran, palloc; suppliers app is
  read-only against Opera in production today — no postings)
- SAM's Opera 3 Agent when `OPERA_VERSION=3` (HTTP — handles both
  reads and writes; no direct DBF access needed)
- IMAP / SMTP
- Gemini API

## SAM migration notes

- The periodic bank-detail-change scan task (audit F1) needs SAM
  to provide either:
  - A scheduled-task primitive (cron-like service), OR
  - The IMAP poller continues running in-app and triggers the scan
    via post-sync callback (current behaviour)
- Supplier remittance emails go via SMTP — confirm SAM provides
  SMTP credentials per tenant

## Health

- `GET /healthz` → process up
- `GET /api/suppliers/health` → SMTP reachable + Opera SQL connected
