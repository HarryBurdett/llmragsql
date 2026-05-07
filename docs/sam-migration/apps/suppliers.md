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

**Always:**
- `DATABASE_*` — Opera SQL connection
- `EMAIL_IMAP_*` — inbox for statements
- `EMAIL_SMTP_*`, `EMAIL_FROM_ADDRESS` — remittance + contact email
- `GEMINI_API_KEY` — AI extraction
- `OPERA_VERSION` — `SE` or `3`

**If `OPERA_VERSION=3`:**
- `OPERA3_DATA_PATH`
- `OPERA3_WRITE_AGENT_URL`

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
- Opera 3 file share / Write Agent when `OPERA_VERSION=3`
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
