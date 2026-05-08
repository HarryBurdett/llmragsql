# bank-reconcile

Bank statement reconciliation: scan the inbox, extract PDFs, match
to Opera cashbook entries, post missing transactions, reconcile.

## Image target

`bank-reconcile` (in `Dockerfile`).

```
INSTALLED_APPS=bank_reconcile
APP_NAME=bank-reconcile
```

## Required env vars

See [`../env-var-contract.md`](../env-var-contract.md) for full
descriptions.

**Always:**
- `DATABASE_*` — Opera SQL connection
- `EMAIL_PROVIDER` — `microsoft` (MS Graph) or `imap`
- `EMAIL_MAILBOX` — **the inbox this app reads bank statements from**
  (e.g. `banking@customer.com`, or the customer's shared
  `accounts@customer.com` if they only use one inbox)
- If `EMAIL_PROVIDER=microsoft`: `EMAIL_MICROSOFT_TENANT_ID`,
  `EMAIL_MICROSOFT_CLIENT_ID`, `EMAIL_MICROSOFT_CLIENT_SECRET`
  (central, shared with all apps for the same customer)
- If `EMAIL_PROVIDER=imap`: `EMAIL_IMAP_SERVER`, `EMAIL_IMAP_USERNAME`,
  `EMAIL_IMAP_PASSWORD`
- `GEMINI_API_KEY`, `GEMINI_MODEL` — AI extraction
- `OPERA_VERSION` — `SE` or `3`
- `COMPANY_DATA_BASE_PATH` — per-company SQLite root
- `SYSTEM_LOG_LEVEL`

**If `OPERA_VERSION=3`:**
- `OPERA3_AGENT_URL` — SAM's expanded Opera 3 Agent (handles both
  reads and writes; replaces the legacy `OPERA3_DATA_PATH` and
  `OPERA3_WRITE_AGENT_URL` from the standalone era)

**Phase B (per-app split):**
- `CORE_EMAIL_URL` — point at the core-email service
- `CORE_OPERA_SE_URL` — only if using a shared SQL gateway

## Owns

- `bank_aliases.db` — payee → account mappings learned from imports
- `bank_patterns.db` — pattern learning for AI matching
- `pdf_extraction_cache.db` — cache of Gemini extractions

These live under `${COMPANY_DATA_BASE_PATH}/{company_id}/bank_reconcile/`
and are mounted as a volume.

## Provides (HTTP)

| Endpoint prefix | Purpose |
|---|---|
| `/api/bank-import/scan-emails` | List PDF candidates from inbox |
| `/api/bank-import/scan-folder` | List PDFs from local folder |
| `/api/bank-import/scan-all-banks` | Multi-bank inbox scan |
| `/api/bank-import/preview-from-email` | Preview match results |
| `/api/bank-import/preview-from-pdf` | Preview from disk PDF |
| `/api/bank-import/import-from-email` | Post matched transactions |
| `/api/bank-import/import-from-pdf` | Post matched transactions |
| `/api/reconcile/bank/{code}/*` | Reconcile UI endpoints |
| `/api/opera3/bank-import/*` | Opera 3 mirrors |
| `/api/repeat-entries/*` | Repeat-entry processing |
| `/healthz` | Container health |

## Consumes (HTTP)

- `core-email` — fetches PDF attachments, lists emails

## External dependencies

- Opera SQL Server (writes via SQLAlchemy + pyodbc)
- Opera 3 file share (read DBF) when `OPERA_VERSION=3`
- SAM's Opera 3 Agent (HTTP) for ALL Opera 3 reads + writes — no
  direct DBF access needed
- Gemini API for PDF extraction
- IMAP server (via core-email today; direct in Phase A monolith)

## SAM migration notes

- If SAM provides a document-ingestion service, replace
  `core-email` with a SAM adapter
- If SAM provides a SQL connection broker, point pyodbc through
  it (or use HTTP gateway)
- Gemini API key remains app-side (or SAM-secrets-provided)

## Health

- `GET /healthz` → 200 if process is up
- `GET /api/health` → detailed (Opera SQL connection status, etc.)
