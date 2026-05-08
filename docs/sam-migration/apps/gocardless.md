# gocardless

GoCardless (Direct Debit) payout import. Scans the inbox for
GoCardless payout notifications, extracts payment details, matches
to Opera customers by invoice ref or name, posts as sales receipts.

## Image target

`gocardless` (in `Dockerfile`).

```
INSTALLED_APPS=gocardless
APP_NAME=gocardless
```

## Required env vars

**Always:**
- `DATABASE_*` — Opera SQL connection
- `EMAIL_PROVIDER` — `microsoft` (MS Graph) or `imap`
- `EMAIL_MAILBOX` — **the inbox GoCardless payout emails arrive in**
  (e.g. `payments@customer.com`, or the shared `accounts@customer.com`
  if the customer uses one inbox for everything)
- If `EMAIL_PROVIDER=microsoft`: `EMAIL_MICROSOFT_TENANT_ID`,
  `EMAIL_MICROSOFT_CLIENT_ID`, `EMAIL_MICROSOFT_CLIENT_SECRET`
  (central, shared with all apps for the same customer)
- If `EMAIL_PROVIDER=imap`: `EMAIL_IMAP_SERVER`, `EMAIL_IMAP_USERNAME`,
  `EMAIL_IMAP_PASSWORD`
- `EMAIL_SMTP_*`, `EMAIL_FROM_ADDRESS` (optional — defaults to
  `EMAIL_MAILBOX`) — remittance emails
- `GEMINI_API_KEY` — AI extraction
- `GOCARDLESS_ACCESS_TOKEN` — API token (sandbox in dev)
- `GOCARDLESS_ENVIRONMENT` — sandbox / live
- `GOCARDLESS_WEBHOOK_SECRET` — inbound webhook validation
- `OPERA_VERSION` — `SE` or `3`

⚠️ **Use sandbox tokens in development** per
[CLAUDE.md](../../CLAUDE.md). Never make live API requests against
the production GoCardless endpoint while testing.

**Phase B:**
- `CORE_EMAIL_URL`
- `BANK_RECONCILE_URL` (for bank_aliases lookup)

## Owns

- `gocardless_payments.db` — payment-tracking SQLite per company

## Provides (HTTP)

| Endpoint prefix | Purpose |
|---|---|
| `/api/gocardless/settings` | API token + bank account config |
| `/api/gocardless/scan-emails` | Scan inbox for payout emails |
| `/api/gocardless/preview-batch` | Preview customer matches |
| `/api/gocardless/import-batch` | Post sales receipts to Opera |
| `/api/gocardless/remittance/*` | Send remittance emails |
| `/api/opera3/gocardless/*` | Opera 3 mirrors |
| `/healthz` | Container health |

## Consumes (HTTP)

- `core-email` — fetches GoCardless payout emails
- `bank-reconcile` (Phase B) — bank-aliases lookup

## External dependencies

- Opera SQL Server (writes sales receipts, atran, anoml, ntran,
  nacnt, nbank — see opera_knowledge_base.md)
- GoCardless API (payouts, mandates, customers)
- IMAP / SMTP
- Gemini API

## SAM migration notes

- `GOCARDLESS_ACCESS_TOKEN` is **per-tenant**: SAM must provide
  the right one based on which customer's instance this is
- `GOCARDLESS_ENVIRONMENT` is **per-deployment**: dev = sandbox,
  prod = live (NEVER per-tenant — would blow real money)

## Health

- `GET /healthz` → process up
- `GET /api/gocardless/health` → API token validity + bank reachable
