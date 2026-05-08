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

**Always (SAM-hosted):**
- `DATABASE_*` — Opera SQL connection (per tenant)
- `OPERA_VERSION` — `SE` or `3`
- `EMAIL_MAILBOX` — **the inbox GoCardless payout emails arrive in**
  (e.g. `payments@customer.com`, or the shared `accounts@customer.com`
  if the customer uses one inbox for everything)
- `SAM_EMAIL_URL` — SAM's email service base URL for this tenant
  (also used to send remittance — SAM handles inbound and outbound)
- `SAM_AUTH_TOKEN` — service token for calls to SAM
- `GEMINI_API_KEY` — AI extraction
- `GOCARDLESS_ACCESS_TOKEN` — API token (sandbox in dev)
- `GOCARDLESS_ENVIRONMENT` — sandbox / live
- `GOCARDLESS_WEBHOOK_SECRET` — inbound webhook validation

**Local dev / standalone (not SAM):**
- Replace SAM email vars with `EMAIL_PROVIDER`, `EMAIL_IMAP_*` or
  `EMAIL_MICROSOFT_*`, plus `EMAIL_SMTP_*` for remittance send.

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
