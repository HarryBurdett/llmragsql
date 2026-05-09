# suppliers (SAM plugin)

Supplier statement reconciliation against Pegasus Opera Purchase
Ledger. TypeScript port of `apps/suppliers/` — scans for supplier
statement emails, AI-extracts line items, and matches against
`ptran` / `pname`. Also handles supplier onboarding, contacts,
remittance generation, and creditor reporting.

## What SAM provides

| ctx field | Required? | Purpose |
| --- | --- | --- |
| `db.app` | yes | Per-app DB for statement queue, reconciliations, security audit log |
| `db.getCompanyDb(code)` | yes | Knex pool for Opera (pname, ptran, palloc, pnoml writes) |
| `operaType` | yes | `'opera-se'` or `'opera-3'` |
| `logger` | yes | Standard logger interface |
| `llm` | yes | Required by statement-from-email extractor |
| `emailIngest` | yes (in prod) | Required for `/api/supplier-statements/*` flows |
| `email` | optional | Sends remittance / response emails to suppliers |

## Built-in defaults

| Default | Override key on ctx | Activates when |
| --- | --- | --- |
| [defaultEmailIngestAdapter](src/services/default-email-ingest.ts) — wraps `ctx.emailIngest` as `supplierEmailAttachments` (returns email body or proxies to `getAttachmentText`) | `supplierEmailAttachments` | `ctx.emailIngest` available, `config.mailboxes` non-empty |

The default body extractor handles both `body_text` / `body_html`
fields and Microsoft Graph's `body: { contentType, content }`
shape; HTML is stripped to plain text when no text alternative is
present.

## Required `ctx.config` keys

| Key | Type | Purpose |
| --- | --- | --- |
| `mailboxes` | string[] | Mailbox addresses to claim for supplier statement intake |

## Routes

~60 endpoints — every Python `/api/supplier-*` URL has a 1:1 SAM
equivalent. Note: a few SAM canonical paths use a different
namespace than Python (`/api/suppliers/:code/contacts` vs Python's
`/api/supplier-contacts/:account`); the opera-3 mirror middleware
handles both spellings via prefix-stripping.

## Database

`db/migrations/` holds 4 Knex migrations covering the per-app
schema (statement queue, reconciliations, security audit). Smoke
tests at `tests/migrations.test.ts` catch dialect-agnostic bugs.

## Tests

```sh
npm test               # vitest run — 212 tests
npm run lint           # tsc --noEmit
```

## Frontend

Four ported pages from the legacy frontend:

- `SupplierAccount.tsx` — 1,342 LOC, deepest reconciliation UI
- `SupplierDashboard.tsx` — 457 LOC
- `SupplierReconciliations.tsx` — 195 LOC
- `SupplierStatementQueue.tsx` — 255 LOC

Tab switcher in `Suppliers.tsx` routes between them inside a
`QueryClientProvider`. Tailwind scoped to `.suppliers-app`.

```sh
cd frontend
npm install
npm run build
```
