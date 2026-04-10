# GoCardless Direct Debit Import — Integration Guide

> Generated 10 April 2026

## Overview

Collect payments via GoCardless Direct Debit, match to customer invoices, post receipts to the accounting system.

**Module ID:** `gocardless`
**Version:** 1.0

---

## 1. Files to Copy

### Backend (Python)

Copy the entire module folder into your apps directory:

```
apps/gocardless/
  apps/gocardless/__init__.py
  apps/gocardless/api/__init__.py
  apps/gocardless/api/routes.py
  apps/gocardless/logic/__init__.py
  apps/gocardless/module.json
```

### Shared Libraries

These files from `sql_rag/` are required:

```
  api/main.py (per-request globals only)
  sql_rag/company_data.py
  sql_rag/gocardless_api.py
  sql_rag/gocardless_parser.py
  sql_rag/gocardless_payments.py
  sql_rag/import_lock.py
  sql_rag/opera3_config.py
  sql_rag/opera3_foxpro.py
  sql_rag/opera3_write_provider.py
  sql_rag/opera_config.py
  sql_rag/opera_sql_import.py
```

### Frontend

Copy these React pages into your `pages/` directory:

- **GoCardless Import** — route: `/cashbook/gocardless`
- **Payment Requests** — route: `/cashbook/gocardless/requests`
- **GoCardless Settings** — route: `/cashbook/gocardless/settings`

---

## 2. Register the API Router

Add to your main FastAPI app (e.g. `main.py`):

```python
from apps.gocardless.api.routes import router as gocardless_router
app.include_router(gocardless_router)
```

---

## 3. Add Routes (React)

Add to your router (e.g. `App.tsx`):

```tsx
<Route path="/cashbook/gocardless" element={<GoCardlessImport />} />
<Route path="/cashbook/gocardless/requests" element={<PaymentRequests />} />
<Route path="/cashbook/gocardless/settings" element={<GoCardlessSettings />} />
```

---

## 4. Add Menu Items

Add to your navigation/menu:

| Path | Label |
|------|-------|
| `/cashbook/gocardless` | GoCardless Import |
| `/cashbook/gocardless/requests` | Payment Requests |
| `/cashbook/gocardless/settings` | GoCardless Settings |

---

## 5. Dependencies

This module requires the host system to provide:

| Dependency | Type | Access | Description |
|-----------|------|--------|-------------|
| accounting_system | sql_connection | read_write | Connection to Opera SQL SE or equivalent accounting database |
| email | imap_smtp |  | IMAP for receiving, SMTP for sending |
| gocardless_api | rest_api | bearer_token | GoCardless API for payment creation, mandate management, payout retrieval |

### Config Adaptation

The module reads its connections from the host system. You need to ensure these are available:

- **SQL Connection**: The module calls `sql_connector` (passed per-request from the host app). Your system must provide a SQL connector to the Opera database and make it available to the module's routes.
- **Email** (if needed): IMAP/SMTP settings for receiving and sending emails.
- **Company Context**: The module is multi-company. It expects a company ID per request to scope its data.

The module does NOT have its own connection settings — it uses whatever the host system provides.

---

## 6. Settings to Configure

These settings are stored in the module's SQLite database and configured via the UI:

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `api_access_token` | secret | Yes |  |  |
| `api_sandbox` | boolean | No | False |  |
| `default_bank_code` | string | Yes |  | Opera bank account that receives GoCardless payouts |
| `gocardless_bank_code` | string | No |  | Intermediate bank if using two-stage posting |
| `fees_nominal_account` | string | Yes |  | Nominal code for GoCardless fee charges |
| `fees_vat_code` | string | No |  |  |
| `subscription_tag` | string | No | SUB |  |
| `company_reference` | string | No |  |  |
| `payout_lookback_days` | integer | No | 30 |  |
| `request_statement_reference` | string | No |  |  |

---

## 7. Data Files

These local databases/files need to be in the module's data directory:

| File | Type | Scope | Description |
|------|------|-------|-------------|
| `gocardless_payments.db` | sqlite | company | Mandate links, payment requests, subscription tracking |
| `gocardless_settings.json` | json | company | Module settings per company |

For migration: copy these from the old system's `data/{company}/` folder.

---

## 8. Locking Rules

These locking patterns are built into the module code. The host database must support them:

| Rule | Implementation |
|------|----------------|
| **Bank Import Lock** | Application-level lock per bank account — prevents concurrent imports to same bank (import_lock module) |
| **Sequence Allocation** | UPDLOCK, ROWLOCK on nparm (journal numbers), atype (entry numbers), nextid (row IDs) |
| **Balance Update** | ROWLOCK on nbank, sname, nacnt when updating balances |
| **Deadlock Retry** | All posting wrapped in execute_with_deadlock_retry (3 attempts, exponential backoff) |
| **Read Queries** | All SELECT queries use WITH (NOLOCK) — no read locks |
| **Duplicate Prevention** | Check existing payments in local DB + GoCardless API before creating new request |

---

## 9. API Endpoints

The module exposes these endpoints:

- `/api/gocardless/settings`
- `/api/gocardless/due-invoices`
- `/api/gocardless/payment-requests/*`
- `/api/gocardless/api-payouts`
- `/api/gocardless/import-batch`
- `/api/gocardless/mandates/*`
- `/api/gocardless/import-history`

---

## Migration Checklist

- [ ] Copy `apps/gocardless/` to the new system
- [ ] Copy required `sql_rag/` shared libraries
- [ ] Copy frontend page(s)
- [ ] Register API router in main app
- [ ] Add React routes
- [ ] Add menu items
- [ ] Provide SQL connection to Opera
- [ ] Provide email (IMAP/SMTP) configuration
- [ ] Configure external API credentials
- [ ] Copy data files from old system
- [ ] Configure module settings via the UI
- [ ] Test each endpoint and page
- [ ] Verify locking works correctly
