# Supplier Statement Reconciliation — Integration Guide

> Generated 10 April 2026

## Overview

Receive supplier statements by email, auto-reconcile against purchase ledger, generate responses with queries and payment schedules, track communications.

**Module ID:** `suppliers`
**Version:** 1.0

---

## 1. Files to Copy

### Backend (Python)

Copy the entire module folder into your apps directory:

```
apps/suppliers/
  apps/suppliers/__init__.py
  apps/suppliers/api/__init__.py
  apps/suppliers/api/background.py
  apps/suppliers/api/routes.py
  apps/suppliers/api/routes_aged.py
  apps/suppliers/api/routes_contacts.py
  apps/suppliers/api/routes_onboarding.py
  apps/suppliers/api/routes_remittance.py
  apps/suppliers/logic/__init__.py
  apps/suppliers/module.json
```

### Shared Libraries

These files from `sql_rag/` are required:

```
  api/main.py (per-request globals only)
  sql_rag/company_data.py
  sql_rag/opera3_foxpro.py
  sql_rag/opera3_write_provider.py
  sql_rag/supplier_config.py
  sql_rag/supplier_data_provider.py
  sql_rag/supplier_reconciler.py
  sql_rag/supplier_statement_db.py
  sql_rag/supplier_statement_extract.py
  sql_rag/supplier_statement_reconcile.py
```

### Frontend

Copy these React pages into your `pages/` directory:

- **Supplier Dashboard** — route: `/suppliers/dashboard`
- **Statement Queue** — route: `/suppliers/queue`
- **Supplier Directory** — route: `/suppliers/directory`
- **Supplier Settings** — route: `/suppliers/settings`

---

## 2. Register the API Router

Add to your main FastAPI app (e.g. `main.py`):

```python
from apps.suppliers.api.routes import router as suppliers_router
app.include_router(suppliers_router)
```

---

## 3. Add Routes (React)

Add to your router (e.g. `App.tsx`):

```tsx
<Route path="/suppliers/dashboard" element={<SupplierDashboard />} />
<Route path="/suppliers/queue" element={<StatementQueue />} />
<Route path="/suppliers/directory" element={<SupplierDirectory />} />
<Route path="/suppliers/settings" element={<SupplierSettings />} />
```

---

## 4. Add Menu Items

Add to your navigation/menu:

| Path | Label |
|------|-------|
| `/suppliers/dashboard` | Supplier Dashboard |
| `/suppliers/queue` | Statement Queue |
| `/suppliers/directory` | Supplier Directory |
| `/suppliers/settings` | Supplier Settings |

---

## 5. Dependencies

This module requires the host system to provide:

| Dependency | Type | Access | Description |
|-----------|------|--------|-------------|
| accounting_system | sql_connection | read_only | Read purchase ledger data for reconciliation |
| email | imap_smtp |  | IMAP for receiving, SMTP for sending responses |
| ai_extraction | rest_api |  | Extract transaction data from supplier statement PDFs |

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
| `send_acknowledgement` | boolean | No | True |  |
| `send_agreed_response` | boolean | No | True |  |
| `send_query_response` | boolean | No | True |  |
| `send_follow_up_reminders` | boolean | No | True |  |
| `auto_respond_if_reconciled` | boolean | No | True |  |
| `require_approval_for_queries` | boolean | No | True |  |
| `query_response_days` | integer | No | 7 |  |
| `follow_up_reminder_days` | integer | No | 14 |  |
| `max_follow_up_reminders` | integer | No | 3 |  |
| `next_payment_run_date` | date | No |  |  |
| `large_discrepancy_threshold` | decimal | No | 500 |  |
| `email_template_agreed` | text | No |  |  |
| `email_template_query` | text | No |  |  |
| `response_sign_off` | text | No | Regards, Accounts Department |  |
| `response_company_name` | text | No |  |  |
| `security_alert_recipients` | text | No |  |  |

---

## 7. Data Files

These local databases/files need to be in the module's data directory:

| File | Type | Scope | Description |
|------|------|-------|-------------|
| `supplier_statements.db` | sqlite | company | Statements, reconciliations, communications, config, approved senders |

For migration: copy these from the old system's `data/{company}/` folder.

---

## 8. Locking Rules

These locking patterns are built into the module code. The host database must support them:

| Rule | Implementation |
|------|----------------|
| **Read Only** | This module only READS from the accounting system — no write locks needed |
| **Read Queries** | All SELECT queries use WITH (NOLOCK) |
| **Local Db** | SQLite WAL mode for concurrent read/write on local supplier_statements.db |
| **Dedup** | Unique index on supplier_code + statement_date prevents duplicate processing |
| **Email Dedup** | processed_emails table prevents re-processing of same email |

---

## 9. API Endpoints

The module exposes these endpoints:

- `/api/supplier-statements/*`
- `/api/supplier-settings`
- `/api/supplier-config/*`
- `/api/supplier-queries/*`
- `/api/supplier-security/*`

---

## Migration Checklist

- [ ] Copy `apps/suppliers/` to the new system
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
