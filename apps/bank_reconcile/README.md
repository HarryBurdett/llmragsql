# Bank Statement Reconciliation — Integration Guide

> Generated 10 April 2026

## Overview

Import bank statements from PDF (email or file), AI-extract transactions, auto-match to cashbook, post unmatched entries, reconcile.

**Module ID:** `bank_reconcile`
**Version:** 1.0

---

## 1. Files to Copy

### Backend (Python)

Copy the entire module folder into your apps directory:

```
apps/bank_reconcile/
  apps/bank_reconcile/__init__.py
  apps/bank_reconcile/api/__init__.py
  apps/bank_reconcile/api/routes.py
  apps/bank_reconcile/logic/__init__.py
  apps/bank_reconcile/module.json
```

### Shared Libraries

These files from `sql_rag/` are required:

```
  api/main.py (per-request globals only)
  sql_rag/bank_aliases.py
  sql_rag/bank_duplicates.py
  sql_rag/bank_import.py
  sql_rag/bank_import_opera3.py
  sql_rag/bank_patterns.py
  sql_rag/file_archive.py
  sql_rag/import_lock.py
  sql_rag/opera3_config.py
  sql_rag/opera3_data_provider.py
  sql_rag/opera3_foxpro.py
  sql_rag/opera3_write_provider.py
  sql_rag/opera_config.py
  sql_rag/opera_sql_import.py
  sql_rag/pdf_extraction_cache.py
  sql_rag/smb_access.py
  sql_rag/statement_reconcile.py
  sql_rag/statement_reconcile_opera3.py
```

### Frontend

Copy these React pages into your `pages/` directory:

- **Bank Statement Reconciliation** — route: `/cashbook/statement-reconcile`

---

## 2. Register the API Router

Add to your main FastAPI app (e.g. `main.py`):

```python
from apps.bank_reconcile.api.routes import router as bank_reconcile_router
app.include_router(bank_reconcile_router)
```

---

## 3. Add Routes (React)

Add to your router (e.g. `App.tsx`):

```tsx
<Route path="/cashbook/statement-reconcile" element={<BankStatementReconciliation />} />
```

---

## 4. Add Menu Items

Add to your navigation/menu:

| Path | Label |
|------|-------|
| `/cashbook/statement-reconcile` | Bank Statement Reconciliation |

---

## 5. Dependencies

This module requires the host system to provide:

| Dependency | Type | Access | Description |
|-----------|------|--------|-------------|
| accounting_system | sql_connection | read_write | Connection to Opera SQL SE or equivalent accounting database |
| email | imap_smtp |  | IMAP for receiving statements by email |
| ai_extraction | rest_api |  | Gemini Vision API for PDF text extraction and transaction parsing |

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
| `gemini_api_key` | secret | Yes |  |  |
| `statement_folder` | path | No |  | Local folder to scan for bank statement PDFs |

---

## 7. Data Files

These local databases/files need to be in the module's data directory:

| File | Type | Scope | Description |
|------|------|-------|-------------|
| `bank_aliases.db` | sqlite | company | Learned payee name to Opera account mappings |
| `bank_patterns.db` | sqlite | company | Pattern learning from successful imports |
| `pdf_extraction_cache.db` | sqlite | company | Cached PDF extraction results by content hash |
| `bank_statement_imports.db` | sqlite | company | Import history and duplicate tracking |

For migration: copy these from the old system's `data/{company}/` folder.

---

## 8. Locking Rules

These locking patterns are built into the module code. The host database must support them:

| Rule | Implementation |
|------|----------------|
| **Bank Import Lock** | Application-level lock per bank account — prevents concurrent imports to same bank |
| **Sequence Allocation** | UPDLOCK, ROWLOCK on nparm, atype, nextid when posting transactions |
| **Balance Update** | ROWLOCK on nbank, sname, pname, nacnt when updating balances |
| **Deadlock Retry** | All posting wrapped in execute_with_deadlock_retry (3 attempts) |
| **Read Queries** | All SELECT queries use WITH (NOLOCK) |
| **Record Lock Check** | Checks for Opera application-level locks before posting (prevents conflict with Opera users) |
| **Duplicate Detection** | ABS(ABS(at_value) - amount) with date range check before posting |

---

## 9. API Endpoints

The module exposes these endpoints:

- `/api/bank-reconcile/scan-statements`
- `/api/bank-reconcile/extract`
- `/api/bank-reconcile/match`
- `/api/bank-reconcile/import`
- `/api/bank-reconcile/reconcile`

---

## Migration Checklist

- [ ] Copy `apps/bank_reconcile/` to the new system
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
