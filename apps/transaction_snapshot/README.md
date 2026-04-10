# Transaction Snapshot — Integration Guide

> Generated 10 April 2026

## Overview

Capture before/after snapshots of Opera database transactions to learn exact posting patterns. Feeds the import engine registry.

**Module ID:** `transaction_snapshot`
**Version:** 1.0

---

## 1. Files to Copy

### Backend (Python)

Copy the entire module folder into your apps directory:

```
apps/transaction_snapshot/
  apps/transaction_snapshot/api/__init__.py
  apps/transaction_snapshot/api/routes.py
  apps/transaction_snapshot/module.json
```

### Shared Libraries

These files from `sql_rag/` are required:

```
  api/main.py (per-request globals only)
  sql_rag/smb_access.py
```

### Frontend

Copy these React pages into your `pages/` directory:

- **Transaction Snapshot** — route: `/utilities/transaction-snapshot`

---

## 2. Register the API Router

Add to your main FastAPI app (e.g. `main.py`):

```python
from apps.transaction_snapshot.api.routes import router as transaction_snapshot_router
app.include_router(transaction_snapshot_router)
```

---

## 3. Add Routes (React)

Add to your router (e.g. `App.tsx`):

```tsx
<Route path="/utilities/transaction-snapshot" element={<TransactionSnapshot />} />
```

---

## 4. Add Menu Items

Add to your navigation/menu:

| Path | Label |
|------|-------|
| `/utilities/transaction-snapshot` | Transaction Snapshot |

---

## 5. Dependencies

This module requires the host system to provide:

| Dependency | Type | Access | Description |
|-----------|------|--------|-------------|
| accounting_system | sql_connection | read_only | Read-only access to Opera SQL SE database — snapshots all tables |

### Config Adaptation

The module reads its connections from the host system. You need to ensure these are available:

- **SQL Connection**: The module calls `sql_connector` (passed per-request from the host app). Your system must provide a SQL connector to the Opera database and make it available to the module's routes.
- **Email** (if needed): IMAP/SMTP settings for receiving and sending emails.
- **Company Context**: The module is multi-company. It expects a company ID per request to scope its data.

The module does NOT have its own connection settings — it uses whatever the host system provides.

---

## 6. Settings

No local settings — reads directly from Opera database.

---

## 7. Data Files

These local databases/files need to be in the module's data directory:

| File | Type | Scope | Description |
|------|------|-------|-------------|
| `opera-transaction-library/` | json_directory | global | Captured transaction snapshots as JSON files |

For migration: copy these from the old system's `data/{company}/` folder.

---

## 8. Locking Rules

These locking patterns are built into the module code. The host database must support them:

| Rule | Implementation |
|------|----------------|
| **Read Only** | All queries use WITH (NOLOCK) — no write operations |
| **Snapshot Storage** | Local JSON files — no database locking needed |

---

## 9. API Endpoints

The module exposes these endpoints:

- `/api/transaction-snapshot/before`
- `/api/transaction-snapshot/after`
- `/api/transaction-snapshot/library`
- `/api/transaction-snapshot/library/*`
- `/api/transaction-snapshot/field-analysis`
- `/api/transaction-snapshot/relationship-analysis`
- `/api/transaction-snapshot/export-to-knowledge`

---

## Migration Checklist

- [ ] Copy `apps/transaction_snapshot/` to the new system
- [ ] Copy required `sql_rag/` shared libraries
- [ ] Copy frontend page(s)
- [ ] Register API router in main app
- [ ] Add React routes
- [ ] Add menu items
- [ ] Provide SQL connection to Opera
- [ ] Copy data files from old system
- [ ] Test each endpoint and page
- [ ] Verify locking works correctly
