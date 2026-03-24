# Application Restructure Plan

## Overview

Split the monolithic `api/main.py` (44,732 lines, 537 endpoints) into independent application modules, each self-contained and deployable into a future core system.

## Target Structure

```
apps/
├── core/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes_auth.py          → login, logout, me, preferences, modules
│   │   ├── routes_users.py         → user CRUD, permissions
│   │   ├── routes_companies.py     → list, switch, current, discover
│   │   ├── routes_licences.py      → licence CRUD, session licence
│   │   ├── routes_installations.py → system profiles, apply/switch
│   │   ├── routes_settings.py      → global settings, LLM config, database config
│   │   ├── routes_email.py         → providers, sync, messages, attachments, send
│   │   └── middleware.py           → auth middleware, company context
│   ├── logic/
│   │   ├── __init__.py
│   │   ├── company_data.py         → moved from sql_rag/company_data.py
│   │   ├── user_auth.py            → moved from sql_rag/user_auth.py
│   │   ├── sql_connector.py        → moved from sql_rag/sql_connector.py
│   │   ├── opera_config.py         → moved from sql_rag/opera_config.py
│   │   ├── opera3_config.py        → moved from sql_rag/opera3_config.py
│   │   ├── opera3_foxpro.py        → moved from sql_rag/opera3_foxpro.py
│   │   └── email/                  → moved from api/email/
│   ├── models/
│   │   └── schemas.py              → shared Pydantic models
│   └── frontend/
│       └── (core UI: Login, Settings, Users, Company, Installations)
│
├── bank_reconcile/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes_import.py        → scan, preview, import (SE + Opera 3)
│   │   ├── routes_reconcile.py     → match, confirm, complete, unreconciled
│   │   ├── routes_cashbook.py      → auto-match, create entry, bank accounts
│   │   ├── routes_manage.py        → archive, restore, drafts, statement files
│   │   └── routes_aliases.py       → bank alias CRUD
│   ├── logic/
│   │   ├── __init__.py
│   │   ├── bank_import.py          → moved from sql_rag/bank_import.py
│   │   ├── bank_import_opera3.py   → moved from sql_rag/bank_import_opera3.py
│   │   ├── statement_reconcile.py  → moved from sql_rag/statement_reconcile.py
│   │   ├── statement_reconcile_opera3.py
│   │   ├── bank_patterns.py        → moved from sql_rag/bank_patterns.py
│   │   ├── bank_aliases.py         → moved from sql_rag/bank_aliases.py
│   │   ├── bank_duplicates.py      → moved from sql_rag/bank_duplicates.py
│   │   ├── opera_sql_import.py     → moved from sql_rag/opera_sql_import.py
│   │   ├── opera3_foxpro_import.py → moved from sql_rag/opera3_foxpro_import.py
│   │   └── pdf_extraction_cache.py → moved from sql_rag/pdf_extraction_cache.py
│   └── frontend/
│       └── (BankStatementHub, BankStatementReconcile, Imports, CashbookOptions)
│
├── gocardless/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes_import.py        → scan, preview, import, history
│   │   ├── routes_requests.py      → payment requests, bulk, cancel, sync
│   │   ├── routes_mandates.py      → list, link, setup, sync
│   │   ├── routes_subscriptions.py → CRUD, pause, resume, cancel, sync
│   │   ├── routes_settings.py      → GC-specific settings, partner config
│   │   └── routes_partner.py       → partner signup, callback, merchant
│   ├── logic/
│   │   ├── __init__.py
│   │   ├── gocardless_api.py       → moved from sql_rag/gocardless_api.py
│   │   └── gocardless_payments.py  → moved from sql_rag/gocardless_payments.py
│   └── frontend/
│       └── (GoCardlessImport, GoCardlessRequests, GoCardlessSettings, etc.)
│
├── suppliers/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes_statements.py    → dashboard, extract, reconcile, approve
│   │   ├── routes_queries.py       → query management, auto-resolve
│   │   ├── routes_comms.py         → communications log
│   │   ├── routes_security.py      → alerts, approved senders, audit
│   │   ├── routes_directory.py     → supplier directory, account views
│   │   └── routes_settings.py      → supplier-specific config
│   ├── logic/
│   │   ├── __init__.py
│   │   ├── supplier_statement_extract.py
│   │   ├── supplier_statement_reconcile.py
│   │   ├── supplier_statement_db.py
│   │   └── procurement_provider.py → stub interface for future procurement API
│   └── frontend/
│       └── (SupplierDashboard, Queue, Reconciliations, Queries, etc.)
│
├── balance_check/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py               → debtors, creditors, cashbook, VAT, trial balance
│   ├── logic/
│   │   └── __init__.py
│   └── frontend/
│       └── (ReconcileSummary, DebtorsReconcile, CreditorsReconcile, etc.)
│
├── dashboards/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py               → finance, revenue, credit control, executive
│   ├── logic/
│   │   └── __init__.py
│   └── frontend/
│       └── (Home dashboard components)
│
├── pension_export/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes.py               → pension schemes, NEST, contributions
│   ├── logic/
│   │   └── __init__.py
│   └── frontend/
│       └── (PensionExport page)
│
└── lock_monitor/
    ├── __init__.py
    ├── api/
    │   ├── __init__.py
    │   └── routes.py               → SQL Server + Opera 3 lock monitoring
    ├── logic/
    │   └── __init__.py
    └── frontend/
        └── (LockMonitor page)
```

## New main.py (slim orchestrator)

After restructure, `api/main.py` becomes ~200 lines:

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Core
from apps.core.api.middleware import auth_middleware, ensure_company_context
from apps.core.api.routes_auth import router as auth_router
from apps.core.api.routes_users import router as users_router
from apps.core.api.routes_companies import router as companies_router
# ... other core routers

# Apps
from apps.bank_reconcile.api.routes_import import router as bank_import_router
from apps.bank_reconcile.api.routes_reconcile import router as bank_rec_router
from apps.gocardless.api.routes_import import router as gc_import_router
from apps.suppliers.api.routes_statements import router as supplier_router
# ... other app routers

app = FastAPI(title="SQL RAG API", lifespan=lifespan)

# Middleware
app.add_middleware(CORSMiddleware, ...)
app.middleware("http")(auth_middleware)

# Register core
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(companies_router)

# Register apps
app.include_router(bank_import_router)
app.include_router(bank_rec_router)
app.include_router(gc_import_router)
app.include_router(supplier_router)
# ...
```

## Migration Order

Each step is a git commit. Test after every step.

### Phase 1: Foundation
1. Create `apps/` directory structure with `__init__.py` files
2. Create `apps/core/` with shared utilities (company_data, connectors, auth)
3. Create slim `api/main_new.py` that imports from core — verify it starts
4. Move middleware and lifespan to core

### Phase 2: Smallest Apps First (prove the pattern)
5. **lock_monitor** (25 endpoints) → `apps/lock_monitor/api/routes.py`
6. **pension_export** (13 endpoints) → `apps/pension_export/api/routes.py`
7. **balance_check** (16 endpoints) → `apps/balance_check/api/routes.py`

### Phase 3: Medium Apps
8. **dashboards** (30 endpoints) → `apps/dashboards/api/routes.py`
9. **suppliers** (40 endpoints) → `apps/suppliers/api/routes.py`

### Phase 4: Large Apps (most critical — do last)
10. **gocardless** (116 endpoints) → split into 6 route files
11. **bank_reconcile** (125 endpoints) → split into 5 route files

### Phase 5: Cleanup
12. Archive old code (stock, SOP, POP, BOM, projects)
13. Remove original `api/main.py` (replaced by slim version)
14. Move frontend pages to app folders
15. Update all imports and verify

## Shared State & Globals

Currently `main.py` has module-level globals:
- `sql_connector`, `email_storage` — set by `_ensure_company_context()`
- `config`, `vector_db`, `llm`, `user_auth`
- `email_sync_manager`, `customer_linker`
- `_company_sql_connectors`, `_company_email_storages`

These move to `apps/core/` as a shared state module:
```python
# apps/core/state.py
sql_connector = None
email_storage = None
config = None
# etc.
```

All apps import from `apps.core.state` instead of module-level globals.

## Risk Mitigation

1. **No deletion until verified** — old code stays until new code is proven
2. **One app at a time** — each move is a separate commit
3. **Import compatibility** — old `sql_rag/` paths kept as re-exports during migration
4. **Endpoint testing** — after each move, verify endpoints respond via curl
5. **Frontend unchanged initially** — frontend moves are a separate phase
6. **Rollback plan** — each commit can be reverted independently

## What Does NOT Change

- Database schemas (Opera, SQLite)
- API endpoint URLs (same paths, just different source files)
- Frontend routing
- Company context mechanism
- Authentication flow
- Opera SE / Opera 3 parity

## Estimated Effort

| Phase | Endpoints | Complexity | Estimate |
|-------|-----------|------------|----------|
| Foundation | 0 | High (architecture) | 1 session |
| Small apps | 54 | Low | 1 session |
| Medium apps | 70 | Medium | 1 session |
| Large apps | 241 | High (dependencies) | 2 sessions |
| Cleanup | 0 | Medium | 1 session |
| Frontend | 0 | Low (just moves) | 1 session |

Total: ~6 sessions
