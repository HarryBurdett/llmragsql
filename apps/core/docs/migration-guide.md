# App Migration Guide

## Overview

Each app under `apps/` is designed to be independently deployable into a new core system. This guide defines how to migrate an app, what it owns, and how it connects to shared system services.

## Architecture Principle

```
Core System (provides)          App (consumes)
─────────────────────          ────────────────
SQL connection          ──►    sql_connector
Email service           ──►    email_storage
Company context         ──►    current_company
User authentication     ──►    user_auth
Configuration           ──►    config

App owns its own:
  - API routes
  - Business logic
  - Frontend pages
  - Per-company data (data/{company}/{app}/)
  - App-specific settings
```

## What Each App Owns

Everything inside its directory is self-contained and migrates as a unit:

```
apps/{app_name}/
  api/
    routes.py            → FastAPI router (self-registering)
    routes_*.py          → Additional route files
  logic/                 → Business logic (future: moved from sql_rag/)
  docs/
    spec.md              → Specification
    manual.md            → User instruction manual
  frontend/              → React pages (future: moved from frontend/src/pages/)

data/{company}/{app_name}/
  *.db                   → SQLite databases (per-company)
  *.json                 → Configuration files (per-company)
```

## What Core Provides

System-wide services that apps depend on but do NOT own:

| Service | Current Location | Core Interface |
|---------|-----------------|----------------|
| SQL connection to Opera | `config.ini` [database] | `sql_connector` via `_ensure_company_context()` |
| Email IMAP/SMTP | `config.ini` [email] + `email_data.db` | `email_storage` + `email_sync_manager` |
| Company switching | `companies/*.json` | `current_company` + `_ensure_company_context()` |
| User auth & sessions | `users.db` (shared, root level) | `user_auth` |
| Opera config reader | `sql_rag/opera_config.py` | `get_period_posting_decision()`, `get_control_accounts()`, etc. |
| Opera 3 FoxPro reader | `sql_rag/opera3_foxpro.py` | `Opera3Reader(data_path)` |
| Opera SQL importer | `sql_rag/opera_sql_import.py` | `OperaSQLImport(sql_connector)` |
| Opera 3 write agent | `opera3_agent/` | `get_opera3_writer(data_path)` |

## Migration Steps (Per App)

### Step 1: Verify Self-Containment

Before migrating, confirm the app has NO dependencies outside its structure except core services:

```
Check:
□ All API routes are in apps/{app}/api/
□ All app-specific data is in data/{company}/{app}/
□ App settings are stored per-company in the app's data folder
□ No hardcoded paths to other apps
□ No imports from other apps (apps import from core only, NEVER from each other)
□ Frontend pages reference only /api/ endpoints (not internal module paths)
```

### Step 2: Identify Core Dependencies

List every core service the app uses. This becomes the app's **dependency manifest**:

#### bank_reconcile
```yaml
dependencies:
  - sql_connector          # Query Opera cashbook, ptran, stran, etc.
  - email_storage          # Email scanning for bank statements
  - email_sync_manager     # IMAP provider access
  - config                 # System configuration
  - current_company        # Company context
  - opera_sql_import       # Post transactions to Opera SE
  - opera3_foxpro_import   # Post transactions to Opera 3
  - opera_config           # Period posting, control accounts
  - import_lock            # Bank-level locking
```

#### gocardless
```yaml
dependencies:
  - sql_connector          # Query Opera for customers, invoices
  - email_storage          # Email scanning for GC notifications
  - config                 # System configuration
  - current_company        # Company context
  - customer_linker        # Match GC names to Opera customers
  - opera_sql_import       # Post receipts to Opera SE
  - opera3_foxpro_import   # Post receipts to Opera 3
  - opera_config           # Period posting, control accounts
```

#### suppliers
```yaml
dependencies:
  - sql_connector          # Query Opera PL (ptran, pname, zcontacts)
  - email_storage          # Email scanning for supplier statements
  - config                 # System configuration
  - current_company        # Company context
  - opera_config           # Period info for aging
```

#### balance_check
```yaml
dependencies:
  - sql_connector          # Query Opera NL, SL, PL for reconciliation
```

#### dashboards
```yaml
dependencies:
  - sql_connector          # Query Opera for KPIs, revenue, etc.
```

#### lock_monitor
```yaml
dependencies:
  - sql_connector          # Monitor SQL Server locks
```

#### pension_export
```yaml
dependencies:
  - sql_connector          # Query Opera payroll tables
  - config                 # Export folder paths
```

### Step 3: Register in New System

In the target system, the app registers itself:

```python
# Each app provides a register() function
from apps.bank_reconcile.api.routes import router as bank_rec_router

# The new system's main.py:
app.include_router(bank_rec_router)

# Core provides services via dependency injection or globals sync:
from apps.core.state import _sync_from_main
# OR via a service locator:
from apps.core.services import get_sql_connector, get_email_storage
```

### Step 4: Migrate Data

Per-company data moves with the app:

```bash
# Source system
data/{company}/bank_reconcile/
  bank_aliases.db
  bank_patterns.db
  import_locks.db
  pdf_extraction_cache.db

# Target system — same structure
data/{company}/bank_reconcile/
  bank_aliases.db
  bank_patterns.db
  import_locks.db
  pdf_extraction_cache.db
```

The `get_company_db_path()` function handles path resolution automatically based on company context. No path changes needed.

### Step 5: Migrate Frontend

React pages move from `frontend/src/pages/` to the app's frontend folder:

```bash
# Future structure (not yet moved):
apps/bank_reconcile/frontend/
  BankStatementHub.tsx
  BankStatementReconcile.tsx
  Imports.tsx
  CashbookOptions.tsx
```

Routes are defined in the app's manifest and registered by the core system's router.

### Step 6: Verify

After migration, test:

```
□ All API endpoints return 200
□ Multi-company switching works
□ Data isolation between companies verified
□ Frontend pages load and function
□ Opera SE endpoints work
□ Opera 3 endpoints work (if applicable)
□ Settings persist across restarts
□ No references to other apps (only core)
```

## System-Wide Settings Migration

### What Migrates with Core (NOT with any app)

| Setting | File | Notes |
|---------|------|-------|
| SQL Server connection | `config.ini` [database] | Server, database, credentials |
| Email server config | `config.ini` [email] | IMAP/SMTP server, port, credentials |
| LLM configuration | `config.ini` [llm] | AI provider, API keys |
| Company definitions | `companies/*.json` | Company ID, database name, Opera version |
| System profiles | `systems.json` | Installation profiles |
| User accounts | `users.db` | Shared across all apps |
| Table mapping | `table_mapping.json` | Opera table definitions |

### What Migrates with Each App

| App | Settings | Location |
|-----|----------|----------|
| bank_reconcile | Bank folders, archive paths | `data/{co}/bank_reconcile/` + `data/{co}/core/company_settings.json` |
| gocardless | API token, bank code, fees config | `data/{co}/gocardless/gocardless_settings.json` |
| suppliers | Automation params, timing, thresholds | `data/{co}/suppliers/supplier_statements.db` (config table) |
| lock_monitor | Monitor settings | `data/{co}/lock_monitor/lock_monitor.db` |

### Migration Checklist for Core Settings

When setting up a new installation:

```
□ Copy config.ini (update server/database for target environment)
□ Copy companies/*.json (update database names if different)
□ Copy systems.json
□ Copy users.db (or create fresh users)
□ Verify SQL connection works
□ Verify email connection works (if email apps are installed)
□ Run initial company context to create data directories
```

## Rules

1. **Apps import from core — NEVER from each other**
2. **Core provides services — apps consume them**
3. **Each app's data is self-contained in data/{company}/{app}/**
4. **System-wide config stays with core**
5. **App-specific config stays with the app**
6. **No hardcoded paths, account codes, or company-specific values**
7. **Both Opera SE and Opera 3 must work**
