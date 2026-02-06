# Claude Code Project Instructions

## Project Overview

This is **SQL RAG** - a financial management application that integrates with **Pegasus Opera SQL SE**, an accounting system. The application provides:

- Dashboard and reporting for financial data
- Bank statement import and processing
- Ledger reconciliation (Debtors/Creditors)
- AI-assisted natural language queries against the database

## Key Documentation

**Read these before working on Opera-related tasks:**

- `docs/opera_knowledge_base.md` - Comprehensive guide to Opera database tables, field conventions, amount storage (pence vs pounds), reference formats, and integration patterns. **Includes:**
  - Cashbook transaction types (atype table, at_type values 1-6)
  - anoml transfer file patterns for all transaction types
  - Unique ID generation format (`_XXXXXXXXX` base-36)
  - Period posting rules and control accounts

## Architecture

- **Backend**: Python FastAPI (`api/main.py`)
- **Frontend**: React + TypeScript (`frontend/`)
- **Database**: SQL Server (Opera SQL SE)
- **Opera 3 Data**: FoxPro DBF files (`sql_rag/opera3_foxpro.py`)
- **Import Logic**: `sql_rag/bank_import.py`, `sql_rag/opera_sql_import.py`

## Data Sources

### Opera SQL SE (Primary)
- SQL Server database
- Modern version with SQL queries

### Opera 3 (Legacy)
- Location: `C:\Apps\O3 Server VFP`
- Format: Visual FoxPro DBF files
- Reader: `sql_rag/opera3_foxpro.py`
- Same table structures (pname, ptran, stran, etc.)

## Critical Conventions

### Opera Database

1. **Amount Storage**:
   - `aentry`/`atran` tables: amounts in **PENCE**
   - `ntran`/`ptran`/`stran` tables: amounts in **POUNDS**

2. **Payment Signs in atran**:
   - Payments = NEGATIVE values
   - Receipts = POSITIVE values

3. **nt_trnref Format** (Nominal Ledger):
   - First 30 chars: Supplier/Customer name
   - Use this for matching NL entries to PL/SL

4. **Locking Concerns**:
   - Direct database writes bypass Opera's application-level locking
   - Prefer Opera COM automation or standard import files for production
   - See `sql_rag/opera_com.py` for COM interface (requires Windows)

### Control Accounts
Control account codes vary by installation. They are loaded dynamically from Opera configuration:
- **Primary source**: `sprfls` table (`sc_dbtctrl` for debtors, `pc_crdctrl` for creditors)
- **Fallback**: `nparm` table (`np_dca` for debtors, `np_cca` for creditors)
- **Config module**: `sql_rag/opera_config.py` - use `get_control_accounts()` to retrieve

## Common Tasks

### Bank Statement Import
- Entry point: `sql_rag/bank_import.py`
- Creates entries in: `aentry`, `atran`, `ptran`, `ntran`, `palloc`
- Duplicate detection uses `ABS(ABS(at_value) - amount)` pattern

### Variance Analysis (Reconciliation)
- Located in `api/main.py` around line 4450+
- Matches PL to NL using: date+value+supplier, date+value, value+supplier
- Extracts supplier name from `nt_trnref` first 30 chars

## Testing

- API runs on port 8000
- Use virtual environment: `source venv/bin/activate`
- Start API: `uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`

## Development Guidelines

### NEVER Modify Opera Database Structure
**CRITICAL**: Never create tables, alter tables, or modify the schema of Opera SE or Opera 3 databases. This applies to BOTH:
- **Opera SQL SE** - SQL Server database
- **Opera 3** - FoxPro DBF files

These are third-party accounting systems and modifying their structure:
- Could break Opera upgrades
- Could cause support issues with Pegasus
- Could cause data integrity issues

All custom data storage (aliases, monitoring logs, caches) must use:
- Local SQLite databases (e.g., `bank_aliases.db`, `lock_monitor.db`)
- Separate application databases
- NEVER the Opera database itself

### Read-Only Utilities by Default
**When building utilities or diagnostic tools**, do NOT modify any tables in Opera 3 or Opera SQL SE unless explicitly specified by the user. Utilities should:
- Take snapshots for comparison (read-only)
- Generate reports and analysis
- Query data without modification
- Only write to Opera when explicitly requested for import/posting operations

### Multi-User Locking Requirements
**CRITICAL**: Always apply optimal locking when writing to Opera databases to ensure other users in the system do not experience locks:

1. **Short Transaction Windows**: Keep database transactions as brief as possible
2. **Row-Level Locking**: Use row-level locks instead of table locks where possible
3. **Lock Order Consistency**: Always acquire locks in a consistent order to prevent deadlocks
4. **Immediate Commit**: Commit transactions immediately after completing the operation
5. **No Long-Running Transactions**: Never hold a transaction open while waiting for user input or external operations
6. **Error Handling**: Always release locks in finally blocks to ensure cleanup on errors

**For SQL Server (Opera SQL SE)**:
```sql
-- Use READ COMMITTED isolation (default) for normal operations
-- Use ROWLOCK hint when updating specific rows
UPDATE table WITH (ROWLOCK) SET ... WHERE key = value
```

**For FoxPro (Opera 3)**:
- Use `FLOCK()` briefly, release immediately after write
- Prefer record-level locking with `RLOCK()` over table locking
- Always `UNLOCK` in finally/cleanup code

### Dual Data Source Support
**Important**: Any changes to Opera utilities must be applied to BOTH Opera SQL SE and Opera 3 versions:
- SQL SE: `sql_rag/bank_import.py`, `sql_rag/opera_sql_import.py`
- Opera 3: `sql_rag/bank_import_opera3.py`, `sql_rag/opera3_foxpro.py`
- Data providers: `sql_rag/opera_sql_provider.py`, `sql_rag/opera3_data_provider.py`
- API endpoints: `/api/opera-sql/...` and `/api/opera3/...`
- Frontend: Include data source toggle where applicable

### Backend-First Logic
**All business logic and validation MUST be implemented in the backend (API layer)**:
- **Validation**: All input validation must happen in the backend. Frontend validation is optional UX enhancement only.
- **Business Rules**: All business logic (calculations, transformations, conditions) must be in Python/FastAPI, not JavaScript/React.
- **Security**: Never trust frontend data - always validate and sanitize on the backend.
- **Error Handling**: Backend must return clear, specific error messages that the frontend can display.
- **Data Processing**: All data aggregation, filtering, and transformation should happen in the API, not the frontend.

**Why**:
- Frontend can be bypassed (API calls directly)
- Ensures consistency across all clients
- Single source of truth for business rules
- Easier to test and maintain

**Frontend Role**:
- Display data from API
- Collect user input and send to API
- Show loading states and error messages from API
- Optional: UX-only validation hints (but backend validates too)

## Git Workflow

- Main branch: `main`
- Remote: GitHub (`HarryBurdett/llmragsql`)
- Always commit with descriptive messages
