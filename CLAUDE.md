# Claude Code Project Instructions

## Project Overview

This is **SQL RAG** - a financial management application that integrates with **Pegasus Opera SQL SE**, an accounting system. The application provides:

- Dashboard and reporting for financial data
- Bank statement import and processing
- Ledger reconciliation (Debtors/Creditors)
- AI-assisted natural language queries against the database

## Key Documentation

**Read these before working on Opera-related tasks:**

- `docs/opera_knowledge_base.md` - Comprehensive guide to Opera database tables, field conventions, amount storage (pence vs pounds), reference formats, and integration patterns

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

## Git Workflow

- Main branch: `main`
- Remote: GitHub (`HarryBurdett/llmragsql`)
- Always commit with descriptive messages
