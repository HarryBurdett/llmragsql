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

### Complete Data Updates (CRITICAL for Finance)
**CRITICAL**: This is a finance system - ALL related tables must be updated correctly when posting transactions. Incomplete updates cause control account mismatches and audit failures.

When posting to **Nominal Ledger (ntran)**, you MUST also update:
1. **nacnt** (Nominal Account Balances):
   - `na_ptddr/na_ptdcr` - Period to date debit/credit
   - `na_ytddr/na_ytdcr` - Year to date debit/credit
   - `na_balc{period}` - Period balance (na_balc01 for Jan, na_balc02 for Feb, etc.)

   Update pattern:
   - DEBIT (positive nt_value): `na_ptddr += value`, `na_ytddr += value`, `na_balc{period} += value`
   - CREDIT (negative nt_value): `na_ptdcr += ABS(value)`, `na_ytdcr += ABS(value)`, `na_balc{period} += value`

2. **ae_complet flag**: Only set to 1 if ntran entries are created (post_to_nominal=True)

When posting **Sales Ledger transactions**:
- `stran` - Transaction record
- `snoml` - Transfer file (sx_done='Y' when posted to NL)
- `ntran` + `nacnt` - Nominal entries and balances
- `sname.sn_currbal` - Customer balance

When posting **Purchase Ledger transactions**:
- `ptran` - Transaction record
- `pnoml` - Transfer file (px_done='Y' when posted to NL)
- `ntran` + `nacnt` - Nominal entries and balances
- `pname.pn_currbal` - Supplier balance

When posting **Cashbook transactions**:
- `aentry` + `atran` - Cashbook header and detail
- `anoml` - Transfer file (ax_done='Y' when posted to NL)
- `ntran` + `nacnt` - Nominal entries and balances
- `nbank.nk_curbal` - Bank current balance (in pence)
- `stran`/`ptran` - Sales/Purchase ledger if allocating

Use `OperaSQLImport.update_nacnt_balance()` helper after every ntran INSERT.
Use `OperaSQLImport.update_nbank_balance()` helper for cashbook bank account postings:
- Receipts (sales receipt, purchase refund): +amount increases bank balance
- Payments (purchase payment, sales refund): -amount decreases bank balance

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

## Email Configuration

**Demo Recipient (always send demos to):**
- charlieb@intsysuk.com

**User Contact:**
- Charlie Burdett: charlieb@intsysuk.com

**IMPORTANT - Always use these settings when sending emails:**
- **From**: `intsys@wimbledoncloud.net` (required for external relay)
- **To**: `charlieb@intsysuk.com`

**Mail Server (configured):**
- Server: 10.10.100.12
- Login: intsys@aoc.local
- Type: IMAP for receiving, SMTP port 587 for sending

**API endpoint**: `POST /api/email/send`
```json
{
  "to": "charlieb@intsysuk.com",
  "subject": "Subject here",
  "body": "<html>...</html>",
  "from_email": "intsys@wimbledoncloud.net",
  "attachments": ["/path/to/file.html"]
}
```

**Demo files location**: `/Users/maccb/llmragsql/demos/`
- bank-statement-import-demo.html
- gocardless-import-demo.html
- ap-automation-demo.html
- cashbook-reconcile-demo.html\n- balance-check-demo.html

## Feature Demos

### Demo Settings
- **Default timing**: 20 seconds per slide
- **Voice narration**: ON by default (Web Speech API)
- **Voice pause**: 800ms between title and description
- **Duration selector**: User can choose 5s/10s/15s/20s/30s
- **Opera logos**: Embedded as base64 for self-contained HTML files

### Demo Key Messaging

**All demos should emphasize:**
- Automated email mailbox monitoring
- Automatic document identification and processing
- Zero-touch processing where possible
- All communications are automated

**Bank Statement Import** (`bank-statement-import-demo.html`):
- Transactions are matched to customers/suppliers then **posted directly into Opera**
- Eliminates manual data entry completely
- **Bank references are preserved** - Opera entries mirror the bank statement exactly
- Because entries match the bank, **subsequent bank reconciliation is automatic/seamless**
- Unmatched transactions can be assigned customer/supplier before posting
- System learns from manual assignments for future automation

**GoCardless Import** (`gocardless-import-demo.html`):
- Monitors mailbox for GoCardless payout notification emails
- Automatically extracts payment details from email body
- Matches payments to customers by invoice reference or name
- Posts as sales receipts to Opera

**AP Automation** (`ap-automation-demo.html`):
- Supplier statement reconciliation (supplier statement vs Purchase Ledger)
- AI-powered extraction from PDF/email statements
- Automatic matching against Purchase Ledger transactions
- Generates variance reports

**Cashbook Reconciliation** (`cashbook-reconcile-demo.html`):
- Bank statement reconciliation (tick off cashbook entries against bank statement)
- Enter statement date and closing balance
- View unreconciled cashbook entries, tick those on statement
- Manual or Auto-Match modes
- Post to mark as reconciled, difference should reach zero
- **Location**: Cashbook > Cashbook Reconcile

## Reconciliation Features (Important Distinction)

There are TWO different reconciliation concepts - don't confuse them:

### Bank Reconciliation (Cashbook > Cashbook Reconcile)
- **Purpose**: Reconcile bank statement against Opera cashbook entries
- **Component**: `BankStatementReconcile.tsx`
- **Route**: `/cashbook/statement-reconcile`
- **Process**: Enter statement date/balance, tick entries that appear on statement, post when difference = 0

### Balance Check (Utilities > Balance Check)
- **Purpose**: Check internal Opera balances agree (control account reconciliation)
- **Components**: `CashbookReconcile.tsx`, `DebtorsReconcile.tsx`, `CreditorsReconcile.tsx`
- **Route**: `/reconcile/cashbook`, `/reconcile/debtors`, `/reconcile/creditors`
- **Process**: Compares sub-ledger totals against Nominal Ledger control accounts
- **Cashbook Balance Check**: Compares Cashbook (atran) vs Bank Master (nbank) vs Nominal Ledger
- **Debtors Balance Check**: Compares Sales Ledger total vs Debtors Control Account
- **Creditors Balance Check**: Compares Purchase Ledger total vs Creditors Control Account

## Opera Product Logos

Logos are displayed in the SQL RAG UI header based on connected Opera version:
- **Opera SQL SE**: `frontend/public/opera-se-logo.png`
- **Opera 3**: `frontend/public/opera3-logo.png`
- Component: `frontend/src/components/OperaVersionBadge.tsx`

## Git Workflow

- Main branch: `main`
- Remote: GitHub (`HarryBurdett/llmragsql`)
- Always commit with descriptive messages
