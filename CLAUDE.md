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

When posting **Bank Transfers** (at_type=8):
- `aentry` (2 records) - One per bank, opposite signs (pence)
- `atran` (2 records) - at_type=8, at_account=counterpart bank, shared at_unique
- `anoml` (2 records) - ax_source='A' (Admin), values in pounds, shared ax_unique
- `ntran` (2 records) - Opposite signs (pounds), nt_posttyp='T'
- `nbank` - Update both bank balances
- `nacnt` - Update both bank nominal account balances

Use `OperaSQLImport.update_nacnt_balance()` helper after every ntran INSERT.
Use `OperaSQLImport.update_nbank_balance()` helper for cashbook bank account postings:
- Receipts (sales receipt, purchase refund): +amount increases bank balance
- Payments (purchase payment, sales refund): -amount decreases bank balance

### VAT Tracking (MANDATORY for VAT Returns)
**CRITICAL**: Any transaction with VAT MUST create both `zvtran` AND `nvat` records for VAT returns to be accurate.

When posting transactions **with VAT**, you MUST also create:
1. **zvtran** - VAT analysis record (for VAT reporting)
2. **nvat** - VAT return tracking record

**nvat.nv_vattype values:**
- `'S'` = Sales/Output VAT (payable TO HMRC) - used for sales invoices
- `'P'` = Purchase/Input VAT (reclaimable FROM HMRC) - used for purchase invoices, fees

**Transactions requiring VAT tracking:**
| Transaction Type | nv_vattype | Example |
|------------------|------------|---------|
| Sales Invoice | 'S' | Customer invoice with VAT |
| Purchase Invoice | 'P' | Supplier invoice with VAT |
| GoCardless Fees | 'P' | Fees charged with VAT |
| Any expense with VAT | 'P' | Reclaimable input VAT |

**Transactions NOT requiring VAT tracking:**
- Sales receipts/refunds (VAT was on the original invoice)
- Purchase payments/refunds (VAT was on the original invoice)
- Bank transfers (no VAT involved)
- Nominal journals (unless specifically VAT-related)

### Opera Posting Checklist (MANDATORY)
**Before marking any posting code as complete, verify ALL of the following:**

```
□ Amounts in correct units (aentry/atran=PENCE, ntran/anoml=POUNDS)
□ Correct signs (receipts=positive, payments=negative in cashbook)
□ All related tables updated (see transaction type above)
□ nacnt balances updated via update_nacnt_balance()
□ nbank balances updated via update_nbank_balance() (if cashbook)
□ Customer/supplier balances updated (sname.sn_currbal / pname.pn_currbal)
□ Transfer files created (anoml/snoml/pnoml) with correct ax_done flag
□ ae_complet flag set correctly (1 only if posted to nominal)
□ VAT tracking: zvtran AND nvat created (if transaction has VAT)
□ Unique IDs generated correctly (shared where Opera shares them)
□ Period/year set correctly from posting date
□ Double-entry balanced (debits = credits in nominal)
```

**If ANY item is missing, the posting is INCOMPLETE and will cause:**
- Control account mismatches
- VAT return errors
- Audit failures
- Balance discrepancies

### Dual Data Source Support
**Important**: Any changes to Opera utilities must be applied to BOTH Opera SQL SE and Opera 3 versions:
- SQL SE: `sql_rag/bank_import.py`, `sql_rag/opera_sql_import.py`
- Opera 3: `sql_rag/bank_import_opera3.py`, `sql_rag/opera3_foxpro.py`
- Data providers: `sql_rag/opera_sql_provider.py`, `sql_rag/opera3_data_provider.py`
- API endpoints: `/api/opera-sql/...` and `/api/opera3/...`
- Frontend: Include data source toggle where applicable

**WARNING - Opera 3 (FoxPro) Implementation Gaps:**
The Opera 3 implementation in `sql_rag/opera3_foxpro_import.py` is INCOMPLETE and does NOT follow full Opera posting rules:

| Feature | SQL SE | Opera 3 | Action Required |
|---------|--------|---------|-----------------|
| Sales Receipt | ✓ Complete | ✓ Basic | Add nacnt/nbank updates |
| Purchase Payment | ✓ Complete | ✓ Basic | Add nacnt/nbank updates |
| GoCardless Batch | ✓ Complete | ✗ Incomplete | Missing: fees, ntran, zvtran, nvat |
| Bank Transfer | ✓ Complete | ✗ Missing | Not implemented |
| Sales Invoice | ✓ Complete | ✗ Missing | Not implemented |
| Purchase Invoice | ✓ Complete | ✗ Missing | Not implemented |
| VAT Tracking | ✓ zvtran + nvat | ✗ Missing | Not implemented |

**Before using Opera 3 import methods in production:**
1. Verify the specific method follows full Opera posting rules
2. Check all required tables are updated (see checklist above)
3. Test against Opera 3 to confirm data integrity
4. Consider using SQL SE version where possible for complete postings

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

### Error Message Clarity
**All error messages must be clear, actionable, and user-friendly**:
- **Plain Language**: Avoid technical jargon where possible. Users should understand what went wrong.
- **Actionable**: Tell users what they can do to resolve the issue (e.g., "Please wait 1-2 minutes and try again" for rate limits).
- **Inline Display**: Show errors inline on the page, not just as alerts. Use colored boxes (red for errors, amber for warnings).
- **Dismissible**: Error displays should have a close/dismiss button.
- **Rate Limits**: For API rate limit errors (429), explicitly tell users to wait and retry. Detect "429", "Resource exhausted", "rate limit" in error messages.
- **Categorize Errors**:
  - **User errors**: Invalid input, missing required fields - explain what's wrong
  - **System errors**: API failures, database issues - explain it's a temporary issue
  - **External API errors**: Third-party service issues (Gemini, etc.) - explain the service is temporarily unavailable

**Example error display pattern (React/Tailwind)**:
```tsx
{error && (
  <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
    <span className="text-red-500">⚠</span>
    <div className="flex-1">
      <p className="text-sm text-red-800 font-medium">Error Title</p>
      <p className="text-sm text-red-700">{error}</p>
    </div>
    <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600">×</button>
  </div>
)}
```

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
- index.html (menu page)
- dashboard-demo.html
- cashbook-reconcile-demo.html (includes bank statement import)
- gocardless-import-demo.html
- ap-automation-demo.html
- balance-check-demo.html

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

**Bank Reconciliation** (`cashbook-reconcile-demo.html`):
- **Complete workflow**: Import statement → Post missing entries → Reconcile
- AI extracts transactions from bank statement PDF
- Auto-matches statement lines to Opera cashbook entries
- **Unmatched receipts/payments are posted seamlessly** - no separate data entry needed
- Bank references preserved so Opera mirrors bank statement exactly
- Tick matched entries to reconcile, difference should reach zero
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
