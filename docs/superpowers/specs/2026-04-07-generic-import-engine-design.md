# Generic Import Engine — Design Spec

## Goal

Build a generic integration engine that connects any external application to Opera's tables. Any system that produces data — HR, payroll, ecommerce, stock management, CRM, expenses — should be connectable to Opera without bespoke development. Users map external data to Opera fields using reusable templates. Every import is fully validated with plain-English error messages and audited before anything is posted.

## Problem

Currently, each integration (GoCardless, bank statements, supplier statements) is a bespoke implementation. Businesses run dozens of systems that need to flow into Opera:

- **HR / Payroll** → salary journals, pension postings, employee master records into the Nominal Ledger
- **Ecommerce** (Shopify, WooCommerce, Amazon) → sales orders, stock adjustments, customer creation, invoice/receipt posting
- **Stock / Warehouse** → stock movements, counts, transfers, purchase orders
- **CRM** → new customer/supplier creation, contact updates
- **Expenses** (Dext, Concur) → purchase invoices, employee reimbursements
- **Ad-hoc spreadsheets** → bulk journal entries, opening balances, data corrections

Each of these currently requires developer work. The goal is: **define the mapping once, import forever**.

## Approach

**Template-based integration engine** with a clean separation:

```
Source (CSV / Excel / API / Email)
    ↓
Import Template (mapping + transforms + defaults + error handling)
    ↓
Intermediate Format (standardised rows with Opera field names)
    ↓
Target Adapter (validates and posts to destination system)
```

The template defines how to get data in. The adapter defines how to get data out. Everything in between is generic.

---

## 1. Transaction Type Registry

The snapshot tool captures exact field requirements for each Opera transaction type. This data becomes a machine-readable registry that drives the import engine.

For each transaction type the registry defines:

| Property | Example (Sales Receipt) |
|----------|------------------------|
| Tables written | aentry, atran, stran, ntran, anoml, nbank, nacnt, sname |
| Required fields | account, amount, date, bank_code, cbtype |
| Optional fields | reference, description, VAT code |
| Auto-generated fields | entry number, journal number, unique IDs, row IDs |
| Amount conventions | aentry/atran in pence, stran/ntran in pounds |
| Sign rules | atran positive for receipts, negative for payments |
| Sequence sources | atype for entry, nparm for journal, nextid for row IDs |
| Validation rules | Account must exist, not dormant, period open |

**Extensibility**: When a new transaction type is snapshotted AND has an existing import method, it becomes available in the import tool.

**Registry Entry Schema**

Each registry entry is hand-authored (informed by snapshot data) and defines the bridge between user-provided data and the import method:

```
{
  "type_id": "sales_receipt",
  "label": "Sales Receipt",
  "category": "Cashbook",
  "import_method": "import_sales_receipt",
  "user_fields": [
    {"name": "customer_account", "label": "Customer Code", "required": true, "lookup": "sname"},
    {"name": "amount_pounds", "label": "Amount (£)", "required": true, "type": "decimal"},
    {"name": "reference", "label": "Reference", "required": false, "type": "string", "max_length": 30},
    {"name": "post_date", "label": "Date", "required": true, "type": "date"}
  ],
  "default_fields": [
    {"name": "bank_account", "label": "Bank Account", "source": "setting_or_template"},
    {"name": "cbtype", "label": "Type Code", "source": "setting_or_template"},
    {"name": "input_by", "label": "Input By", "default": "IMPORT"}
  ],
  "auto_fields": ["entry_number", "journal_number", "unique_id", "row_id"],
  "duplicate_detection": {"match_on": ["customer_account", "amount_pounds", "post_date"], "date_range_days": 7},
  "validate_only_supported": true
}
```

Snapshots inform what fields matter. The registry codifies what the user provides, what the template defaults, and what the adapter auto-generates. The `import_method` name maps directly to an `OperaSQLImport` method — the adapter calls it with the assembled parameters.

**Available transaction types** (requires both snapshot AND import method):
- Cashbook: Sales Receipt, Purchase Payment, Nominal Entry, Sales Refund, Purchase Refund, Bank Transfer
- Sales Ledger: Invoice, Credit Note
- Purchase Ledger: Invoice, Credit Note
- Nominal: Journal Entry
- Stock: Adjustment, Transfer

Types without an existing import method (e.g. Payroll) are not available until one is built. The registry enforces this — no method, no entry.

### Conditional Field Requirements

Some fields are only mandatory depending on the target account's configuration in Opera. For example:

- **Department code**: mandatory for some nominal accounts (`nacnt` analysis settings), optional for others
- **Project code**: same — account-specific
- **VAT code**: required for purchase/sales invoices, not for bank transfers

These cannot be expressed as simple required/optional in the registry. Instead, the field is marked as `conditionally_required` and the adapter checks the actual Opera account at validation time:

```
{"name": "department_code", "label": "Department", "required": "conditional",
 "condition": "Check nacnt analysis settings for the target nominal account"}
```

The adapter resolves this during dry-run validation and returns a plain-English message:
"Row 3: Account 7100 (Motor Expenses) requires a department code — add a Department column or set a default in the template."

This is a key reason snapshots are essential — only by observing real transactions to different accounts can we discover which conditions apply.

### Composite Transaction Types

Some integrations require a chain of related Opera postings from a single source row. For example:

| Source | Opera Postings Required |
|--------|------------------------|
| Ecommerce order (Shopify) | Create customer (if new) → Sales order → Stock adjustment → Sales invoice → Receipt when paid |
| Payroll journal (Sage) | Multiple nominal journal lines (salary, NI, pension, PAYE) per employee |
| Expense claim (Dext) | Purchase invoice → Nominal posting → VAT record |
| Stock receipt (warehouse system) | Purchase order receipt → Stock adjustment → GRN |

A composite type is defined as an ordered sequence of registry entries:

```
{
  "type_id": "ecommerce_order",
  "label": "Ecommerce Order (Full Cycle)",
  "steps": [
    {"type": "customer_create", "condition": "if_not_exists"},
    {"type": "sales_invoice", "required": true},
    {"type": "stock_adjustment", "required": true},
    {"type": "sales_receipt", "condition": "if_paid"}
  ]
}
```

Each step uses the same row data but maps different fields. Conditions control which steps execute. The pipeline validates the entire chain before posting any step.

This is a Phase 2+ feature — Phase 1 handles single transaction types only. But the registry schema accommodates it from day one.

### Template Library Strategy

Pre-built templates are created by researching third-party system export formats:

- **Web research**: export file layouts, API documentation, CSV column schemas for common systems (Sage, Xero, Shopify, QuickBooks, WooCommerce, Dext, etc.)
- **Community knowledge**: common integration patterns documented by accountants and bookkeepers
- **User contributions**: successful custom templates can be promoted to the shared library

The library ships with the system. Users pick their source system from a list, upload their file, and import — no mapping required. For systems not in the library, the template builder handles it.

---

## 2. Import Template

A saved, reusable configuration that defines how external data maps to a transaction type.

### Template Properties

| Property | Description | Example |
|----------|-------------|---------|
| Name | Human-readable name | "Sage Payroll Monthly" |
| Target system | Which adapter to use | Opera SE |
| Transaction type | From the registry | Sales Receipt |
| Source type | CSV, Excel, API, Email | CSV |
| Column mappings | Source field → Opera field | Column D → amount |
| Defaults | Fixed values not in the source | bank_code = "CURR", cbtype = "CR" |
| Transformations | Data conversions | Date: DD/MM/YYYY → YYYY-MM-DD, Amount × 100 |
| Error handling | What to do on failure | abort_all / skip_failed / pause_and_ask |
| Duplicate handling | What to do on duplicates | skip / warn / block |
| Warning handling | What to do on warnings | auto_proceed / require_acknowledgement |
| Auto-approve | Skip manual approval (trusted sources) | false |

### Template Tiers

**Pre-built templates**: Shipped with the system for common sources (Sage, Xero, standard CSV formats). User uploads file and imports — no mapping needed.

**Custom templates**: Power user creates via the UI. Picks transaction type, maps columns, sets defaults, saves. Reusable from then on.

**AI-assisted mapping**: Optional. User uploads a sample file, AI suggests column mappings with confidence scores. User confirms or adjusts each suggestion. AI never auto-confirms.

### Transformations (Phase 1)

A closed set of named operations. No expression language — keep it simple.

| Transform | Description | Example |
|-----------|-------------|---------|
| `date_format` | Convert date string to YYYY-MM-DD | DD/MM/YYYY → 2026-04-15 |
| `multiply` | Scale a numeric value | × 100 (pounds to pence, though import methods handle this) |
| `truncate` | Limit string length | max 30 chars (Opera field limits) |
| `strip` | Remove whitespace | "  SMIT001  " → "SMIT001" |
| `uppercase` | Convert to uppercase | "smit001" → "SMIT001" |
| `pad` | Pad with leading characters | "123" → "000123" (6-digit account code) |
| `lookup` | Map source value to Opera code | "Smith & Sons" → "SMIT001" via sname fuzzy match |
| `default_if_empty` | Use a fixed value when source is blank | Empty → "MISC" |

### Storage

Templates stored per-company in SQLite (consistent with other modules). A library of common templates can be installed from a shared collection.

### Data Model

```sql
-- Import templates (reusable configurations)
CREATE TABLE import_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    source_type TEXT DEFAULT 'csv',
    target_system TEXT DEFAULT 'opera_se',
    mappings_json TEXT,          -- column mappings as JSON array
    defaults_json TEXT,          -- fixed default values as JSON object
    transforms_json TEXT,        -- transformations as JSON array
    error_handling TEXT DEFAULT 'stop_on_error',
    duplicate_handling TEXT DEFAULT 'warn',
    warning_handling TEXT DEFAULT 'require_acknowledgement',
    auto_approve INTEGER DEFAULT 0,
    created_by TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME
);

-- Import history (audit trail)
CREATE TABLE import_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER REFERENCES import_templates(id),
    template_name TEXT,
    transaction_type TEXT,
    source_filename TEXT,
    source_type TEXT,
    total_rows INTEGER,
    rows_posted INTEGER,
    rows_skipped INTEGER,
    rows_failed INTEGER,
    status TEXT,                 -- completed, partial, failed
    audit_report_json TEXT,     -- full validation report
    imported_by TEXT,
    imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

---

## 3. Processing Pipeline

Every import follows the same stages regardless of source. The user can stop, fix, and retry at any point.

### Stage 1 — Ingest & Parse

- Read source data (CSV, Excel, API payload, email attachment)
- Apply column mappings and transformations from the template
- Output: table of rows with Opera field names and values
- Errors caught: missing required columns, unparseable dates, non-numeric amounts

### Stage 2 — Preview

- Show the user what will be posted in plain English
- Each row described as a sentence, not raw field values
- Account codes resolved to names from Opera
- Example: "Row 1: Sales Receipt £1,240.00 from SMIT001 (Smith & Sons Ltd) to bank CURR, dated 15/04/2026"

### Stage 3 — Dry Run Validation

Every row checked against Opera. For each issue, report three things:

1. **What's wrong** — plain English
2. **Which row** — highlighted in the preview
3. **How to fix it** — specific, actionable suggestion

Example validation messages:

| Issue | Message | Suggestion |
|-------|---------|------------|
| Account not found | "Row 3: Account 'SMITH01' does not exist" | "Did you mean 'SMIT001' (Smith & Sons Ltd)?" |
| Dormant account | "Row 7: Account 'OLD001' is dormant" | "Reactivate in Opera or remove this row" |
| Period closed | "Rows 1-15: Period 3/2026 is closed" | "Change posting date to current period, or ask administrator to reopen" |
| Amount zero | "Row 9: Amount is £0.00" | "Check source data — zero-value transactions cannot be posted" |
| Duplicate detected | "Row 4: Matches existing transaction on 12/04 for £500.00" | "Remove if already posted, or continue if intentional" |
| Missing reference | "Row 6: No invoice reference" | "Add a reference in column C, or set a default in the template" |
| Double-entry imbalance | "Total debits £5,000 ≠ credits £4,800" | "Check row 12 — amount may be incorrect" |

Severity levels:
- **Error**: blocks posting. Must be fixed.
- **Warning**: allows posting with acknowledgement. User decides.
- **Info**: FYI only, no action needed.

### Stage 4 — Audit Report

Before posting:
- Summary: X rows ready, Y warnings, Z errors
- Full row-by-row detail available
- Report downloadable as PDF/CSV
- Logged permanently: who, when, which file, which template, which rows

### Stage 5 — Post

- Uses existing `OperaSQLImport` methods — no new posting code
- Each row posted within a transaction with deadlock retry
- Progress shown live (row X of Y)
- On row failure, behaviour depends on template error handling setting:
  - **Stop on error**: stop processing at the failed row. Rows already posted remain posted (each row commits individually — true multi-row rollback is not feasible with Opera's per-transaction commit model). Report shows which rows succeeded and which didn't.
  - **Skip failed**: post successful rows, skip failures, report both
  - **Pause and ask**: stop and let user decide whether to continue or stop
- Post-import report: what was posted, what was skipped, references allocated
- Note: existing import methods support `validate_only=True` — the dry run stage uses this to run full validation without committing, ensuring all issues are caught before any posting begins

---

## 4. Data Sources

Three ingestion methods feeding the same pipeline.

### File Upload (CSV/Excel)

- Upload via UI, pick a template (or create mapping on the fly)
- AI assist reads headers and sample rows, suggests mappings
- User confirms, previews, validates, approves, imports

### API Push (Phase 2)

- REST endpoint: `POST /api/imports/ingest`
- Payload: template ID + array of row data (JSON)
- Same validation pipeline — returns audit report in response
- Can be configured to auto-approve if all rows pass (for trusted integrations)
- Authentication via API key per integration

### Email Attachment (Phase 2)

- Follows existing email monitoring pattern
- Template defines: sender/subject match rules, which attachment to process
- Auto-ingested, validated, held for approval unless auto-approve is enabled

---

## 5. Target Adapters

The adapter is the only component that knows about the destination system.

### Adapter Interface

Each adapter implements:
- `get_supported_types()` — which transaction types it can post
- `get_field_requirements(type)` — required/optional/auto fields for a type
- `validate_row(type, row)` — check a single row against the target system
- `post_rows(type, rows)` — post validated rows
- `get_suggestions(field, value)` — fuzzy match suggestions for bad values

### Phase 1 Adapter: Opera SE

- Wraps existing `OperaSQLImport` methods
- Field requirements populated from snapshot data
- Validation uses existing helpers: account lookup, dormancy check, period validation, duplicate detection
- Posting uses existing methods with their built-in locking, sequence allocation, and balance updates
- No new posting code written — the adapter is a thin orchestration layer

### Future Adapters

- **Opera 3**: posts via the Opera 3 write agent
- **Other accounting systems**: Xero, Sage, QuickBooks — same template engine, different adapter
- **Generic SQL**: for custom databases

---

## 6. AI Assist Layer

Optional enhancement on top of the template engine. AI suggests, user confirms.

### Column Mapping Suggestions

- User uploads a file with headers
- AI reads headers + sample data rows
- Returns suggested mappings with confidence: "Amount (column D) → Transaction Amount (95%)"
- User accepts, adjusts, or ignores each suggestion
- Suggestions improve over time from successful imports

### Data Quality Suggestions

- During validation, AI can suggest fixes for common issues
- "Row 3 has 'Smith and Sons' — closest Opera account is 'SMIT001' (Smith & Sons Ltd)"
- Fuzzy matching already exists in the codebase (bank import uses it)

### Guardrails

- AI never auto-posts. Every suggestion requires user confirmation.
- AI mapping confidence below 70% shows as "uncertain — please verify"
- Full audit trail includes whether AI suggestions were accepted or modified

---

## 7. Phasing

### Phase 1 — Opera SE File Import
- Transaction type registry from snapshots
- Import template model (create, save, reuse)
- File upload (CSV/Excel)
- Full validation pipeline with plain-English messages
- Preview, dry run, audit report, approve, post
- Template builder UI
- Opera SE adapter wrapping existing import methods
- **Opera 3 parity**: Phase 1 is SE-only by design. The core engine (templates, pipeline, UI) is adapter-agnostic — the Opera 3 adapter is added in Phase 3 via the write agent. This is acknowledged as a deliberate exception to the SE/Opera 3 parity rule, as the write agent must exist first.

### Phase 2 — API Ingestion, Email & Composite Types
- REST API endpoint for programmatic imports (third-party systems push data)
- Email attachment monitoring and auto-ingestion
- Auto-approve setting for trusted sources
- API key authentication per integration
- Composite transaction types (e.g. ecommerce order → invoice + stock + receipt)

### Phase 3 — AI Assist, Template Library & Additional Adapters
- AI-assisted column mapping suggestions
- AI data quality suggestions during validation
- Pre-built template library from web research (Sage, Xero, Shopify, QuickBooks, WooCommerce, Dext, etc.)
- Opera 3 adapter (via write agent)
- Additional target system adapters as needed

### Integration Roadmap (informs template library priorities)

| Source System | Opera Module | Transaction Types |
|--------------|-------------|-------------------|
| Sage Payroll | Nominal Ledger | Salary journals, NI, pension, PAYE |
| Shopify / WooCommerce | SOP + Stock + SL | Orders, stock adjust, invoices, receipts |
| Amazon Seller | SOP + Stock + SL | Orders, FBA stock, settlement reports |
| Dext / Concur | Purchase Ledger | Purchase invoices, expense claims |
| Xero | All ledgers | Journals, invoices, payments, receipts |
| Warehouse WMS | Stock | Stock movements, counts, transfers |
| CRM (HubSpot etc.) | SL / PL master | Customer/supplier creation and updates |
| Stripe / PayPal | Cashbook | Receipt posting with fee splitting |

Priority is driven by user demand — the engine handles any of these once the template exists.

---

## 8. What Changes

### Add
- `apps/imports/` — new module: API routes, template management
- `sql_rag/import_engine.py` — core pipeline: parse, map, validate, audit
- `sql_rag/import_registry.py` — transaction type registry built from snapshots
- `sql_rag/import_templates.py` — template CRUD and storage
- `sql_rag/import_adapters/opera_se.py` — Opera SE adapter
- `imports.db` — SQLite: templates, import history, audit logs
- Frontend pages: Import dashboard, template builder, file upload, preview/validate, audit report

### Reuse
- `sql_rag/opera_sql_import.py` — all existing import methods (no changes)
- `apps/transaction_snapshot/` — provides registry data
- Fuzzy matching from bank import module
- Email monitoring from existing email infrastructure
- Data provider pattern from supplier module

### Don't Touch
- Existing GoCardless, bank statement, or supplier import flows — they continue as-is
- Opera database structure
- Existing import methods (they're called by the adapter, not modified)

---

## 9. Success Criteria

1. User can upload a CSV, map columns to a transaction type, validate, and import to Opera — under 2 minutes for a familiar template
2. Every validation error explains what's wrong and how to fix it in plain English
3. Full audit trail for every import: who, when, what file, what template, what was posted
4. Templates are reusable — second import with same source takes seconds
5. Only transaction types with completed snapshots are available — no guessing
6. Existing import methods are reused, not rewritten
7. Architecture supports additional target systems without changing the core engine
