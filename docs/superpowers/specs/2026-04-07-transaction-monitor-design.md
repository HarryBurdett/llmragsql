# Opera Transaction Monitor — Design Spec

## Goal

Passively monitor any live Opera SQL Server installation in real-time, auto-capture every transaction type with full field-level data, and build a comprehensive library of Opera's posting patterns. This feeds the import engine registry — the more transaction types captured, the more import capabilities unlocked.

## Problem

The current snapshot tool requires manual before/after captures one transaction at a time. Building a complete picture of Opera's field requirements across all modules (Cashbook, Sales, Purchase, Nominal, SOP, POP, Stock) would take weeks of manual work. Live client systems process hundreds of transactions daily — we need to tap into that.

## Approach

Read-only background polling against any Opera SQL Server. Auto-discovers all tables, tracks new rows by ID watermark, groups related rows into transactions using Opera's linking fields, classifies by type, and builds the library automatically.

**Zero impact on the target system**: SELECT WITH (NOLOCK) only. No schema changes, no triggers, no installations. The Opera users won't know it's there.

---

## 1. Connection Management

### Settings (stored in local SQLite, separate from main app)

| Field | Description |
|-------|-------------|
| Connection name | Human-readable label, e.g. "Acme Ltd - Live" |
| Server host | SQL Server hostname or IP |
| Port | Default 1433 |
| Database name | Opera database name |
| Username | SQL login with read-only access |
| Password | Stored encrypted |
| Active | Whether this connection is currently being monitored |

Multiple connections can be saved. One active monitor at a time (to keep resource usage minimal).

### Test Connection

Verifies:
1. SQL Server is reachable
2. Credentials are valid
3. Database contains Opera tables (checks for `aentry`, `nparm`, `sname`, `pname`)
4. Reports: table count, Opera version indicators, company name if detectable
5. Queries `alogin` to build the list of legitimate Opera user codes (for verified/suspicious filtering)

### Setup Requirements

The client provides:
- A SQL Server login with `db_datareader` role on the Opera database
- Network access from the monitoring machine to their SQL Server

Nothing is installed or changed on their system.

---

## 2. Monitoring Engine

### Auto-Discovery

On first connection, the monitor:
1. Queries `INFORMATION_SCHEMA.TABLES` to get all user tables
2. For each table, checks if an `id` column exists (Opera SE uses `nextid`-allocated identity)
3. Records current `MAX(id)` as the starting watermark for each table
4. Stores watermarks in local SQLite — survives restart

### Polling

- Polls every 5 seconds (configurable)
- For each monitored table: `SELECT * FROM {table} WITH (NOLOCK) WHERE id > @watermark`
- Typically returns 0 rows (no activity) — negligible load
- When new rows found, updates watermark and passes rows to the grouping engine
- Watermarks persisted after each poll cycle

### Resilience

- Reconnects automatically if connection drops (exponential backoff: 5s, 10s, 30s, 60s)
- Resumes from last watermark — no data loss on reconnection
- Logs connection status changes

---

## 3. Transaction Grouping

New rows from different tables are grouped into a single logical transaction using Opera's linking fields.

### Linking Strategy

| Transaction Origin | Primary Key Chain |
|-------------------|-------------------|
| Cashbook entry | `aentry` (ae_entry + ae_cbtype + ae_acnt) → `atran` → `ntran` (nt_jrnl) → `anoml` (ax_unique) |
| Sales ledger | `stran` (st_account + st_trref) → `snoml` → `ntran` → `sname` balance |
| Purchase ledger | `ptran` (pt_account + pt_trref) → `pnoml` → `ntran` → `pname` balance |
| Sales order | `ihead` (ih_order) → `iline` → `stran` if invoiced → stock movements |
| Purchase order | `ohead` (oh_order) → `oline` → `ptran` if receipted → stock movements |
| Nominal journal | `ntran` grouped by nt_jrnl (no cashbook involvement) |
| Stock movement | `stmove` → linked stock tables → nominal postings |
| Master record | New row in `sname`, `pname`, `nacnt`, `nbank`, or `stitem` with no transaction chain |
| VAT | `zvtran` + `nvat` linked to parent transaction |

### Grouping Window

- Rows detected in the same poll cycle that share linking fields → grouped immediately
- Incomplete groups (e.g. `aentry` found but `atran` not yet committed) → held for up to 30 seconds
- After 30 seconds, group is finalised with whatever rows are present (may be flagged as incomplete)

### Verification Filter

**Verified transactions** — all of:
- `input_by` field matches a known Opera user from `alogin`
- Expected table chain is complete for the detected transaction type
- Field values consistent with Opera conventions (amounts in correct units, signs correct)

**Suspicious transactions** — any of:
- `input_by` not in `alogin` list (likely third-party software)
- Incomplete table chain (missing anoml, nacnt updates, etc.)
- Unexpected field patterns

Suspicious transactions are captured separately and flagged for manual review. They are NOT added to the verified library and will NOT feed the import engine registry unless manually approved.

---

## 4. Transaction Classification

The monitor auto-classifies each captured transaction by examining the table/field pattern:

| Pattern | Classification |
|---------|---------------|
| aentry + atran(at_type=4) + stran | Sales Receipt |
| aentry + atran(at_type=5) + ptran | Purchase Payment |
| aentry + atran(at_type=3) + stran | Sales Refund |
| aentry + atran(at_type=6) + ptran | Purchase Refund |
| aentry + atran(at_type=1) + ntran only | Nominal Payment |
| aentry + atran(at_type=2) + ntran only | Nominal Receipt |
| aentry + atran(at_type=8) × 2 | Bank Transfer |
| stran(st_trtype='I') + snoml + ntran | Sales Invoice |
| stran(st_trtype='C') + snoml + ntran | Sales Credit Note |
| ptran(pt_trtype='I') + pnoml + ntran | Purchase Invoice |
| ptran(pt_trtype='C') + pnoml + ntran | Purchase Credit Note |
| ntran only (multiple rows, same nt_jrnl) | Nominal Journal |
| ihead + iline (no stran) | Sales Order |
| ohead + oline (no ptran) | Purchase Order |
| stmove / stock tables | Stock Movement |
| salloc / palloc records | Allocation |
| New sname row | New Customer |
| New pname row | New Supplier |

Unrecognised patterns are saved as "Unknown" for manual classification.

---

## 5. Coverage Dashboard

### Type Checklist

Shows all known Opera transaction types with capture status:

```
Module              Type                    Captured    Status
─────────────────────────────────────────────────────────────
Cashbook            Sales Receipt           4           ✓
Cashbook            Purchase Payment        7           ✓
Cashbook            Sales Refund            0           ✗
Cashbook            Bank Transfer           0           ✗
Sales Ledger        Invoice                 12          ✓
Sales Ledger        Credit Note             0           ✗
Sales Ledger        Allocation              3           ✓
Purchase Ledger     Invoice                 8           ✓
...
─────────────────────────────────────────────────────────────
Coverage: 18 of 26 types (69%)
```

### Per-Type Analysis

Click any captured type to see:
- All captured instances listed by date/time
- Field-by-field analysis: which fields are always/sometimes/never populated (across all captures of that type)
- Sample values for each field
- Tables written with row counts

### Monitoring Status

- Connection status (connected / disconnected / reconnecting)
- Monitoring duration
- Total transactions captured
- Transactions per hour rate
- Last activity timestamp

---

## 6. Export to Import Registry

When a transaction type has sufficient captures (at least 1 verified), the user can export it to the import engine registry:

1. Click "Export to Registry" on a captured type
2. System generates a registry entry with:
   - All fields classified as user-provided / template-default / auto-generated
   - Field types, max lengths, lookup tables inferred from captured data
   - Linked to the corresponding `OperaSQLImport` method
3. User reviews and confirms
4. Entry saved to `data/import_registry/`

This is the bridge between monitoring and importing — captured patterns become importable transaction types.

---

## 7. Storage

### Local SQLite: `transaction_monitor.db`

Separate from the main app's databases. Company-independent — this is a development/admin tool.

```sql
-- Monitor connections
CREATE TABLE monitor_connections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    server_host TEXT NOT NULL,
    server_port INTEGER DEFAULT 1433,
    database_name TEXT NOT NULL,
    username TEXT NOT NULL,
    password_encrypted TEXT NOT NULL,
    is_active INTEGER DEFAULT 0,
    last_connected_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Table watermarks (per connection)
CREATE TABLE table_watermarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    last_id INTEGER DEFAULT 0,
    row_count INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(connection_id, table_name)
);

-- Captured transactions
CREATE TABLE captured_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id INTEGER NOT NULL,
    transaction_type TEXT,
    classification TEXT,
    is_verified INTEGER DEFAULT 0,
    is_suspicious INTEGER DEFAULT 0,
    suspicious_reason TEXT,
    input_by TEXT,
    tables_json TEXT,
    rows_json TEXT,
    field_summary_json TEXT,
    captured_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Opera users (from alogin on target system)
CREATE TABLE opera_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id INTEGER NOT NULL,
    user_code TEXT NOT NULL,
    user_name TEXT,
    UNIQUE(connection_id, user_code)
);
```

---

## 8. UI Pages

### Monitor Settings Page

- Add/edit/delete connections
- Test connection button (with result display)
- Start/Stop monitor button
- Connection status indicator

### Monitor Dashboard

- Coverage checklist (types captured vs missing)
- Live status: connected, transactions/hour, duration
- Recent captures list (last 50)
- Suspicious transactions section (separate, for review)

### Transaction Detail

- Click any captured transaction to see:
  - Full table/row breakdown
  - Every field with value, type, and population status
  - Verified/Suspicious badge with reason
  - "Export to Registry" button

---

## 9. What Changes

### Add
- `apps/transaction_monitor/api/routes.py` — API endpoints
- `sql_rag/transaction_monitor.py` — monitoring engine (polling, grouping, classification)
- `sql_rag/transaction_monitor_db.py` — SQLite schema and queries
- `frontend/src/pages/TransactionMonitor.tsx` — dashboard
- `frontend/src/pages/MonitorSettings.tsx` — connection management

### Modify
- `api/main.py` — register new router
- `frontend/src/App.tsx` — add routes
- `frontend/src/components/Layout.tsx` — add menu items

### Reuse
- Snapshot tool's PRESETS list for the coverage checklist
- Import registry module for export
- Existing table metadata queries from snapshot tool

---

## 10. Success Criteria

1. Point at any Opera SQL Server with a read-only login — monitor starts capturing within seconds
2. Zero impact on the target system — no locks, no schema changes, no performance degradation
3. Transactions correctly grouped across tables using Opera's linking fields
4. Concurrent transactions from different users are never confused
5. Third-party writes identified and flagged separately from genuine Opera transactions
6. Coverage dashboard shows exactly which types have been captured and which are missing
7. Captured data exports to the import engine registry with one click
8. Monitor survives connection drops and app restarts — resumes from last watermark
