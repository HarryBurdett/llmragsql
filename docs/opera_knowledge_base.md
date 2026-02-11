# Opera SQL SE Knowledge Base

This document captures knowledge about Pegasus Opera SQL SE database structure, conventions, and integration patterns learned through development.

## Database Tables

### Account/Transaction Tables

#### `aentry` - Bank Account Entries (Header)
- `ae_acnt` - Bank account code (e.g., 'BC010')
- `ae_entry` - Entry reference (e.g., 'R200006824' for receipt, 'P500007182' for payment)
- `ae_value` - Entry value in **PENCE** (not pounds)
  - **Payments are stored as NEGATIVE values**
  - **Receipts are stored as POSITIVE values**
- `ae_recdate` - Record date
- `ae_cbtype` - Cashbook type

#### `atran` - Bank Account Transactions (Detail)
- `at_acnt` - Bank account code
- `at_entry` - Links to aentry.ae_entry
- `at_pstdate` - Post date
- `at_value` - Transaction value in **PENCE**
  - **Payments are stored as NEGATIVE values**
  - **Receipts are stored as POSITIVE values**
- `at_type` - Transaction type:
  - `4` = Receipt (sales receipt)
  - `5` = Payment (purchase payment)
- `at_account` - Customer/Supplier account code
- `at_name` - Customer/Supplier name
- `at_inputby` - User who entered transaction

#### `ntran` - Nominal Ledger Transactions
- `nt_acnt` - Nominal account code
- `nt_year` - Financial year
- `nt_value` - Transaction value in **POUNDS** (not pence)
- `nt_entr` - Entry date
- `nt_cmnt` - Comment/reference (contains PL reference)
- `nt_ref` - NL reference
- `nt_trnref` - Transaction reference with embedded data:
  - **First 30 characters**: Supplier/Customer name (left-padded)
  - **Characters 31-40**: Payment type
  - **Remaining**: Additional reference info (e.g., "(RT)" for remittance)
  - Format: `{supplier_name[:30]:<30}{payment_type:<10}(RT)`
- `nt_type` - Transaction type

#### `ptran` - Purchase Ledger Transactions
- `pt_account` - Supplier account code
- `pt_trref` - Transaction reference
- `pt_trdate` - Transaction date
- `pt_trvalue` - Transaction value in **POUNDS**
- `pt_trbal` - Transaction balance in **POUNDS** (outstanding amount)
- `pt_trtype` - Transaction type
- `pt_supref` - Supplier reference

#### `stran` - Sales Ledger Transactions
- `st_account` - Customer account code
- `st_trref` - Transaction reference
- `st_trdate` - Transaction date
- `st_trvalue` - Transaction value in **POUNDS**
- `st_trbal` - Transaction balance in **POUNDS**
- `st_trtype` - Transaction type

#### `palloc` - Purchase Ledger Allocations
- Links payments to invoices in purchase ledger
- Used for allocation tracking

#### `salloc` - Sales Ledger Allocations
- Links receipts to invoices in sales ledger
- Used for allocation tracking

### Master Data Tables

#### `pname` - Supplier Master
- `pn_account` - Supplier account code
- `pn_name` - Supplier name
- `pn_balance` - Current balance

#### `sname` - Customer Master
- `sn_account` - Customer account code
- `sn_name` - Customer name
- `sn_balance` - Current balance

#### `nname` - Nominal Account Master
- `nn_acnt` - Nominal account code
- `nn_name` - Account name
- `nn_type` - Account type
- Control accounts vary by installation (see Control Accounts section below)

## Key Conventions

### Field Naming Convention
Both Opera SQL SE and Opera 3 use the same field naming convention with table-based prefixes:
- `ae_` prefix for bank entry fields (aentry)
- `at_` prefix for bank transaction fields (atran)
- `pn_` prefix for supplier fields (pname)
- `sn_` prefix for customer fields (sname)
- `nn_` prefix for nominal account fields (nname)
- `pt_` prefix for purchase transaction fields (ptran)
- `st_` prefix for sales transaction fields (stran)
- `nt_` prefix for nominal transaction fields (ntran)
- `nk_` prefix for bank account fields (nbank)
- `co_` prefix for company fields (seqco)

### SQL SE Additional Fields
Opera SQL SE includes extra fields not present in Opera 3:

**Common to all tables:**
- `id` - SQL Server identity column (primary key)
- `datecreated` - Record creation timestamp
- `datemodified` - Record modification timestamp
- `state` - Record state flag

**Foreign key fields:**
- `_fk_company_*` prefixed fields for SQL Server relationship references

**Table-specific:**
- `stran`: `jxservid`, `jxrenewal` (service/renewal tracking)

### Amount Storage
| Table | Unit | Notes |
|-------|------|-------|
| aentry, atran | PENCE | Multiply by 100 when converting from pounds |
| ntran, ptran, stran | POUNDS | Direct pound values |

### Payment Signs
- **atran**: Payments = NEGATIVE, Receipts = POSITIVE
- **ntran**: Sign depends on debit/credit nature
- **ptran/stran**: Usually positive, balance indicates outstanding

### Reference Formats
- PL references typically: Invoice number or payment reference
- NL references contain embedded supplier/customer name in first 30 chars of `nt_trnref`

## Transaction Structure

When transactions are entered through Opera, multiple tables are updated in a coordinated way.

### Sales Receipt (Type 4)
When a sales receipt is entered, the following tables are updated:

| Table | Records | Description |
|-------|---------|-------------|
| `aentry` | 1 | Header record with entry reference (e.g., 'R200006824') |
| `atran` | 1 | Bank transaction with `at_type=4`, **positive** value in pence |
| `stran` | 1 | Sales ledger transaction reducing customer balance |
| `ntran` | 2 | Double-entry: DR Bank account, CR Debtors control |
| `salloc` | 1 | Links receipt to original invoice(s) |

**Entry reference format**: `R` prefix followed by 9-digit number (e.g., 'R200006824')

### Purchase Payment (Type 5)
When a purchase payment is entered, the following tables are updated:

| Table | Records | Description |
|-------|---------|-------------|
| `aentry` | 1 | Header record with entry reference (e.g., 'P500007182') |
| `atran` | 1 | Bank transaction with `at_type=5`, **negative** value in pence |
| `ptran` | 1 | Purchase ledger transaction reducing supplier balance |
| `ntran` | 2 | Double-entry: DR Creditors control, CR Bank account |
| `palloc` | 1 | Links payment to original invoice(s) |

**Entry reference format**: `P` prefix followed by 9-digit number (e.g., 'P500007182')

### ntran Double-Entry Pattern
Each cashbook entry creates two nominal ledger entries:

**For Receipts:**
```
DR Bank Account (e.g., BC010)     [positive value]
CR Debtors Control (e.g., BB020)  [negative value]
```

**For Payments:**
```
DR Creditors Control (e.g., CA030) [positive value]
CR Bank Account (e.g., BC010)      [negative value]
```

### Key ntran Fields
- `nt_inp` - Input by user (max 10 characters, may be truncated)
- `nt_ref` - Reference number
- `nt_trnref` - Transaction reference (first 30 chars contain supplier/customer name)
- `nt_value` - Value in **POUNDS** (not pence)

## Import Considerations

### Bank Statement Import Flow
1. Parse bank statement CSV
2. Match transactions to suppliers (for payments) or customers (for receipts)
3. Create entries in:
   - `aentry` / `atran` - Bank account entry
   - `ptran` - Purchase ledger payment (with allocation via `palloc`)
   - `ntran` - Nominal ledger entries (bank account + creditors control)

### Duplicate Detection
When checking if a transaction already exists in `atran`:
```sql
SELECT COUNT(*) FROM atran
WHERE at_acnt = '{bank_code}'
AND at_pstdate = '{date}'
AND ABS(ABS(at_value) - {amount_pence}) < 1
```
**Important**: Use `ABS(ABS(at_value) - amount)` because payments are stored as negative values.

### Locking Strategy

**Locking Concerns:**
- Direct database writes bypass Opera's application-level locking
- Opera maintains in-memory locks, session locks, and batch posting queues
- **Recommended**: Use Opera's COM automation or standard import files for production

**SQL Server Locking Approach (for multi-user environments):**

1. **Validation/Read Queries** - Use `NOLOCK` hint:
   ```sql
   SELECT * FROM sname WITH (NOLOCK) WHERE sn_account = 'A001'
   ```
   - Allows dirty reads but prevents blocking other users
   - Acceptable for validation where absolute consistency isn't critical

2. **Sequence Generation** - Use `UPDLOCK, ROWLOCK`:
   ```sql
   SELECT MAX(nt_jrnl) + 1 FROM ntran WITH (UPDLOCK, ROWLOCK)
   ```
   - UPDLOCK: Locks rows for update (prevents concurrent reads getting same number)
   - ROWLOCK: Forces row-level locks instead of table locks

3. **Lock Timeout** - Set at transaction start:
   ```sql
   SET LOCK_TIMEOUT 5000  -- 5 second timeout
   ```
   - Prevents indefinite blocking if another user holds a lock
   - Operation fails cleanly if lock cannot be acquired

4. **Master File Updates** - Use `ROWLOCK`:
   ```sql
   UPDATE sname WITH (ROWLOCK) SET sn_currbal = ... WHERE sn_account = 'A001'
   ```
   - Minimizes lock scope to single row
   - Other users can update different customers simultaneously

**Avoid:**
- `HOLDLOCK` on large tables (causes table-level serialization)
- Long-running transactions (hold locks for minimum time)
- Table scans in transactional queries

## Variance Analysis (Creditors Reconciliation)

### Matching Strategy
Match PL entries to NL entries using multiple strategies in order:

1. **Date + Value + Supplier** (most precise)
   - Key format: `{date}|{abs_value:.2f}|{supplier_account}`

2. **Date + Value only** (for supplier name mismatches)
   - Key format: `{date}|{abs_value:.2f}`

3. **Value + Supplier** (for date differences)
   - Match by absolute value within 0.02 tolerance and supplier account

### Extracting Supplier from NL
```python
# nt_trnref first 30 chars contain supplier name
description = txn['description'].strip()
nl_supplier_name = description[:30].strip().upper()

# Look up supplier account from pname
nl_supplier_account = supplier_name_to_account.get(nl_supplier_name, '')

# Partial match for truncated names
if not nl_supplier_account and nl_supplier_name:
    for name, acc in supplier_name_to_account.items():
        if name.startswith(nl_supplier_name) or nl_supplier_name.startswith(name):
            nl_supplier_account = acc
            break
```

## Opera COM Automation

### COM Object IDs (to be verified)
- `Pegasus.Opera3.Application` - Main application
- `Pegasus.Opera3.SalesLedger` - Sales Ledger module
- `Pegasus.Opera3.PurchaseLedger` - Purchase Ledger module
- `Pegasus.Opera3.NominalLedger` - Nominal Ledger module
- `Pegasus.Opera3.Cashbook` - Cashbook module

### Discovering COM Interface
```python
import win32com.client
app = win32com.client.Dispatch("Pegasus.Opera3.Application")
print(dir(app))  # List available methods/properties
```

**Note**: The exact COM interface methods need to be verified against Opera's actual implementation.

## Standard Import File Formats

Opera supports standard import file formats documented in the Pegasus help files. Using these allows Opera to handle all locking and validation.

Import types available:
- Sales Invoices
- Purchase Invoices
- Nominal Journals
- Customers
- Suppliers
- Products
- Sales Orders
- Purchase Orders
- Cashbook entries

**Recommendation**: For production use, generate Opera-compatible import files rather than direct database writes to ensure data integrity with concurrent users.

## Financial Year Handling

- `current_year` typically from system settings or calculated
- NL transactions have `nt_year` field for financial year
- PL/SL transactions use date-based year calculation: `YEAR(pt_trdate)`

## Control Accounts

Control account codes vary by installation - debtors and creditors control accounts will never be the same code.

**Where control accounts are configured:**

1. **Primary locations** (separate tables for Sales and Purchase):
   - `sprfls` table (Sales Profiles): `sc_dbtctrl` - Debtors control account
   - `pprfls` table (Purchase Profiles): `pc_crdctrl` - Creditors control account

2. **Fallback location**: `nparm` table (Nominal Parameters) - used if profile fields are blank
   - `np_dca` - Debtors control account
   - `np_cca` - Creditors control account

**To retrieve control accounts programmatically:**
```sql
-- Get debtors control account (from sprfls, fallback to nparm)
SELECT COALESCE(NULLIF((SELECT sc_dbtctrl FROM sprfls), ''), (SELECT np_dca FROM nparm)) AS debtors_control

-- Get creditors control account (from pprfls, fallback to nparm)
SELECT COALESCE(NULLIF((SELECT pc_crdctrl FROM pprfls), ''), (SELECT np_cca FROM nparm)) AS creditors_control
```

These are the nominal accounts that should reconcile to the respective ledger totals.

## Transaction Reference Matching (KEY INSIGHT)

**The most reliable way to match ntran to ptran/stran is via the transaction reference:**

- `nt_cmnt` (Nominal Ledger comment) = `pt_trref` (Purchase Ledger reference)
- `nt_cmnt` (Nominal Ledger comment) = `st_trref` (Sales Ledger reference)

This is a **direct link** established when transactions are posted. Example:
```
ntran.nt_cmnt = 'SNAPSHOT-TEST-PAY'
ptran.pt_trref = 'SNAPSHOT-TEST-PAY'
```

**Matching priority for reconciliation:**
1. **Reference match**: `nt_cmnt = pt_trref` (most accurate)
2. **Date + Value + Account**: Fallback when reference doesn't match
3. **Value + Account only**: For timing differences

## Reconciliation Formulas

### Creditors (Purchase Ledger)
```
PL Total = SUM(pt_trbal) from ptran for active suppliers
NL Total = SUM(nt_value) from ntran where nt_acnt = '{creditors_control}'
Variance = NL Total - PL Total
```
(Replace `{creditors_control}` with your installation's creditors control account, e.g., 'CA010' or 'CA030')

### Debtors (Sales Ledger)
```
SL Total = SUM(st_trbal) from stran for active customers
NL Total = SUM(nt_value) from ntran where nt_acnt = '{debtors_control}'
Variance = NL Total - SL Total
```
(Replace `{debtors_control}` with your installation's debtors control account, e.g., 'DA010' or 'BB020')

---

## Opera 3 (FoxPro Version)

Opera 3 is the older version of Pegasus Opera that uses Visual FoxPro DBF files instead of SQL Server.

### Installation Location
- **Server Path**: `C:\Apps\O3 Server VFP`
- **System Folder**: `C:\Apps\O3 Server VFP\System`
- **Company List**: `seqco.dbf` (in System folder)
- **File Format**: Visual FoxPro DBF files (.dbf)

### Field Naming Convention
Opera 3 uses the same field naming convention as Opera SQL SE (see "Key Conventions" section above). All fields use a table-based prefix (e.g., `pn_` for pname, `st_` for stran).

### Company Table (seqco.dbf)
| Field | Description |
|-------|-------------|
| co_code | Company code |
| co_name | Company name |
| co_subdir | Data subdirectory path |

### Reading Opera 3 Data
Use the `sql_rag/opera3_foxpro.py` module:
```python
from sql_rag.opera3_foxpro import Opera3System, Opera3Reader

# System-level access (companies, system settings)
system = Opera3System(r"C:\Apps\O3 Server VFP")

# Get list of companies from seqco.dbf
companies = system.get_companies()

# Get reader for a specific company
reader = system.get_reader_for_company("COMPANY01")

# Or access company data directly if you know the path
reader = Opera3Reader(r"C:\Apps\O3 Server VFP\COMPANY01")

# List available tables
tables = reader.list_tables()

# Read a table
suppliers = reader.read_table("pname")

# Query with filters
invoices = reader.query("ptran", filters={"pt_account": "SUP001"})
```

### Table Structure
Opera 3 uses the same table names and similar field structures as Opera SQL SE:

| Table | Description |
|-------|-------------|
| pname | Supplier Master |
| sname | Customer Master |
| nname | Nominal Account Master |
| stock | Stock/Product Master |
| ptran | Purchase Ledger Transactions |
| stran | Sales Ledger Transactions |
| ntran | Nominal Ledger Transactions |
| atran | Bank Account Transactions |
| aentry | Bank Account Entries |
| palloc | Purchase Ledger Allocations |
| salloc | Sales Ledger Allocations |
| sysparm | System Parameters |

### Key Differences from SQL SE
1. **File Format**: DBF files vs SQL Server tables
2. **Encoding**: Windows CP1252 character encoding
3. **No SQL**: Must iterate through records, no SQL queries
4. **Locking**: FoxPro file-level locking vs SQL Server row locking

### Requirements
- `dbfread` package: `pip install dbfread`

---

## Cashbook Type Codes (atype Table)

The `atype` table defines the payment and receipt type codes available for cashbook transactions. These are **user-configurable** per company - the specific codes (P1, R2, etc.) are NOT fixed by Opera.

### atype Table Structure

| Field | Description |
|-------|-------------|
| `ay_cbtype` | Type code (e.g., 'P1', 'R2', 'P5', 'PR') - user-defined |
| `ay_desc` | Description (e.g., 'Cheque', 'BACS', 'Direct Credit') |
| `ay_type` | Category: 'P' = Payment, 'R' = Receipt, 'T' = Transfer |
| `ay_entry` | Next entry number counter (e.g., 'P100008025') |

### ay_type Categories

| Category | Code | Description | Money Direction |
|----------|------|-------------|-----------------|
| Payment | P | Money going out | Negative in aentry/atran |
| Receipt | R | Money coming in | Positive in aentry/atran |
| Transfer | T | Internal bank transfers | Between accounts |

### Example atype Records

```
Code     Description          ay_type   ay_entry
P1       Cheque               P         P100008025
P2       BACS                 P         P200001443
P5       Direct Credit        P         P500007176
PR       Purchase refund      R         PR00000533
R1       Cheque Receipt       R         R100001277
R2       BACS                 R         R200006822
R3       Direct Credit        R         R300000484
T1       Bank Transfer        T         T100000657
```

### Key Insight: Type Codes vs Transaction Types

- **`ae_cbtype` / `at_cbtype`**: User-selected type from atype table (variable per company)
- **`at_type`**: Internal transaction category (FIXED values 1-6, see below)

The import code must:
1. Accept a user-specified type code (or find a default from atype)
2. Validate the type code exists and has correct ay_type category
3. Set at_type based on the TRANSACTION CONTEXT, not the cbtype

---

## Internal Transaction Types (at_type Field)

The `atran.at_type` field contains a FIXED internal category code that determines the transaction type. These values are NOT user-configurable.

### at_type Values

| at_type | Transaction Type | Description | ae_cbtype Category |
|---------|------------------|-------------|--------------------|
| 1.0 | Nominal Payment | Cashbook payment to nominal account (no ledger) | P |
| 2.0 | Nominal Receipt | Cashbook receipt from nominal account (no ledger) | R |
| 3.0 | Sales Refund | Refund TO customer (money out, reduces debtors) | P |
| 4.0 | Sales Receipt | Receipt FROM customer (money in, reduces debtors) | R |
| 5.0 | Purchase Payment | Payment TO supplier (money out, reduces creditors) | P |
| 6.0 | Purchase Refund | Refund FROM supplier (money in, reduces creditors) | R |

### Ledger Transaction Types

| Ledger | Field | Payment | Receipt | Refund |
|--------|-------|---------|---------|--------|
| Purchase (ptran) | `pt_trtype` | 'P' | N/A | 'F' |
| Sales (stran) | `st_trtype` | N/A | 'R' | 'F' |

---

## Complete Transaction Patterns (From Testing)

The following patterns were verified by before/after snapshot testing in a live Opera system.

### Purchase Payment (at_type = 5)

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Payment category), ae_value = -amount_pence |
| `atran` | 1 | at_type = 5, at_value = -amount_pence |
| `ntran` | 2 | Bank (CR, -amount_pounds), Creditors control (DR, +amount_pounds) |
| `ptran` | 1 | pt_trtype = 'P', pt_trvalue = -amount_pounds |
| `palloc` | 1 | al_type = 'P', al_val = -amount_pounds |
| `anoml` | 2 | Bank + Creditors control (ax_source = 'P') |
| `pname` | Modified | pn_currbal reduced |
| `atype` | Modified | ay_entry incremented |

### Sales Receipt (at_type = 4)

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Receipt category), ae_value = +amount_pence |
| `atran` | 1 | at_type = 4, at_value = +amount_pence |
| `ntran` | 2 | Bank (DR, +amount_pounds), Debtors control (CR, -amount_pounds) |
| `stran` | 1 | st_trtype = 'R', st_trvalue = -amount_pounds |
| `salloc` | 1 | al_type = 'R', al_val = -amount_pounds |
| `anoml` | 2 | Bank + Debtors control (ax_source = 'S') |
| `sname` | Modified | sn_currbal reduced |
| `atype` | Modified | ay_entry incremented |

### Purchase Refund (at_type = 6)

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Receipt category, e.g., 'PR'), ae_value = +amount_pence |
| `atran` | 1 | at_type = 6, at_value = +amount_pence |
| `ntran` | 2 | Bank (DR, +amount_pounds), Creditors control (CR, -amount_pounds) |
| `ptran` | 1 | pt_trtype = 'F', pt_trvalue = +amount_pounds |
| `palloc` | 1 | al_type = 'F' or 'R', al_val = +amount_pounds |
| `anoml` | 2 | Bank + Creditors control (ax_source = 'P') |
| `pname` | Modified | pn_currbal increased (refund reduces what we owe) |
| `atype` | Modified | ay_entry incremented |

### Sales Refund (at_type = 3)

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Payment category, e.g., 'P1'), ae_value = -amount_pence |
| `atran` | 1 | at_type = 3, at_value = -amount_pence |
| `ntran` | 2 | Bank (CR, -amount_pounds), Debtors control (DR, +amount_pounds) |
| `stran` | 1 | st_trtype = 'F', st_trvalue = +amount_pounds |
| `salloc` | 1 | al_type = 'F', al_val = +amount_pounds |
| `anoml` | 2 | Bank + Debtors control (ax_source = 'S') |
| `sname` | Modified | sn_currbal increased (refund increases what they owe) |
| `atype` | Modified | ay_entry incremented |

### Nominal Payment (at_type = 1) - No Ledger

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Payment category), ae_value = -amount_pence |
| `atran` | 1 | at_type = 1, at_value = -amount_pence |
| `ntran` | 2 | Bank (CR, -amount_pounds), Nominal account (DR, +amount_pounds) |
| `anoml` | 2 | Bank + Nominal account (ax_source = 'A') |
| `atype` | Modified | ay_entry incremented |

Note: No ptran/stran records - payment is directly to nominal account.

### Nominal Receipt (at_type = 2) - No Ledger

**Tables affected:**

| Table | Records | Key Fields |
|-------|---------|------------|
| `aentry` | 1 | ae_cbtype from atype (Receipt category), ae_value = +amount_pence |
| `atran` | 1 | at_type = 2, at_value = +amount_pence |
| `ntran` | 2 | Bank (DR, +amount_pounds), Nominal account (CR, -amount_pounds) |
| `anoml` | 2 | Bank + Nominal account (ax_source = 'A') |
| `nvat` | 1 | If VAT applicable (nv_vattype = 'S' for sales, 'P' for purchase) |
| `atype` | Modified | ay_entry incremented |

---

## Transfer Files (anoml, pnoml, snoml)

### Key Discovery: anoml Used for ALL Cashbook Transactions

Opera uses `anoml` for **ALL** cashbook transactions, not pnoml/snoml. Each transaction creates 2 anoml records (double-entry):

| Record | Purpose | Value Sign |
|--------|---------|------------|
| 1 | Bank account | +/- depending on direction |
| 2 | Control/Nominal account | Opposite sign |

### anoml Table Structure

| Field | Description |
|-------|-------------|
| `ax_nacnt` | Nominal account code |
| `ax_ncntr` | Cost centre (usually blank) |
| `ax_source` | Source: 'P' = Purchase, 'S' = Sales, 'A' = Analysis/Nominal |
| `ax_date` | Transaction date |
| `ax_value` | Value in POUNDS |
| `ax_tref` | Transaction reference |
| `ax_comment` | Comment (supplier/customer name + description) |
| `ax_done` | Done flag: 'Y' = posted to NL, 'N' = pending |
| `ax_srcco` | Source company (usually 'I') |
| `ax_unique` | Links to atran.at_unique |
| `ax_project` | Project code |
| `ax_job` | Job code |
| `ax_jrnl` | Journal number (from ntran) |
| `ax_nlpdate` | Nominal ledger post date |

### ax_source Values

| Value | Source | Used For |
|-------|--------|----------|
| 'P' | Purchase | Purchase payments, purchase refunds |
| 'S' | Sales | Sales receipts, sales refunds |
| 'A' | Analysis | Nominal payments/receipts (direct to NL) |

### pnoml / snoml Tables

These tables are **NOT used** for standard cashbook transactions. They appear to be used for:
- Invoice posting (pnoml for purchase invoices)
- Invoice posting (snoml for sales invoices)
- Other module-specific postings

---

## Unique ID Format

Opera uses a specific format for unique identifiers across tables:

### Format: `_XXXXXXXXX`
- Underscore prefix
- 9 alphanumeric characters (base-36 encoded timestamp/sequence)
- Total length: 10 characters

### Examples
```
_7E30ZKQD0
_7E30ZKR13
_7E30ZKR8X
```

### Fields Using This Format
- `at_unique` (atran)
- `pt_unique` (ptran)
- `st_unique` (stran)
- `ax_unique` (anoml)
- `nt_pstid` (ntran)

### Generation in Import Code
```python
class OperaUniqueIdGenerator:
    CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    @classmethod
    def generate(cls) -> str:
        current_time = int(time.time() * 1000)  # Milliseconds
        combined = (current_time << 8) + (sequence & 0xFF)
        # Convert to base-36, pad to 9 chars
        id_str = base36_encode(combined).zfill(9)
        return f"_{id_str[-9:]}"
```

---

## GoCardless Batch Posting Pattern

When importing GoCardless payout batches, the fees must be correctly split between net fees and VAT. This ensures cashbook (atran), transfer file (anoml), and nominal ledger (ntran) all have proper detail records.

### GoCardless Fees Structure

GoCardless deducts fees from payouts before sending the net amount to your bank. The fee breakdown is:
- **Net fees**: The fees charged (excluding VAT)
- **VAT on fees**: 20% VAT on the fees (if applicable)
- **Total fees (gross)**: Net fees + VAT

### atran Records for Fees (CRITICAL)

**When VAT is present, atran MUST have 2 lines:**

| Line | Account | Value | Name |
|------|---------|-------|------|
| 1 | Fees expense (e.g., GA030) | -net_fees_pence | "GoCardless net fees" |
| 2 | VAT input (e.g., CA060) | -vat_pence | "GoCardless VAT" |

**When no VAT (VAT = 0):**

| Line | Account | Value | Name |
|------|---------|-------|------|
| 1 | Fees expense (e.g., GA030) | -gross_fees_pence | "GoCardless fees" |

### anoml Records for Fees

The transfer file (anoml) should mirror the atran structure:

| Record | ax_nacnt | ax_value | ax_source |
|--------|----------|----------|-----------|
| 1 | Bank (e.g., BC010) | +net_amount_pounds | 'A' |
| 2 | Fees expense (e.g., GA030) | -net_fees_pounds | 'A' |
| 3 | VAT input (e.g., CA060) | -vat_pounds | 'A' |

### ntran Records for Fees

The nominal ledger (ntran) posts as:

| Record | nt_acnt | nt_value | Description |
|--------|---------|----------|-------------|
| 1 | Bank (e.g., BC010) | +net_amount_pounds | DR Bank |
| 2 | Fees expense (e.g., GA030) | -net_fees_pounds | CR Fees (expense) |
| 3 | VAT input (e.g., CA060) | -vat_pounds | CR VAT (input) |

### Why Split Fees?

1. **VAT Recovery**: The VAT on GoCardless fees is reclaimable as input VAT
2. **Accurate Expense Reporting**: Shows true cost of service (net of VAT)
3. **Data Integrity**: Opera cashbook detail (atran) should match nominal postings
4. **Audit Trail**: Clear breakdown of fees vs VAT in all records
5. **VAT Return**: nvat record required for VAT return processing

### nvat Record for VAT Return

When VAT is present on GoCardless fees, an nvat record MUST be created:

| Field | Value | Description |
|-------|-------|-------------|
| nv_acnt | CA060 (VAT input) | VAT nominal account |
| nv_date | Post date | Transaction date |
| nv_crdate | Post date | Creation date |
| nv_taxdate | Post date | Tax point date |
| nv_ref | GoCardless reference | e.g., 'INTSYSUKLTD-R2VB7P' |
| nv_type | 'P' | Purchase type |
| nv_advance | 0 | Not an advance |
| nv_value | Net fees | Net fees value (e.g., 4.00) |
| nv_vatval | VAT amount | VAT value (e.g., 0.80) |
| nv_vatctry | ' ' | UK (blank) |
| nv_vattype | 'P' | Purchase/Input VAT |
| nv_vatcode | 'S' | Standard rate |
| nv_vatrate | 20.0 | VAT percentage |
| nv_comment | 'GoCardless fees VAT' | Description |

This record is essential for:
- VAT return Box 4 (Input VAT reclaimable)
- VAT reconciliation reports
- HMRC compliance

### Example: £4.80 Fees (£4.00 net + £0.80 VAT)

**atran (2 records):**
```
at_account=GA030, at_value=-400, at_name="GoCardless net fees"
at_account=CA060, at_value=-80, at_name="GoCardless VAT"
```

**anoml (3 records including bank):**
```
ax_nacnt=BC010, ax_value=+486.00 (net payout)
ax_nacnt=GA030, ax_value=-4.00
ax_nacnt=CA060, ax_value=-0.80
```

**ntran (3 records):**
```
nt_acnt=BC010, nt_value=+486.00 (DR bank)
nt_acnt=GA030, nt_value=-4.00 (CR fees expense)
nt_acnt=CA060, nt_value=-0.80 (CR VAT input)
```

---

## Period Posting Rules

When a transaction date is in a different period/year from the current period:

1. **Same Period**: Post directly to ntran (nominal ledger)
2. **Different Period**:
   - Create anoml transfer file records with `ax_done = 'N'`
   - Do NOT post to ntran
   - Opera's period-end process will post these later

This is handled by the `get_period_posting_decision()` function in the import modules.

---

## Payroll & Pension Tables

Opera Payroll stores employee, pay history, and pension data across multiple interconnected tables. This section documents the key tables for building pension provider export utilities.

### Payroll Group Tables (wg* prefix)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `wgrup` | Payroll groups | `wg_group` (code), `wg_name` (e.g., "Intsys UK", "Leavers") |
| `wgarc` | Archive by year | `wga_year` (tax year e.g., "2526"), `wga_fnlpyp` (final pay period) |
| `wgonf` | RTI/FPS configuration | FPS submission status and dates |
| `wggact` | Actions log | Employee actions history |
| `wggaud` | Audit log | Data modification audit trail |

### Employee Master Tables

#### `wname` - Employee Master (253 columns)
Primary employee record with comprehensive personal and pension details.

**Identity Fields:**
- `wn_ref` (PK) - Employee code (e.g., "AP001", "CB001")
- `wn_ninum` - National Insurance Number - **CRITICAL for pension providers**
- `wn_surname`, `wn_forenam` - Employee name
- `wn_title` - Title (Mr, Mrs, etc.)

**Address Fields:**
- `wn_addrs1` to `wn_addrs5` - Address lines
- `wn_pstcde` - Postcode

**Employment Fields:**
- `wn_birth` - Date of birth
- `wn_startdt` - Start date
- `wn_leavdt` - Leave date
- `wn_taxcde` - Tax code
- `wn_nicde` - NI category letter

**Pension Summary Fields:**
- `wn_totpens` - Total pension contributions (employee)
- `wn_totpenl` - Total pensionable pay
- `wn_annpen` - Annual pension value
- `wn_occpen` - Occupational pension flag
- `wn_flexpen` - Flexible pension flag
- `wn_pendb` - Pension DB flag
- `wn_pensih` - Pension SIH flag

#### `wepen` - Employee Pension Enrollment (79 columns)
Links employees to pension schemes with contribution rates.

**Key Fields:**
- `wep_ref` (FK) - Links to `wname.wn_ref`
- `wep_code` (FK) - Pension scheme code (links to `wpnsc.wps_code`)
- `wep_erper` - Employer contribution percentage
- `wep_eeper` - Employee contribution percentage
- `wep_jndt` - Join date
- `wep_lfdt` - Leave date
- `wep_ter` - Total employer contributions to date
- `wep_tee` - Total employee contributions to date
- `wep_tser` - Total statutory employer contributions
- `wep_tsee` - Total statutory employee contributions

#### `wcontacts` - Employee Contact Preferences (18 columns)
Communication preferences including pension correspondence flags.

### Pay History Tables

#### `whist` - Wage History (126 columns)
Monthly/weekly pay records with detailed pension breakdown. **Primary source for pension contribution exports.**

**Period Identification:**
- `wh_ref` (FK) - Employee reference
- `wh_year` - Tax year (e.g., "2526" = 2025/26)
- `wh_period` - Pay period (1-12 for monthly, 1-52 for weekly)
- `wh_paydt` - Pay date

**Pay Fields:**
- `wh_gross` - Gross pay
- `wh_net` - Net pay
- `wh_tax` - Tax deducted
- `wh_eeni` - Employee NI contribution
- `wh_erni` - Employer NI contribution

**Pension Fields (CRITICAL):**
- `wh_pen` - Employee pension contribution THIS PERIOD
- `wh_penbl` - Pensionable pay balance THIS PERIOD
- `wh_totpens` - YTD employee pension contributions
- `wh_totpenl` - YTD pensionable earnings
- `wh_salspen` - Salary sacrifice pension amount
- `wh_ploan` - Pension loan amount
- `wh_spbp` - Stakeholder pension balance

#### `wpayd` - Pay Codes/Deductions
Pay and deduction code definitions.

#### `whtran` - Wage History Transactions
Detailed transaction breakdown per pay period.

#### `wp32tr` - P32 Transactions (66 columns)
HMRC P32 summary records by period.

**Key Fields:**
- `w3_year` - Tax year
- `w3_period` - Pay period
- `w3_month` - Month number
- `w3_grtax` - Gross tax
- `w3_nttax` - Net tax
- `w3_grnic` - Gross NIC
- `w3_erni` - Employer NI total
- `w3_eeni` - Employee NI total
- `w3_paydate` - Pay date
- `w3_grpname` - Payroll group name

### Pension Scheme Tables

#### `wpnsc` - Pension Scheme Configuration (81 columns)
Defines pension schemes and provider details. **Critical for generating provider-specific exports.**

**Scheme Identity:**
- `wps_code` (PK) - Scheme code (e.g., "RCB01", "AUOTENROL")
- `wps_desc` - Description (e.g., "AvivA stakeholders pension", "Nest Autoenrolment scheme")
- `wps_type` - Scheme type (determines export format)
- `wps_ae` - Auto-enrolment flag

**Provider Contact:**
- `wps_prname` - Provider name
- `wps_pradd1` to `wps_pradd5` - Provider address
- `wps_prpscd` - Provider postcode
- `wps_prref` - Provider scheme reference (for remittance files)
- `wps_prtel` - Provider telephone
- `wps_preml` - Provider email
- `wps_prcnt` - Provider contact name

**Contribution Rates (scheme defaults):**
- `wps_erper` - Employer contribution %
- `wps_eeper` - Employee contribution %
- `wps_erlel` - Employer lower earnings limit
- `wps_eruel` - Employer upper earnings limit
- `wps_eelel` - Employee lower earnings limit
- `wps_eeuel` - Employee upper earnings limit

**Tax Treatment:**
- `wps_eebtax` - Employee benefit tax flag
- `wps_erbtax` - Employer benefit tax flag

#### `wpnpy` - Pension to Pay Code Mapping (8 columns)
Links pension schemes to payroll deduction codes.

- Maps scheme code to pay code (e.g., "RCB01" → "A010" Personal Contribution)

#### `wpnps` - Pension Plan Sections (7 columns)
Pension plan section definitions.

#### `wephld` - Pension Holds/Freezes (25 columns)
Records of pension contribution holds or freezes.

### Calendar Tables

#### `wclndr` - Payroll Calendar
Pay period calendar with dates.

**Key Fields:**
- `wd_group` - Payroll group
- `wd_payfrq` - Pay frequency ('M' = Monthly, 'W' = Weekly)
- `wd_taxyear` - Tax year
- `wd_period` - Period number
- `wd_startdt` - Period start date
- `wd_enddt` - Period end date
- `wd_paydt` - Pay date
- `wd_actpydt` - Actual pay date

### Tax & NI Tables

#### `wnitb` - NI Bands (42 columns)
National Insurance band thresholds and rates by category.

#### `wpaye` / `wpayec` - PAYE Configuration
Tax configuration and thresholds.

---

## Pension Export Query Pattern

To generate a pension provider export file, use this query pattern:

```sql
SELECT
    -- Employee identification
    w.wn_ref AS employee_code,
    w.wn_ninum AS ni_number,
    w.wn_surname AS surname,
    w.wn_forenam AS forename,
    w.wn_title AS title,
    w.wn_birth AS date_of_birth,
    w.wn_addrs1 AS address_1,
    w.wn_addrs2 AS address_2,
    w.wn_addrs3 AS address_3,
    w.wn_pstcde AS postcode,

    -- Pay period
    h.wh_year AS tax_year,
    h.wh_period AS pay_period,
    h.wh_paydt AS pay_date,

    -- Pension contributions
    h.wh_pen AS employee_contribution,
    h.wh_penbl AS pensionable_pay,
    h.wh_totpens AS ytd_employee_contributions,
    h.wh_totpenl AS ytd_pensionable_pay,

    -- Scheme details
    e.wep_code AS scheme_code,
    e.wep_erper AS employer_rate,
    e.wep_eeper AS employee_rate,
    e.wep_jndt AS scheme_join_date,

    -- Provider
    s.wps_prname AS provider_name,
    s.wps_prref AS provider_reference,
    s.wps_desc AS scheme_description

FROM wname w
INNER JOIN wepen e ON w.wn_ref = e.wep_ref
INNER JOIN whist h ON w.wn_ref = h.wh_ref
INNER JOIN wpnsc s ON e.wep_code = s.wps_code
WHERE h.wh_year = @tax_year
  AND h.wh_period = @period
  AND e.wep_lfdt IS NULL  -- Still enrolled
ORDER BY w.wn_surname, w.wn_forenam
```

---

## Pension Provider Export Formats

Different pension providers require different file formats:

| Provider | Format | Standard | Key Fields |
|----------|--------|----------|------------|
| NEST | CSV | NEST-specific | Employer ref, NI number, contributions |
| Smart Pension | CSV | PAPDIS 1.0 | Full PAPDIS specification |
| NOW: Pensions | CSV | Provider-specific | Similar to PAPDIS |
| Aviva | CSV | Provider-specific | Scheme reference, contributions |
| People's Pension | CSV | Provider-specific | Member ID, contributions |
| Scottish Widows | CSV | Provider-specific | Policy number, contributions |

### PAPDIS Standard Fields (for compliant providers)

PAPDIS (Payroll and Pension Data Interface Standard) common fields:
- Employer reference
- Employee NI number
- Employee name (title, forename, surname)
- Date of birth
- Gender
- Address (lines 1-4, postcode)
- Pensionable earnings
- Employee contribution amount
- Employer contribution amount
- Contribution percentage (employee/employer)
- Assessment date
- Enrolment type
- Group/section code

---

## Current Pension Schemes in Database

From `wpnsc` table:

| Code | Description | Type |
|------|-------------|------|
| RCB01 | AvivA stakeholders pension | Aviva |
| AUOTENROL | Nest Autoenrolment scheme | NEST |

---

## Tax Year Format

Opera uses a 4-digit tax year format:
- `2526` = Tax year 2025/26 (6 April 2025 - 5 April 2026)
- `2425` = Tax year 2024/25
- `2324` = Tax year 2023/24

Period numbers:
- Monthly: 1-12 (Period 1 = April, Period 12 = March)
- Weekly: 1-52/53
