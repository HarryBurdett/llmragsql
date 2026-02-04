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

### Locking Concerns
- Direct database writes bypass Opera's application-level locking
- Opera maintains in-memory locks, session locks, and batch posting queues
- **Recommended**: Use Opera's COM automation or standard import files instead of direct writes
- SQL Server row-level locks (UPDLOCK, HOLDLOCK, ROWLOCK) help but don't replace Opera's locking

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

1. **Primary location**: `sprfls` table (Sales/Purchase Profiles)
   - `sc_dbtctrl` - Debtors control account (Sales Control)
   - `pc_crdctrl` - Creditors control account (Purchase Control)

2. **Fallback location**: `nparm` table (Nominal Parameters) - used if sprfls fields are blank
   - `np_dca` - Debtors control account
   - `np_cca` - Creditors control account

**To retrieve control accounts programmatically:**
```sql
-- Get debtors control account (from sprfls, fallback to nparm)
SELECT COALESCE(NULLIF(sc_dbtctrl, ''), (SELECT np_dca FROM nparm)) AS debtors_control
FROM sprfls

-- Get creditors control account (from sprfls, fallback to nparm)
SELECT COALESCE(NULLIF(pc_crdctrl, ''), (SELECT np_cca FROM nparm)) AS creditors_control
FROM sprfls
```

These are the nominal accounts that should reconcile to the respective ledger totals.

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
