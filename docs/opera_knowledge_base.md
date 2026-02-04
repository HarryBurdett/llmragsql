# Opera SQL SE Knowledge Base

This document captures knowledge about Pegasus Opera SQL SE database structure, conventions, and integration patterns learned through development.

## Database Tables

### Account/Transaction Tables

#### `aentry` - Bank Account Entries (Header)
- `ae_acnt` - Bank account code (e.g., 'BARC', 'HSBC')
- `ae_ref` - Entry reference number
- `ae_date` - Entry date
- `ae_type` - Entry type
- `ae_value` - Entry value in **PENCE** (not pounds)
- `ae_batch` - Batch reference

#### `atran` - Bank Account Transactions (Detail)
- `at_acnt` - Bank account code
- `at_ref` - Transaction reference
- `at_pstdate` - Post date
- `at_value` - Transaction value in **PENCE**
  - **Payments are stored as NEGATIVE values**
  - **Receipts are stored as POSITIVE values**
- `at_type` - Transaction type

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
- Control accounts: 'DA010' (Debtors), 'CA010' (Creditors)

## Key Conventions

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

- **Debtors Control**: Usually 'DA010'
- **Creditors Control**: Usually 'CA010'
- These are the nominal accounts that should reconcile to the respective ledger totals

## Reconciliation Formulas

### Creditors (Purchase Ledger)
```
PL Total = SUM(pt_trbal) from ptran for active suppliers
NL Total = SUM(nt_value) from ntran where nt_acnt = 'CA010'
Variance = NL Total - PL Total
```

### Debtors (Sales Ledger)
```
SL Total = SUM(st_trbal) from stran for active customers
NL Total = SUM(nt_value) from ntran where nt_acnt = 'DA010'
Variance = NL Total - SL Total
```
