# Opera Transaction Types - Database Field Reference

This document describes exactly which tables and fields are updated when each transaction type is entered in Opera SQL SE and Opera 3. This reference enables replication of Opera's behavior in any external application.

---

## Amount Storage Conventions

| Table | Unit | Notes |
|-------|------|-------|
| `aentry` | **PENCE** | Bank/cashbook entry header (multiply pounds by 100) |
| `atran` | **PENCE** | Bank/cashbook transaction detail (multiply pounds by 100) |
| `ntran` | **POUNDS** | Nominal ledger transactions |
| `stran` | **POUNDS** | Sales ledger transactions |
| `ptran` | **POUNDS** | Purchase ledger transactions |
| `salloc` | **POUNDS** | Sales ledger allocations |
| `palloc` | **POUNDS** | Purchase ledger allocations |

---

## Sign Conventions

### Bank/Cashbook (aentry/atran)
- **Payments** = NEGATIVE values
- **Receipts** = POSITIVE values

### Nominal Ledger (ntran)
- **Debits** = POSITIVE values
- **Credits** = NEGATIVE values

### Sales Ledger (stran)
- **Invoices** = POSITIVE (customer owes money)
- **Receipts/Credits** = NEGATIVE (reduces balance)

### Purchase Ledger (ptran)
- **Invoices** = POSITIVE (we owe supplier)
- **Payments/Credits** = NEGATIVE (reduces balance)

---

## 1. Sales Receipt (Customer Payment)

**Purpose**: Record payment received from a customer

### Tables Updated

| Table | Records | Purpose |
|-------|---------|---------|
| `aentry` | 1 | Bank entry header |
| `atran` | 1 | Bank transaction detail |
| `ntran` | 2 | Double-entry nominal (Bank DR, Debtors Control CR) |
| `stran` | 1 | Sales ledger transaction |
| `salloc` | 1 | Payment allocation record |
| `sname` | Update | Customer balance reduced |

### Field Details

#### aentry (Cashbook Entry Header)
```
ae_acnt      = bank_account (e.g., 'BC010')
ae_cntr      = '    ' (cost centre - blank)
ae_cbtype    = 'R5' (Receipt type 5)
ae_entry     = 'R5' + 8-digit sequence (e.g., 'R500000123')
ae_reclnum   = 0
ae_lstdate   = post_date
ae_frstat    = 0
ae_tostat    = 0
ae_statln    = 0
ae_entref    = reference[:20]
ae_value     = amount_pence (POSITIVE for receipt)
ae_recbal    = 0
ae_remove    = 0
ae_tmpstat   = 0
ae_complet   = 1
ae_postgrp   = 0
sq_crdate    = current_date
sq_crtime    = current_time
sq_cruser    = input_by[:8]
ae_comment   = ''
ae_payid     = 0
ae_batchid   = 0
ae_brwptr    = '  '
ae_recdate   = post_date
datecreated  = now
datemodified = now
state        = 1
```

#### atran (Cashbook Transaction)
```
at_acnt      = bank_account
at_cntr      = '    '
at_cbtype    = 'R5'
at_entry     = entry_number (same as aentry)
at_inputby   = input_by[:8]
at_type      = 4 (Receipt type code)
at_pstdate   = post_date
at_sysdate   = post_date
at_tperiod   = 1
at_value     = amount_pence (POSITIVE)
at_disc      = 0
at_fcurr     = '   '
at_fcexch    = 1.0
at_fcmult    = 0
at_fcdec     = 2
at_account   = customer_account
at_name      = customer_name[:35]
at_comment   = ''
at_payee     = '        '
at_payname   = ''
at_sort      = '        '
at_number    = '         '
at_remove    = 0
at_chqprn    = 0
at_chqlst    = 0
at_bacprn    = 0
at_ccdprn    = 0
at_ccdno     = ''
at_payslp    = 0
at_pysprn    = 0
at_cash      = 0
at_remit     = 0
at_unique    = unique_id (shared with stran)
at_postgrp   = 0
at_ccauth    = '0       '
at_refer     = reference[:20]
at_srcco     = 'I'
at_ecb       = 0
at_ecbtype   = ' '
at_atpycd    = '      '
at_bsref     = ''
at_bsname    = ''
at_vattycd   = '  '
at_project   = '        '
at_job       = '        '
at_bic       = ''
at_iban      = ''
at_memo      = ''
datecreated  = now
datemodified = now
state        = 1
```

#### ntran - Row 1 (DEBIT Bank Account)
```
nt_acnt      = bank_account
nt_cntr      = '    '
nt_type      = 'B ' (Balance Sheet - Bank)
nt_subt      = 'BC' (Bank Current)
nt_jrnl      = next_journal_number
nt_ref       = ''
nt_inp       = input_by[:10]
nt_trtype    = 'A'
nt_cmnt      = reference[:50] (padded to 50)
nt_trnref    = customer_name[:30] + payment_type[:10] + '(RT)     ' (50 chars)
nt_entr      = post_date
nt_value     = amount_pounds (POSITIVE = debit bank)
nt_year      = year
nt_period    = month
nt_rvrse     = 0
nt_prevyr    = 0
nt_consol    = 0
nt_fcurr     = '   '
nt_fvalue    = 0
nt_fcrate    = 0
nt_fcmult    = 0
nt_fcdec     = 0
nt_srcco     = 'I'
nt_cdesc     = ''
nt_project   = '        '
nt_job       = '        '
nt_posttyp   = 'R' (Receipt)
nt_pstgrp    = 0
nt_pstid     = unique_id_1
nt_srcnlid   = 0
nt_recurr    = 0
nt_perpost   = 0
nt_rectify   = 0
nt_recjrnl   = 0
nt_vatanal   = 0
nt_distrib   = 0
datecreated  = now
datemodified = now
state        = 1
```

#### ntran - Row 2 (CREDIT Debtors Control)
```
nt_acnt      = debtors_control (e.g., 'BB030')
nt_type      = 'B ' (Balance Sheet)
nt_subt      = 'BB' (Balance Sheet - Debtors)
nt_value     = -amount_pounds (NEGATIVE = credit)
nt_posttyp   = 'R' (Receipt)
nt_pstid     = unique_id_2
[other fields same as Row 1]
```

#### stran (Sales Ledger Transaction)
```
st_account   = customer_account
st_trdate    = post_date
st_trref     = reference[:20]
st_custref   = payment_type[:20]
st_trtype    = 'R' (Receipt)
st_trvalue   = -amount_pounds (NEGATIVE - reduces balance)
st_vatval    = 0
st_trbal     = -amount_pounds (outstanding)
st_paid      = ' '
st_crdate    = post_date
st_advance   = 'N'
st_memo      = ''
st_payflag   = 0
st_set1day   = 0
st_set1      = 0
st_set2day   = 0
st_set2      = 0
st_dueday    = 0
st_fcurr     = '   '
st_fcrate    = 0
st_fcdec     = 0
st_fcval     = 0
st_fcbal     = 0
st_fcmult    = 0
st_dispute   = 0
st_edi       = 0
st_editx     = 0
st_edivn     = 0
st_txtrep    = ''
st_binrep    = 0
st_advallc   = 0
st_cbtype    = 'R5'
st_entry     = entry_number
st_unique    = atran_unique
st_region    = '   '
st_terr      = '   '
st_type      = '   '
st_fadval    = 0
st_delacc    = customer_account
st_euro      = 0
st_payadvl   = 0
st_eurind    = ' '
st_origcur   = '   '
st_fullamt   = 0
st_fullcb    = '  '
st_fullnar   = '          '
st_cash      = 0
st_rcode     = '    '
st_ruser     = '        '
st_revchrg   = 0
st_nlpdate   = post_date
st_adjsv     = 0
st_fcvat     = 0
st_taxpoin   = post_date
datecreated  = now
datemodified = now
state        = 1
```

#### salloc (Sales Allocation)
```
al_account   = customer_account
al_date      = post_date
al_ref1      = reference[:20]
al_ref2      = payment_type[:20]
al_type      = 'R' (Receipt)
al_val       = -amount_pounds
al_dval      = 0
al_origval   = -amount_pounds
al_payind    = 'R'
al_payflag   = 0
al_payday    = post_date
al_ctype     = 'O' (Open)
al_rem       = ' '
al_cheq      = ' '
al_payee     = customer_name[:30]
al_fcurr     = '   '
al_fval      = 0
al_fdval     = 0
al_forigvl   = 0
al_fdec      = 0
al_unique    = stran_id (from stran insert)
al_acnt      = bank_account
al_cntr      = '    '
al_advind    = 0
al_advtran   = 0
al_preprd    = 0
al_bacsid    = 0
al_adjsv     = 0
datecreated  = now
datemodified = now
state        = 1
```

#### sname (Customer Master Update)
```sql
UPDATE sname SET
    sn_currbal = sn_currbal - amount_pounds,
    datemodified = now
WHERE sn_account = customer_account
```

---

## 2. Purchase Payment (Supplier Payment)

**Purpose**: Record payment made to a supplier

### Tables Updated

| Table | Records | Purpose |
|-------|---------|---------|
| `aentry` | 1 | Bank entry header |
| `atran` | 1 | Bank transaction detail |
| `ntran` | 2 | Double-entry nominal (Creditors Control DR, Bank CR) |
| `ptran` | 1 | Purchase ledger transaction |
| `palloc` | 1 | Payment allocation record |
| `pname` | Update | Supplier balance reduced |

### Field Details

#### aentry (Cashbook Entry Header)
```
ae_cbtype    = 'P5' (Payment type 5)
ae_entry     = 'P5' + 8-digit sequence (e.g., 'P500000123')
ae_value     = -amount_pence (NEGATIVE for payment)
[other fields similar to Sales Receipt]
```

#### atran (Cashbook Transaction)
```
at_cbtype    = 'P5'
at_type      = 5 (Payment type code)
at_value     = -amount_pence (NEGATIVE)
at_account   = supplier_account
at_name      = supplier_name[:35]
at_unique    = unique_id (shared with ptran)
[other fields similar to Sales Receipt]
```

#### ntran - Row 1 (CREDIT Bank Account)
```
nt_acnt      = bank_account
nt_type      = 'B '
nt_subt      = 'BC'
nt_value     = -amount_pounds (NEGATIVE = credit bank)
nt_posttyp   = 'P' (Payment)
nt_trnref    = supplier_name[:30] + payment_type[:10] + '(RT)     '
[other fields similar to Sales Receipt]
```

#### ntran - Row 2 (DEBIT Creditors Control)
```
nt_acnt      = creditors_control (e.g., 'CA030')
nt_type      = 'C ' (Current Liability)
nt_subt      = 'CA'
nt_value     = amount_pounds (POSITIVE = debit)
nt_posttyp   = 'P'
[other fields similar]
```

#### ptran (Purchase Ledger Transaction)
```
pt_account   = supplier_account
pt_trdate    = post_date
pt_trref     = reference[:20]
pt_supref    = payment_type[:20]
pt_trtype    = 'P' (Payment)
pt_trvalue   = -amount_pounds (NEGATIVE - reduces balance)
pt_vatval    = 0
pt_trbal     = -amount_pounds
pt_paid      = ' '
pt_crdate    = post_date
pt_advance   = 'N'
pt_payflag   = 0
pt_set1day   = 0
pt_set1      = 0
pt_set2day   = 0
pt_set2      = 0
pt_held      = ' '
pt_fcurr     = '   '
pt_fcrate    = 0
pt_fcdec     = 0
pt_fcval     = 0
pt_fcbal     = 0
pt_adval     = 0
pt_fadval    = 0
pt_fcmult    = 0
pt_cbtype    = 'P5'
pt_entry     = entry_number
pt_unique    = atran_unique
pt_suptype   = '   '
pt_euro      = 0
pt_payadvl   = 0
pt_origcur   = '   '
pt_eurind    = ' '
pt_revchrg   = 0
pt_nlpdate   = post_date
pt_adjsv     = 0
pt_vatset1   = 0
pt_vatset2   = 0
pt_pyroute   = 0
pt_fcvat     = 0
datecreated  = now
datemodified = now
state        = 1
```

#### palloc (Purchase Allocation)
```
al_account   = supplier_account
al_date      = post_date
al_ref1      = reference[:20]
al_ref2      = payment_type[:20]
al_type      = 'P' (Payment)
al_val       = -amount_pounds
al_dval      = 0
al_origval   = -amount_pounds
al_payind    = 'P'
al_payflag   = 0
al_payday    = post_date
al_ctype     = 'O'
al_rem       = ' '
al_cheq      = ' '
al_payee     = supplier_name[:30]
al_fcurr     = '   '
al_fval      = 0
al_fdval     = 0
al_forigvl   = 0
al_fdec      = 0
al_unique    = ptran_id
al_acnt      = bank_account
al_cntr      = '    '
al_advind    = 0
al_advtran   = 0
al_preprd    = 0
al_bacsid    = 0
al_adjsv     = 0
datecreated  = now
datemodified = now
state        = 1
```

#### pname (Supplier Master Update)
```sql
UPDATE pname SET
    pn_currbal = pn_currbal - amount_pounds,
    datemodified = now
WHERE pn_account = supplier_account
```

---

## 3. Sales Invoice

**Purpose**: Record invoice raised to a customer

### Tables Updated

| Table | Records | Purpose |
|-------|---------|---------|
| `stran` | 1 | Sales ledger invoice record |
| `ntran` | 3 | Triple-entry nominal (Debtors DR, Sales CR, VAT CR) |
| `nhist` | 1 | Nominal history for P&L account |
| `sname` | Update | Customer balance increased |

### Field Details

#### stran (Sales Ledger Transaction)
```
st_account   = customer_account
st_trdate    = post_date
st_trref     = invoice_number[:20]
st_custref   = customer_ref[:20] (PO number)
st_trtype    = 'I' (Invoice)
st_trvalue   = gross_amount (POSITIVE - customer owes)
st_vatval    = vat_amount
st_trbal     = gross_amount (outstanding)
st_paid      = ' '
st_crdate    = post_date
st_advance   = 'N'
st_memo      = memo_text[:2000] (detailed invoice breakdown)
st_payflag   = 0
st_set1day   = 0
st_set1      = 0
st_set2day   = 0
st_set2      = 0
st_dueday    = due_date
st_fcurr     = '   '
st_fcrate    = 0
st_fcdec     = 0
st_fcval     = 0
st_fcbal     = 0
st_fcmult    = 0
st_dispute   = 0
st_edi       = 0
st_editx     = 0
st_edivn     = 0
st_txtrep    = ''
st_binrep    = 0
st_advallc   = 0
st_cbtype    = '  '
st_entry     = '          '
st_unique    = unique_id
st_region    = customer_region[:3]
st_terr      = customer_territory[:3]
st_type      = customer_type[:3]
st_fadval    = 0
st_delacc    = customer_account
st_euro      = 0
st_payadvl   = 0
st_eurind    = ' '
st_origcur   = '   '
st_fullamt   = 0
st_fullcb    = '  '
st_fullnar   = '          '
st_cash      = 0
st_rcode     = '    '
st_ruser     = '        '
st_revchrg   = 0
st_nlpdate   = post_date
st_adjsv     = 0
st_fcvat     = 0
st_taxpoin   = post_date
datecreated  = now
datemodified = now
state        = 1
```

#### ntran - Row 1 (CREDIT VAT Output)
```
nt_acnt      = vat_nominal (e.g., 'CA060')
nt_type      = 'C ' (Current Liability)
nt_subt      = 'CA'
nt_jrnl      = next_journal_number
nt_ref       = '          '
nt_inp       = input_by[:10]
nt_trtype    = 'S' (Sales)
nt_cmnt      = invoice_number[:20] + description[:29] (50 chars)
nt_trnref    = customer_name[:30] + customer_ref[:20] (50 chars)
nt_entr      = post_date
nt_value     = -vat_amount (NEGATIVE = credit)
nt_year      = year
nt_period    = month
nt_posttyp   = 'I' (Invoice)
nt_pstid     = unique_id_vat
[other fields standard]
```

#### ntran - Row 2 (CREDIT Sales)
```
nt_acnt      = sales_nominal (e.g., 'E4030')
nt_type      = 'E ' (P&L - Income)
nt_subt      = sales_nominal[:2] (e.g., 'E4')
nt_value     = -net_amount (NEGATIVE = credit sales)
nt_project   = customer_account[:8]
nt_job       = department[:8]
nt_posttyp   = 'I'
nt_pstid     = unique_id_sales
[other fields standard]
```

#### ntran - Row 3 (DEBIT Debtors Control)
```
nt_acnt      = debtors_control (e.g., 'BB030')
nt_type      = 'B '
nt_subt      = 'BB'
nt_cmnt      = ''
nt_trnref    = 'Sales Ledger Transfer (RT)                        '
nt_value     = gross_amount (POSITIVE = debit)
nt_posttyp   = 'I'
nt_pstid     = unique_id_control
[other fields standard]
```

#### nhist (Nominal History)
```
nh_rectype   = 1
nh_ntype     = 'E '
nh_nsubt     = sales_subt
nh_nacnt     = sales_nominal
nh_ncntr     = '    '
nh_job       = department
nh_project   = customer_account
nh_year      = year
nh_period    = month
nh_bal       = -net_amount
nh_budg      = 0
nh_rbudg     = 0
nh_ptddr     = 0
nh_ptdcr     = -net_amount
nh_fbal      = 0
datecreated  = now
datemodified = now
state        = 1
```

#### sname (Customer Master Update)
```sql
UPDATE sname SET
    sn_currbal = sn_currbal + gross_amount,
    datemodified = now
WHERE sn_account = customer_account
```

---

## 4. Purchase Invoice Posting (Nominal Only)

**Purpose**: Post purchase invoice to nominal ledger (expense recognition)

### Tables Updated

| Table | Records | Purpose |
|-------|---------|---------|
| `ntran` | 2-3 | Double/triple-entry (PL Control CR, Expense DR, VAT DR) |

**Note**: This does NOT update ptran. Full purchase invoice entry would also create ptran record.

### Field Details

#### ntran - Row 1 (CREDIT Purchase Ledger Control)
```
nt_acnt      = purchase_ledger_control (e.g., 'CA030')
nt_type      = 'B ' (Balance Sheet)
nt_subt      = 'BB'
nt_jrnl      = next_journal_number
nt_ref       = invoice_number[:10]
nt_inp       = input_by[:10]
nt_trtype    = 'A' (Accrual/AP)
nt_cmnt      = invoice_number[:20] + description[:29]
nt_trnref    = supplier_name[:30] + 'Invoice             '
nt_entr      = post_date
nt_value     = -gross_amount (NEGATIVE = credit)
nt_year      = year
nt_period    = month
nt_posttyp   = 'S' (Supplier/Purchase)
nt_pstid     = unique_id_control
[other fields standard]
```

#### ntran - Row 2 (DEBIT Expense)
```
nt_acnt      = nominal_account (e.g., 'HA010')
nt_type      = 'P ' (P&L - Expense)
nt_subt      = 'HA' (from account code)
nt_value     = net_amount (POSITIVE = debit expense)
nt_posttyp   = 'S'
nt_pstid     = unique_id_expense
[other fields standard]
```

#### ntran - Row 3 (DEBIT VAT Input) - if VAT > 0
```
nt_acnt      = vat_account (e.g., 'BB040')
nt_type      = 'B '
nt_subt      = 'BB'
nt_value     = vat_amount (POSITIVE = debit VAT recoverable)
nt_posttyp   = 'S'
nt_pstid     = unique_id_vat
[other fields standard]
```

---

## 5. Nominal Journal

**Purpose**: General ledger journal entry (must balance)

### Tables Updated

| Table | Records | Purpose |
|-------|---------|---------|
| `ntran` | N | One row per journal line (debits + credits) |

### Validation
- Total of all line amounts MUST equal zero (journal must balance)
- Minimum 2 lines required

### Field Details

#### ntran (One per line)
```
nt_acnt      = line.account
nt_cntr      = '    '
nt_type      = 'J ' (Journal)
nt_subt      = 'JN' (Journal entry)
nt_jrnl      = next_journal_number (same for all lines)
nt_ref       = reference[:10]
nt_inp       = input_by[:10]
nt_trtype    = 'A'
nt_cmnt      = reference[:20] + line_description[:29]
nt_trnref    = 'Journal             ' (padded to 50)
nt_entr      = post_date
nt_value     = line.amount (POSITIVE = debit, NEGATIVE = credit)
nt_year      = year
nt_period    = month
nt_rvrse     = 0
nt_prevyr    = 0
nt_consol    = 0
nt_fcurr     = '   '
nt_fvalue    = 0
nt_fcrate    = 0
nt_fcmult    = 0
nt_fcdec     = 0
nt_srcco     = 'I'
nt_cdesc     = ''
nt_project   = '        '
nt_job       = '        '
nt_posttyp   = 'J' (Journal)
nt_pstgrp    = 0
nt_pstid     = unique_id (different for each line)
nt_srcnlid   = 0
nt_recurr    = 0
nt_perpost   = 0
nt_rectify   = 0
nt_recjrnl   = 0
nt_vatanal   = 0
nt_distrib   = 0
datecreated  = now
datemodified = now
state        = 1
```

---

## Account Type Codes (nt_type)

| Code | Description | Examples |
|------|-------------|----------|
| `A ` | Asset | Fixed assets |
| `B ` | Balance Sheet (Current) | Bank, Debtors, Stock |
| `C ` | Current Liability | Creditors, VAT |
| `D ` | Long-term Liability | Loans |
| `E ` | P&L Income | Sales, Interest received |
| `F ` | Cost of Sales | Direct costs |
| `G ` | Direct Labour | Wages (manufacturing) |
| `H ` | Direct Expenses | |
| `P ` | P&L Overheads | Rent, utilities, admin |
| `J ` | Journal | General journals |

---

## Transaction Type Codes

### atran.at_type (Cashbook)
| Code | Description |
|------|-------------|
| 4 | Receipt |
| 5 | Payment |

### stran.st_trtype (Sales Ledger)
| Code | Description |
|------|-------------|
| I | Invoice |
| C | Credit Note |
| R | Receipt |
| J | Journal |

### ptran.pt_trtype (Purchase Ledger)
| Code | Description |
|------|-------------|
| I | Invoice |
| C | Credit Note |
| P | Payment |
| J | Journal |

### ntran.nt_posttyp (Nominal Ledger Posting)
| Code | Description |
|------|-------------|
| I | Sales Invoice |
| R | Receipt |
| P | Payment |
| S | Purchase/Supplier |
| J | Journal |

---

## Unique ID Generation

Opera uses 20-character unique IDs with format: `YYYYMMDDHHMMSS` + 6 random chars

```python
import random
import string
from datetime import datetime

def generate_unique_id():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    random_chars = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{timestamp}{random_chars}"
```

---

## Control Account Discovery

Control accounts vary by installation. Use these methods:

### Debtors Control
1. Check customer profile: `sprfls.sc_dbtctrl` for the specific customer
2. Fall back to default: `nparm.np_dca`

### Creditors Control
1. Check supplier profile: `pprfls.pc_crdctrl` for the specific supplier
2. Fall back to default: `nparm.np_cca`

See `sql_rag/opera_config.py` for implementation.

---

## Opera 3 (FoxPro DBF) Differences

Opera 3 uses the same table structures but:

1. **Field names are UPPERCASE** (e.g., `SN_ACCOUNT` not `sn_account`)
2. **No SQL transactions** - must handle atomicity manually
3. **Dates as strings** in `YYYY-MM-DD` format
4. **Memo fields** stored in separate `.fpt` files
5. **Record locking** via exclusive file access

Use `sql_rag/opera3_foxpro.py` Opera3Reader class for DBF access.

---

## Locking Considerations

When writing directly to Opera database:

1. **SQL SE**: Use SQL transactions with `UPDLOCK, ROWLOCK` hints
2. **Set lock timeout**: `SET LOCK_TIMEOUT 5000` (5 seconds)
3. **Opera 3**: Use exclusive file locking on DBF files
4. **Best practice**: Use Opera COM automation for production systems

Direct database writes bypass Opera's application-level locking and should be used with caution.
