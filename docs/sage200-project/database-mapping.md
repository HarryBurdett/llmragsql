# Opera to Sage 200 - Database Mapping

## Overview

This document maps Opera SE database tables and fields to Sage 200 API resources.

## Ledger Mapping

### Nominal Ledger

| Opera Table | Opera Field | Sage 200 Resource | Sage 200 Field |
|-------------|-------------|-------------------|----------------|
| `nacnt` | `na_acnt` | `nominal_codes` | `code` |
| `nacnt` | `na_name` | `nominal_codes` | `name` |
| `nacnt` | `na_currbal` | `nominal_codes` | `balance` |
| `ntran` | `nt_acnt` | `nominal_transactions` | `nominal_code_id` |
| `ntran` | `nt_value` | `nominal_transactions` | `value` |
| `ntran` | `nt_date` | `nominal_transactions` | `transaction_date` |
| `ntran` | `nt_trnref` | `nominal_transactions` | `reference` |

### Cash Book

| Opera Table | Opera Field | Sage 200 Resource | Sage 200 Field |
|-------------|-------------|-------------------|----------------|
| `nbank` | `nk_code` | `bank_accounts` | `id` |
| `nbank` | `nk_name` | `bank_accounts` | `account_name` |
| `nbank` | `nk_curbal` | `bank_accounts` | `balance` |
| `nbank` | `nk_acnt` | `bank_accounts` | `nominal_code_id` |
| `aentry` | `ae_date` | `bank_posted_transactions` | `transaction_date` |
| `aentry` | `ae_entref` | `bank_posted_transactions` | `reference` |
| `atran` | `at_type` | `bank_posted_transactions` | `transaction_type` |
| `atran` | `at_value` | `bank_posted_transactions` | `value` |

**Transaction Type Mapping:**

| Opera at_type | Opera Description | Sage 200 Type |
|---------------|-------------------|---------------|
| 1 | Sales Receipt | `BankReceipt` |
| 2 | Sales Refund | `BankPayment` |
| 3 | Purchase Payment | `BankPayment` |
| 4 | Purchase Refund | `BankReceipt` |
| 5 | Other Payment | `BankPayment` |
| 6 | Other Receipt | `BankReceipt` |
| 8 | Bank Transfer | `BankTransfer` |

### Sales Ledger

| Opera Table | Opera Field | Sage 200 Resource | Sage 200 Field |
|-------------|-------------|-------------------|----------------|
| `sname` | `sn_acnt` | `customers` | `reference` |
| `sname` | `sn_name` | `customers` | `name` |
| `sname` | `sn_currbal` | `customers` | `balance` |
| `stran` | `st_type` | - | (derived from transaction) |
| `stran` | `st_value` | `sales_invoices/receipts` | `total_value` |
| `stran` | `st_date` | `sales_invoices/receipts` | `invoice_date` |
| `stran` | `st_ref` | `sales_invoices/receipts` | `reference` |

**Sales Transaction Type Mapping:**

| Opera st_type | Opera Description | Sage 200 Resource |
|---------------|-------------------|-------------------|
| I | Invoice | `sales_invoices` |
| C | Credit Note | `sales_credit_notes` |
| R | Receipt | `sales_receipts` |
| F | Refund | `sales_receipts` (negative) |

### Purchase Ledger

| Opera Table | Opera Field | Sage 200 Resource | Sage 200 Field |
|-------------|-------------|-------------------|----------------|
| `pname` | `pn_acnt` | `suppliers` | `reference` |
| `pname` | `pn_name` | `suppliers` | `name` |
| `pname` | `pn_currbal` | `suppliers` | `balance` |
| `ptran` | `pt_type` | - | (derived from transaction) |
| `ptran` | `pt_value` | `purchase_invoices/payments` | `total_value` |
| `ptran` | `pt_date` | `purchase_invoices/payments` | `invoice_date` |
| `ptran` | `pt_ref` | `purchase_invoices/payments` | `reference` |

## Amount Handling

### Opera

- `aentry` / `atran`: Amounts in **PENCE** (integer)
- `ntran` / `ptran` / `stran`: Amounts in **POUNDS** (decimal)

### Sage 200

- All amounts in **POUNDS** (decimal)

### Conversion

```python
# Opera to Sage 200
sage_amount = opera_pence_amount / 100

# Sage 200 to Opera (if needed)
opera_pence = int(round(sage_amount * 100))
```

## Analysis Codes / Dimensions

### Opera

| Field | Description |
|-------|-------------|
| `ae_cost` | Cost centre |
| `ae_dept` | Department |

### Sage 200

Sage 200 uses Analysis Codes (T1 - T10):

| Analysis Code | Typical Use |
|---------------|-------------|
| T1 | Cost Centre |
| T2 | Department |
| T3 | Project |
| T4-T10 | Custom |

### Mapping

```python
# Map Opera cost centre to Sage 200 analysis code
analysis_codes = {
    "T1": opera_cost_centre,
    "T2": opera_department
}
```

## Period Handling

### Opera

- Custom period definition in `nsprd` table
- Period format: Year (2 digit) + Period (2 digit)
- Example: `2501` = Period 1 of year 25 (Jan 2025)

### Sage 200

- Standard fiscal year periods
- Automatic period determination from transaction date
- API validates date against open periods

### Validation

```python
# Sage 200 will reject transactions in closed periods
# Check period status before posting:
# GET /financial_years
# Verify transaction date falls in open period
```

## VAT / Tax Codes

### Opera

| Table | Field | Description |
|-------|-------|-------------|
| `vatcd` | `vc_code` | VAT code |
| `vatcd` | `vc_rate` | VAT rate |
| `zvtran` | - | VAT transaction record |
| `nvat` | - | VAT return record |

### Sage 200

| Resource | Field | Description |
|----------|-------|-------------|
| `tax_codes` | `id` | Tax code ID |
| `tax_codes` | `rate` | Tax rate |

### Mapping

Standard UK VAT codes typically map:

| Opera Code | Sage 200 ID | Description |
|------------|-------------|-------------|
| 0 | 0 | Zero Rated |
| 1 | 1 | Standard Rate |
| 2 | 2 | Exempt |
| 5 | 5 | Reduced Rate |

**Note:** Actual IDs vary by Sage 200 installation - query `tax_codes` endpoint.

## Unique ID Generation

### Opera

- Format: `_XXXXXXXXX` (underscore + 9 base-36 chars)
- Generated from timestamp

### Sage 200

- IDs are auto-generated by API
- Use returned `id` for subsequent operations
- Store mapping: Opera reference → Sage 200 ID

## Allocation Records

### Opera

| Table | Purpose |
|-------|---------|
| `salloc` | Sales ledger allocations |
| `palloc` | Purchase ledger allocations |

### Sage 200

| Resource | Purpose |
|----------|---------|
| `sales_receipt_allocations` | Allocate receipt to invoice |
| `purchase_payment_allocations` | Allocate payment to invoice |

### Process

1. Post receipt/payment (returns transaction ID)
2. Query outstanding invoices
3. Post allocation record linking receipt to invoice

```python
# Post receipt
receipt = client.post("/sales_receipts", {...})
receipt_id = receipt["id"]

# Get outstanding invoices
invoices = client.get(f"/sales_invoices?customer_id={customer_id}&outstanding_only=true")

# Allocate to invoice
client.post("/sales_receipt_allocations", {
    "sales_receipt_id": receipt_id,
    "sales_invoice_id": invoices[0]["id"],
    "allocation_value": amount
})
```

## Batch Types

### Opera

| Table | Field | Description |
|-------|-------|-------------|
| `atype` | `ay_cbtype` | Batch type code |
| `atype` | `ay_desc` | Description |

### Sage 200

- No direct equivalent
- Use `reference` prefix to identify batch source
- Example: `GC-` for GoCardless, `BACS-` for bank payments

## Control Accounts

### Opera

Control accounts stored in:
- `sprfls.sc_dbtctrl` - Debtors control
- `sprfls.pc_crdctrl` - Creditors control
- `nparm` - Backup location

### Sage 200

Control accounts are:
- Pre-configured in Sage 200 setup
- Automatically used when posting to SL/PL
- Query via `GET /company_settings` for codes

## Migration Considerations

When implementing, consider:

1. **Account Code Mapping** - Opera and Sage 200 may use different code structures
2. **Customer/Supplier References** - May need mapping table
3. **Opening Balances** - Don't migrate history, start from cut-over date
4. **Dual Running** - Consider period of parallel operation
