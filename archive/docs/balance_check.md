# Balance Check Guide (Internal Reconciliation)

## Overview

Balance Check (also called Control Account Reconciliation) verifies that Opera's internal balances agree across different modules. This is essential for financial accuracy and audit readiness.

### What Does It Do?

1. **Compares sub-ledger totals** to control accounts in the Nominal Ledger
2. **Identifies variances** where balances don't match
3. **Drills down** into discrepancies to identify root causes
4. **Validates data integrity** across Opera modules

### Important Distinction

**Balance Check** (this feature) is different from **Cashbook Reconcile** (bank statement reconciliation):

| Feature | Purpose | Location |
|---------|---------|----------|
| **Balance Check** | Internal balance verification | Utilities > Balance Check |
| **Cashbook Reconcile** | Bank statement matching | Cashbook > Cashbook Reconcile |

---

## Balance Check Types

### 1. Debtors Balance Check

Compares **Sales Ledger** total to **Debtors Control Account** in Nominal Ledger.

**Should Match:**
- Sum of all customer balances (`sname.sn_currbal`)
- Debtors Control Account balance (`nacnt` for control code)

### 2. Creditors Balance Check

Compares **Purchase Ledger** total to **Creditors Control Account** in Nominal Ledger.

**Should Match:**
- Sum of all supplier balances (`pname.pn_currbal`)
- Creditors Control Account balance (`nacnt` for control code)

### 3. Cashbook Balance Check

Compares three values that should all agree:
- **Cashbook balance** (sum of `atran`)
- **Bank Master balance** (`nbank.nk_curbal`)
- **Nominal Ledger balance** (`nacnt` for bank account)

### 4. VAT Balance Check

Compares **VAT Return boxes** to underlying transaction values:
- Output VAT (Box 1) vs Sales transactions
- Input VAT (Box 4) vs Purchase transactions
- Net values (Box 6, 7) vs ledger totals

---

## Debtors Balance Check

### What It Checks

| Component | Source | Description |
|-----------|--------|-------------|
| Sales Ledger Total | `sname.sn_currbal` | Sum of all customer balances |
| Control Account | `nacnt` | Debtors control account balance |
| Variance | Calculated | Difference (should be £0.00) |

### Control Account Location

The Debtors Control Account code is found in:
1. `sprfls.sc_dbtctrl` - Sales profile defaults
2. `nparm.np_dca` - Nominal parameters (fallback)

### Drill-Down Analysis

When variance exists, system shows:
- **Customer list** with individual balances
- **Unposted transactions** in `stran` not yet in NL
- **Timing differences** between ledgers
- **Orphan entries** in one ledger but not other

### Common Causes of Variance

| Issue | Cause | Solution |
|-------|-------|----------|
| Unposted journals | Sales not transferred to NL | Run NL transfer |
| Direct NL posting | Posted to control without SL | Investigate and correct |
| Corrupted balance | `sn_currbal` incorrect | Rebuild customer balances |
| Missing transfer | snoml entries not processed | Process transfer file |

---

## Creditors Balance Check

### What It Checks

| Component | Source | Description |
|-----------|--------|-------------|
| Purchase Ledger Total | `pname.pn_currbal` | Sum of all supplier balances |
| Control Account | `nacnt` | Creditors control account balance |
| Variance | Calculated | Difference (should be £0.00) |

### Control Account Location

The Creditors Control Account code is found in:
1. `sprfls.pc_crdctrl` - Purchase profile defaults
2. `nparm.np_cca` - Nominal parameters (fallback)

### Drill-Down Analysis

When variance exists, system shows:
- **Supplier list** with individual balances
- **Unposted transactions** in `ptran` not yet in NL
- **Timing differences** between ledgers
- **Orphan entries** in one ledger but not other

### Common Causes of Variance

| Issue | Cause | Solution |
|-------|-------|----------|
| Unposted journals | Purchases not transferred to NL | Run NL transfer |
| Direct NL posting | Posted to control without PL | Investigate and correct |
| Corrupted balance | `pn_currbal` incorrect | Rebuild supplier balances |
| Missing transfer | pnoml entries not processed | Process transfer file |

---

## Cashbook Balance Check

### What It Checks

For each bank account, three values compared:

| Component | Source | Description |
|-----------|--------|-------------|
| Cashbook Total | `atran` | Sum of cashbook transactions |
| Bank Master | `nbank.nk_curbal` | Bank current balance (in pence) |
| Nominal Ledger | `nacnt` | Bank nominal account balance |

### Three-Way Match

All three should agree:
```
Cashbook Total = Bank Master Balance = Nominal Ledger Balance
```

### Bank Account Selection

Select bank from dropdown:
- Lists all bank accounts from `nbank`
- Shows current balance for each
- Code format: BC010, BC020, etc.

### Common Causes of Variance

| Issue | Cause | Solution |
|-------|-------|----------|
| NL not updated | `nacnt` balances not updated | Check import routine |
| nbank not updated | Bank master balance stale | Update nbank balance |
| Incomplete posting | Only partial tables updated | Complete the posting |
| Direct NL edit | Manual NL entry without cashbook | Investigate entry |

---

## VAT Balance Check

### What It Checks

VAT Return boxes against underlying data:

| Box | Description | Source |
|-----|-------------|--------|
| Box 1 | Output VAT | Sales transactions VAT |
| Box 2 | VAT on acquisitions | EC acquisitions |
| Box 3 | Total VAT due | Box 1 + Box 2 |
| Box 4 | Input VAT | Purchase transactions VAT |
| Box 5 | Net VAT | Box 3 - Box 4 |
| Box 6 | Net sales | Sales ex-VAT |
| Box 7 | Net purchases | Purchases ex-VAT |
| Box 8 | EC supplies | EC sales |
| Box 9 | EC acquisitions | EC purchases |

### Variance Analysis

When VAT boxes don't match:
- **Transaction breakdown** by VAT code
- **Date range analysis** for period
- **Unposted transactions** affecting VAT
- **Journal entries** to VAT accounts

### VAT Code Rates

System shows applicable VAT rates:
- Rate based on effective date
- Historical rate changes tracked
- `ztax.tx_rate1`, `tx_rate2` with dates

---

## Using Balance Check

### Step 1: Navigate to Balance Check

1. Go to **Utilities > Balance Check**
2. Select the check type:
   - Debtors
   - Creditors
   - Cashbook
   - VAT

### Step 2: Review Summary

Summary panel shows:
- **Control Account Balance**
- **Sub-Ledger Total**
- **Variance** (highlighted if non-zero)
- **Status** indicator (green/red)

### Step 3: Investigate Variances

If variance exists:
1. Expand detail sections
2. Review transaction lists
3. Identify discrepancies
4. Note items requiring correction

### Step 4: Take Corrective Action

Common actions:
- Run transfer routines (snoml, pnoml, anoml)
- Correct incorrect postings
- Rebuild sub-ledger balances
- Investigate orphan entries

---

## Status Indicators

| Indicator | Meaning |
|-----------|---------|
| **Green checkmark** | Balances match (£0.00 variance) |
| **Red X** | Balances don't match |
| **Amber warning** | Small variance (rounding) |

---

## API Endpoints

### Balance Check Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/debtors` | GET | Debtors balance check |
| `/api/reconcile/creditors` | GET | Creditors balance check |
| `/api/reconcile/banks` | GET | List bank accounts |
| `/api/reconcile/bank/{code}` | GET | Cashbook balance for bank |
| `/api/reconcile/vat` | GET | VAT reconciliation |

### Summary Endpoint

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/summary` | GET | All balance checks summary |
| `/api/reconcile/trial-balance` | GET | Trial balance report |

### VAT Diagnostic

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/vat/diagnostic` | GET | VAT detailed breakdown |
| `/api/reconcile/vat/variance-drilldown` | GET | VAT variance analysis |

---

## File Structure

```
llmragsql/
├── api/main.py                    # API endpoints (~line 7001)
├── sql_rag/
│   └── opera_config.py            # Control account lookup
├── frontend/src/pages/
│   ├── DebtorsReconcile.tsx       # Debtors UI
│   ├── CreditorsReconcile.tsx     # Creditors UI
│   ├── CashbookReconcile.tsx      # Cashbook UI
│   ├── VATReconcile.tsx           # VAT UI
│   └── Reconcile.tsx              # Summary page
└── docs/
    └── balance_check.md           # This document
```

---

## Troubleshooting

### "Control account not found"

- Check `sprfls` table has control codes set
- Verify `nparm` fallback values
- Confirm account exists in `nacnt`

### Large Unexplained Variance

- Check for unprocessed transfer files
- Look for direct NL postings
- Review recent period-end processes
- Check for corrupted balances

### Intermittent Variance

- Timing issue with transfers
- Refresh data and recheck
- May resolve after next posting run

### VAT Not Matching

- Check VAT period dates
- Verify transaction dates in period
- Review VAT code assignments
- Check for non-standard VAT rates

---

## Best Practices

1. **Check Regularly** - Run balance checks monthly minimum
2. **Before Period End** - Verify balances before closing periods
3. **After Imports** - Check after large batch imports
4. **Document Variances** - Record investigation notes
5. **Timely Correction** - Fix issues before they compound
6. **Audit Trail** - Keep records of corrections made

---

## Relationship to Other Features

| Feature | Relationship |
|---------|--------------|
| Bank Statement Import | Creates entries that affect cashbook balance |
| GoCardless Import | Creates sales receipts affecting debtors |
| Cashbook Reconcile | Different feature - matches to bank statement |
| VAT Return | Uses same data as VAT balance check |
