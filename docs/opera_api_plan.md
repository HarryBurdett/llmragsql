# Opera Integration API - Planning Document

**Created:** 2026-02-11
**Status:** Planning / Incremental Development

---

## Goal

Create a comprehensive Opera Integration API that:
1. Encapsulates all knowledge about writing to Opera databases
2. Can be used within SQL RAG and potentially extracted as standalone service later
3. Interprets transactions, formats appropriately, and requests additional fields as needed
4. Handles validation, business rules, and audit trails

---

## Architecture Decision

**Approach:** Build within SQL RAG first, structure for later extraction

```
sql_rag/
├── opera_integration/           # New package
│   ├── __init__.py              # Public API
│   ├── sales_ledger.py          # Receipts, allocations
│   ├── purchase_ledger.py       # Payments, allocations
│   ├── cashbook.py              # Transactions, reconciliation
│   ├── rules.py                 # Allocation rules engine
│   ├── validation.py            # Business validation
│   ├── config.py                # Opera config
│   └── models.py                # Data classes/types
```

---

## Opera Ledgers

Opera contains multiple ledgers that interact with each other:

| Ledger | Tables | Status |
|--------|--------|--------|
| **Sales Ledger (SL)** | sname, stran, salloc, snoml | Partially known |
| **Purchase Ledger (PL)** | pname, ptran, palloc, pnoml | To learn |
| **Nominal Ledger (NL)** | nacnt, ntran, nbank | Partially known |
| **Cashbook (CB)** | aentry, atran, atype, anoml | Well known |
| **Stock** | ? | To learn |
| **Sales Order Processing** | ? | To learn |
| **Purchase Order Processing** | ? | To learn |
| **Bill of Materials** | ? | To learn |
| **Payroll** | ? | To learn |
| **Fixed Assets** | ? | To learn |
| **Job Costing** | ? | To learn |

**Approach:** Focus on one ledger at a time, understanding how it impacts other ledgers.

---

## What We Already Know

### Sales Ledger - Receipts & Allocations

**Tables updated for Sales Receipt:**
- `aentry` - Cashbook entry header
- `atran` - Cashbook transaction detail
- `stran` - Sales ledger transaction
- `salloc` - Sales allocation record
- `sname` - Customer balance update (sn_currbal)
- `anoml` - Cashbook to NL transfer file
- `snoml` - Sales to NL transfer file
- `ntran` - Nominal ledger transactions
- `nacnt` - Nominal account balances
- `nbank` - Bank balance update

**Allocation Rules (Implemented):**
1. Invoice reference match - INV ref in description, amounts must match exactly
2. Clears account - 2+ invoices, total matches exactly, no partial allocations
3. Single invoice with no ref - REJECTED (should have reference)

**Allocation Audit Trail:**
- `salloc.al_ref2` = 'AUTO:INV_REF' or 'AUTO:CLR_ACCT'

**Key Principles:**
- Amounts must add up exactly (no tolerance in accounting)
- All related tables must be updated (control account reconciliation)
- Period posting rules respected

### Cashbook

**Transaction Types (atype.at_type):**
- 1 = Bank Payment
- 2 = Bank Receipt
- 3 = Sales Receipt
- 4 = Purchase Payment
- 5 = Sales Refund
- 6 = Purchase Refund

**Tables:**
- `atype` - Entry number sequences per cashbook type
- `aentry` - Entry headers
- `atran` - Transaction details
- `anoml` - Transfer to Nominal Ledger

### Configuration

**Control Accounts:**
- Source: `sprfls.sc_dbtctrl` (debtors), `pprfls.pc_crdctrl` (creditors)
- Fallback: `nparm.np_dca`, `nparm.np_cca`

**Period Handling:**
- Current period: `nparm.np_year`, `nparm.np_perno`
- Period status: `nclndd.ncd_slstat`

**Unique IDs:**
- Format: `_XXXXXXXXX` (underscore + 9 base-36 characters)
- Used in: `at_unique`, `st_unique`, `nt_pstid`, etc.

---

## What We Need to Learn

### Priority 1: Purchase Ledger
- [ ] Purchase Payment - trace table updates
- [ ] Purchase Allocation - trace table updates
- [ ] Purchase Refund
- [ ] Parameter screens

### Priority 2: Complete Sales Ledger
- [ ] Sales Invoice creation (if needed)
- [ ] Sales Credit Note
- [ ] Full parameter screens

### Priority 3: Nominal Ledger
- [ ] Journal entries
- [ ] Direct NL postings
- [ ] Period end processing

### Priority 4: Other Ledgers
- [ ] Stock
- [ ] Sales Order Processing
- [ ] Purchase Order Processing
- [ ] Others as needed

---

## Information Required (Per Ledger)

1. **Example Transactions** - Enter in Opera, trace which tables/fields update
2. **Parameter Screens** - Screenshots of all related settings
3. **Validation Rules** - What Opera validates before posting
4. **Cross-Ledger Impact** - How posting affects other ledgers

---

## API Design Principles

1. **Interpret transactions** - Accept high-level request, determine required format
2. **Request missing data** - If fields missing, return what's needed
3. **Validate before posting** - Check all rules before writing
4. **Atomic operations** - All-or-nothing transactions
5. **Audit trail** - Record how each transaction was processed
6. **Amounts must balance** - No tolerance on financial transactions

---

## Next Steps

1. Choose first ledger to focus on (suggest: Purchase Ledger)
2. User provides example transaction traces
3. User provides parameter screen screenshots
4. Build and test that ledger's API
5. Move to next ledger

---

## Notes

- Opera 3 (FoxPro) support: Same table structures, different database access
- Foreign currency: To be determined if needed
- VAT handling: To be determined
