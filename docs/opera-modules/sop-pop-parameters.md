# Opera Module Parameters — SOP, POP, Sales, Purchase, Stock

Captured from live Opera SE test system (2026-04-09).

---

## iparm — Sales Order Processing Parameters

### Document Progression Controls

| Field | Value | Meaning |
|-------|-------|---------|
| `ip_delivry` | Y | Delivery stage is enabled |
| `ip_picking` | N | Picking stage not used |
| `ip_updst` | D | Stock updated at **D**elivery stage (O=Order, D=Delivery, I=Invoice) |
| `ip_forceal` | N | Don't force stock allocation before progression |
| `ip_cndl2in` | Y | Can progress delivery to invoice |
| `ip_cnor2dl` | N | Cannot skip direct from order to delivery via batch |
| `ip_cnor2in` | N | Cannot skip direct from order to invoice |
| `ip_porders` | Y | Purchase orders enabled (back-to-back) |
| `ip_pordreq` | Y | PO required before progressing |
| `ip_wordreq` | Y | Works order required |
| `ip_forward` | N | Forward ordering disabled |

### Document Numbering (Sequence Counters)

| Field | Current Value | Meaning |
|-------|---------------|---------|
| `ip_docno` | DOC05283 | Next document number |
| `ip_quotno` | QUO00108 | Next quotation number |
| `ip_profno` | PRO00011 | Next proforma number |
| `ip_orderno` | ORD01098 | Next order number |
| `ip_deliv` | DEL01294 | Next delivery note number |
| `ip_invno` | INV05230 | Next invoice number |
| `ip_credno` | CRE00027 | Next credit note number |

### UI/Behaviour Settings

| Field | Value | Meaning |
|-------|-------|---------|
| `ip_headfir` | Y | Header entry first (before lines) |
| `ip_immedpr` | Y | Immediate print after progression |
| `ip_stkmemo` | Y | Show stock memo on entry |
| `ip_showcst` | Y | Show cost price |
| `ip_showsp` | Y | Show selling price |
| `ip_save` | F | Save mode: F = Full |
| `ip_condate` | Y | Confirm date on entry |
| `ip_adar` | Y | Address auto-refresh from customer master |
| `ip_wrnmarg` | A | Margin warning: A = Always |
| `ip_restric` | N | No restrictions on document editing |
| `ip_whedit` | N | Warehouse not editable on lines |
| `ip_months` | 12 | Months for analysis |
| `ip_cashacc` | XXX | Cash account code |

### Batch Processing Settings

| Field | Value | Meaning |
|-------|-------|---------|
| `ip_batchex` | N | Batch export disabled |
| `ip_all2pik` | N | Don't auto-allocate to picking |
| `ip_autoext` | N | No auto-extension |

### User-Defined Number Ranges

| Field | Value | Meaning |
|-------|-------|---------|
| `ip_userdel` | N | User-defined delivery numbers: No |
| `ip_userinv` | N | User-defined invoice numbers: No |
| `ip_userord` | N | User-defined order numbers: No |

---

## pparm — Purchase Ledger Parameters

### Payment Methods (12 slots)

| Slot | Method | Cashbook Type | Cash? |
|------|--------|--------------|-------|
| 01 | Cheque | P1 | No |
| 02 | Cash | P1 | No |
| 03 | BACS | P2 | No |
| 04 | DirectDeb | P1 | No |

### Refund Types

| Slot | Description |
|------|-------------|
| 01 | Refund |
| 02 | Cancelled |

### Refund Cashbook Types

| Slot | Type |
|------|------|
| 01 | R4 |
| 02 | R5 |

### Key Nominal Accounts

| Field | Value | Purpose |
|-------|-------|---------|
| `pp_banknom` | C310 | Default bank nominal |
| `pp_discnom` | M410 | Discount nominal |
| `pp_woffnm` | M410 | Write-off nominal |
| `pp_xchgnom` | O190 | Exchange gain/loss nominal |

### Adjustment Types

| Slot | Description | Nominal |
|------|-------------|---------|
| 01 | Discount | M410 |
| 02 | Mispost | M999 |
| 03 | Bal W/Off | U310 |

### Other Settings

| Field | Value | Meaning |
|-------|-------|---------|
| `pp_palloc` | True | Auto-allocate payments |
| `pp_approve` | N | Payment approval not required |
| `pp_advance` | N | Advance payment not used |
| `pp_period` | Monthly | Reporting period |
| `pp_percday` | 12 | Period end day |
| `pp_chist` | 24 | Cheque history months |
| `pp_retain` | 36 | Retain months |
| `pp_sugdays` | 30 | Suggested payment days |
| `pp_bacs` | HSBC | BACS bank name |
| `pp_nlcoid` | Z | NL company ID |
| `pp_incdormant` | False | Exclude dormant suppliers |
| `pp_zero` | N | Don't show zero balance accounts |

---

## sparm — Sales Ledger Parameters

### Receipt Methods (12 slots)

| Slot | Method | Cashbook Type | Cash? |
|------|--------|--------------|-------|
| 01 | Cheque | R1 | No |
| 02 | Cash | R1 | Yes |
| 03 | BACS | R2 | No |
| 04 | CreditCard | R3 | No |

### Refund Types

| Slot | Description | CC Refund? |
|------|-------------|-----------|
| 01 | Refund | No |
| 02 | Cancelled | No |
| 03 | C/Card Rfd | Yes |

### Refund Cashbook Types

| Slot | Type |
|------|------|
| 01 | P6 |
| 02 | P6 |
| 03 | P6 |

### Key Nominal Accounts

| Field | Value | Purpose |
|-------|-------|---------|
| `sp_banknom` | C310 | Default bank nominal |
| `sp_discnom` | K130 | Discount nominal |
| `sp_woffnm` | U225 | Write-off nominal |
| `sp_xchgnom` | O190 | Exchange gain/loss nominal |

### Adjustment Types

| Slot | Description | Nominal |
|------|-------------|---------|
| 01 | Write-off | U220 |
| 02 | Discount | K130 |
| 03 | Mispost | K999 |
| 04 | VATAmend | E220 |
| 05 | Reduction | U221 |
| 06 | BadDebtRel | E220 |

### Credit Control

| Field | Value | Meaning |
|-------|-------|---------|
| `sp_defcrlm` | 5000 | Default credit limit |
| `sp_cost` | Y | Show cost on transactions |
| `sp_slcntr` | Y | Sales ledger control enabled |
| `sp_statcr` | N | Statement credits: No |
| `sp_cashinv` | True | Cash invoices enabled |
| `sp_ovrbank` | True | Override bank on receipts |

### Debt Chasing

3 letter templates stored in `sp_debt1`, `sp_debt2`, `sp_debt3` with merge fields:
- `^CUSTOMER NAME`, `^ADDRESS LINE 1-4`, `^POSTCDE`
- `^ACCOUNT`, `^BALANCE`, `^DATE`
- `^AGE 1-3`, `^PERIOD 1-3`
- `^CONTACT NAME 1`

---

## cparm — Stock/Costing Parameters

| Field | Value | Meaning |
|-------|-------|---------|
| `cp_defware` | MAIN | Default warehouse |
| `cp_locflag` | True | Location tracking enabled |
| `cp_track` | True | Stock tracking enabled |
| `cp_showqty` | True | Show quantities |
| `cp_showsp` | True | Show selling price |
| `cp_frcgrn` | False | Don't force GRN |
| `cp_bomlev` | T | BOM level: T (Top) |
| `cp_bmisal` | O | BOM misallocation: O |
| `cp_grnref` | GRN00301 | Next GRN reference |
| `cp_rtvref` | RTV00010 | Next return to vendor reference |
| `cp_woref` | WO00162 | Next works order reference |
| `cp_keeptrc` | 12 | Keep trace months |
| `cp_nlcoid` | Z | NL company ID |
| `cp_incdormant` | False | Exclude dormant stock items |
| `cp_takepr` | A | Stocktake profile: A |

---

## Document Status Codes (ih_docstat)

| Code | Status | Document Type |
|------|--------|--------------|
| Q | Quote | Quotation |
| P | Proforma | Proforma invoice |
| O | Order | Sales order |
| D | Delivery | Delivery note / despatch |
| U | Despatched | Goods despatched |
| I | Invoice | Sales invoice |
| C | Credit | Credit note |

### Progression Chain

```
Q (Quote) → P (Proforma) → O (Order) → D/U (Delivery) → I (Invoice)
                                                          ↓
                                                     C (Credit)
```

**Controlled by iparm:**
- `ip_delivry = Y/N` — whether delivery stage exists
- `ip_updst = O/D/I` — when stock is updated
- `ip_cnor2in = Y/N` — can skip delivery and go order → invoice
- `ip_cndl2in = Y/N` — can progress delivery → invoice

### Revision Flags (on ihead)

| Field | Set When |
|-------|----------|
| `ih_revquo` = 'A' | Quote created/revised |
| `ih_revpro` = 'A' | Proforma created |
| `ih_revord` = 'A' | Order created |
| `ih_revdel` = 'A' | Delivery created |
| `ih_revinv` = 'A' | Invoice created |
| `ih_revcrn` = 'A' | Credit note created |
