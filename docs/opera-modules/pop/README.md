# Purchase Order Processing (POP) Module

**Status**: Phase 1 Complete (Read-Only)
**Priority**: 2
**Dependencies**: Stock module

## Overview

POP manages the purchase document flow from requisition through to goods receipt and invoice matching. It integrates with Stock for receipts and Purchase Ledger for invoice processing.

## Document Flow

```
Requisition (dhhead/dhline)
       │
       ▼
Uncommitted Order (dodoc/doline)
       │
       ▼
Committed P/Order (dohead/doline) ──► GRN (cghead/cgline) ──► Invoice Match
       │                                      │
       ▼                                      ▼
  CS_ORDER↑                             CS_INSTOCK↑
  CN_ONORDER↑                           CS_ORDER↓
                                        ctran created
```

## Core Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `dhhead` | Requisition header | DH_REF (PK) |
| `dhline` | Requisition lines | DH_REF+DH_LINE |
| `dodoc` | Uncommitted order header | DC_REF (PK) |
| `dohead` | Committed PO header | DC_REF (PK), DC_ACCOUNT |
| `doline` | PO lines (all stages) | DO_DCREF+DO_DCLINE |
| `cghead` | GRN header | CH_REF (PK), CH_DATE |
| `cgline` | GRN lines | CI_CHREF+CI_LINE |
| `cmatch` | GRN to PO matching | CM_CHREF+CM_CILINE |
| `dparm` | POP parameters | Document sequences, options |

## Purchasing Hierarchy

| Table | Description |
|-------|-------------|
| `ddept` | Purchasing departments |
| `dname` | Individuals (buyers) |
| `dauth` | Authorisation limits |

## Key PO Header Fields (dohead)

| Field | Description |
|-------|-------------|
| DC_REF | Purchase order number |
| DC_ACCOUNT | Supplier account (→ pname) |
| DC_CWCODE | Default warehouse |
| DC_DELNAM | Delivery name |
| DC_DELAD1-4 | Delivery address |
| DC_CONTACT | Contact name |
| DC_ODISC | Overall discount % |
| DC_CANCEL | Cancelled flag |
| DC_CURRCY | Currency code |
| DC_EXRATE | Exchange rate |
| DC_TOTVAL | Total order value |
| DC_PRINTED | Printed flag |
| DC_PORDER | PO image (memo) |

## Key PO Line Fields (doline)

| Field | Description |
|-------|-------------|
| DO_DCREF | PO number |
| DO_DCLINE | Line number |
| DO_ACCOUNT | Supplier account |
| DO_LEDGER | Nominal account (non-stock) |
| DO_SUPREF | Supplier's stock reference |
| DO_CNREF | Our stock reference (→ cname) |
| DO_DESC | Description |
| DO_CWCODE | Warehouse |
| DO_REQQTY | Quantity required |
| DO_REQDAT | Date required |
| DO_PRICE | Unit price |
| DO_VALUE | Line value |
| DO_DISCP | Discount % |
| DO_JCSTDOC/JPHASE/JCCODE/JLINE | Job costing link |

## GRN Header Fields (cghead)

| Field | Description |
|-------|-------------|
| CH_REF | GRN reference |
| CH_DATE | Delivery date |
| CH_TIME | Delivery time |
| CH_DREF | Delivery note reference |
| CH_DCREF | Carrier reference |
| CH_USER | Receiver's user ID |
| CH_DELCHG | Delivery charge |
| CH_VAT | VAT on delivery charge |

## GRN Line Fields (cgline)

| Field | Description |
|-------|-------------|
| CI_CHREF | GRN reference |
| CI_LINE | Line number |
| CI_ACCOUNT | Supplier account |
| CI_SUPREF | Supplier's stock ref |
| CI_CNREF | Our stock ref |
| CI_QTYRCV | Quantity received |
| CI_QTYREL | Quantity released (to stock) |
| CI_QTYRET | Quantity returned (RTV) |
| CI_QTYMAT | Quantity matched (to PO) |
| CI_COST | Unit cost |
| CI_VALUE | Line value |
| CI_BKWARE | Warehouse booked into |
| CI_DCREF | Matched PO reference |
| CI_DCLINE | Matched PO line |

## Stock Updates

### On PO Creation
- `CS_ORDER` += order quantity (per warehouse)
- `CN_ONORDER` += order quantity (total)

### On GRN (Goods Receipt)
- `CS_INSTOCK` += received quantity
- `CS_ORDER` -= matched PO quantity
- `CN_INSTOCK` += received quantity
- `CN_ONORDER` -= matched PO quantity
- Create `ctran` record (CT_TYPE='R')

### On RTV (Return to Vendor)
- `CS_INSTOCK` -= returned quantity
- `CN_INSTOCK` -= returned quantity
- Create `ctran` record (CT_TYPE='P')

## Ledger Posting (Invoice Match)

When supplier invoice is matched:
1. `ptran` - Purchase ledger transaction (credit supplier)
2. `ntran` - Nominal entries (debit purchases/stock, debit VAT)
3. `zvtran` - VAT analysis
4. `nvat` - VAT return tracking (nv_vattype='P' for purchases)

## Implementation Phases

### Phase 1: Read-Only (Complete)
- [x] List purchase orders (`GET /api/pop/orders`)
- [x] View PO detail with lines (`GET /api/pop/orders/{ref}`)
- [x] GRN list (`GET /api/pop/grns`)
- [x] GRN detail (`GET /api/pop/grns/{ref}`)
- [x] UI: PurchaseOrders.tsx (`/pop`)

### Phase 2: PO Entry
- [ ] Create requisition
- [ ] Convert to PO
- [ ] PO amendments
- [ ] PO cancellation

### Phase 3: Goods Receipt
- [ ] GRN entry
- [ ] Match to PO
- [ ] Part delivery handling
- [ ] RTV processing

### Phase 4: Invoice Matching
- [ ] Match invoice to GRN
- [ ] Price variance handling
- [ ] Post to Purchase Ledger

## API Endpoints

### Implemented (Read-Only)
```
GET  /api/pop/orders                   # List purchase orders
GET  /api/pop/orders/{ref}             # PO detail with lines
GET  /api/pop/grns                     # List GRNs
GET  /api/pop/grns/{ref}               # GRN detail
```

### Planned (Write Operations)
```
GET  /api/pop/orders/{ref}/deliveries  # GRNs against PO
POST /api/pop/requisitions             # Create requisition
POST /api/pop/orders                   # Create PO
PUT  /api/pop/orders/{ref}             # Amend PO
POST /api/pop/orders/{ref}/cancel      # Cancel PO
POST /api/pop/grns                     # Create GRN
POST /api/pop/grns/{ref}/match         # Match GRN to PO
POST /api/pop/grns/{ref}/release       # Release to stock
```

## UI Screens (Planned)

1. **PO Entry** - Header + lines grid
2. **PO List** - Orders by supplier/status
3. **Outstanding Orders** - What's due
4. **GRN Entry** - Receive goods
5. **GRN Matching** - Match to PO lines
6. **Invoice Matching** - Match invoice to GRNs

## Integration Points

- **Stock**: Receipt on GRN, update quantities
- **Purchase Ledger**: Invoice posting
- **Nominal**: Stock/expense account posting
- **Job Costing**: Line-level cost allocation
- **Supplier Products**: dsprod links supplier refs to our refs

## Supplier Products (dsprod)

Links supplier's stock codes to ours:

| Field | Description |
|-------|-------------|
| DS_CNREF | Our stock reference |
| DS_SUPREF | Supplier's reference |
| DS_ACCOUNT | Supplier account |
| DS_COST | Supplier's price |
| DS_LEADTM | Lead time in days |

## Notes

- PO can be for stock items (DO_CNREF) or non-stock (DO_LEDGER)
- GRN can be received against PO or ad-hoc
- Quarantine warehouse can hold goods pending QC release
- Part deliveries create multiple GRNs against one PO
- DP_NOIMAGE controls whether PO image is stored
