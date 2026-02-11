# Opera Modules Integration Map

This document maps the data flows between Stock, SOP, POP, and BOM modules. Understanding these integrations is **critical** before implementing any module.

## Core Concept: Stock Quantities

Stock quantities are tracked at multiple levels:

```
cname (Product Master)           cstwh (Stock per Warehouse)
├─ CN_INSTOCK  (total all WH)    ├─ CS_INSTOCK  (physical in this WH)
├─ CN_FREEST   (available)       ├─ CS_FREEST   (available in this WH)
├─ CN_ALLOC    (allocated)       ├─ CS_ALLOC    (allocated in this WH)
├─ CN_ONORDER  (on PO)           ├─ CS_ORDER    (on PO for this WH)
├─ CN_SALEORD  (on SO)           ├─ CS_SALEORD  (on SO from this WH)
└─ ...                           ├─ CS_WOALLOC  (allocated to works orders)
                                 └─ CS_WORKORD  (on works orders)
```

**Key Formula**: `FREE_STOCK = IN_STOCK - ALLOCATED`

## Module Integration Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              STOCK MODULE                                    │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐   │
│  │  cname  │    │  cstwh  │    │  ctran  │    │  cware  │    │  cfact  │   │
│  │Products │◄──►│Per W/H  │◄──►│  Trans  │    │Warehouses│   │Profiles │   │
│  └────┬────┘    └────┬────┘    └────┬────┘    └─────────┘    └─────────┘   │
│       │              │              │                                       │
└───────┼──────────────┼──────────────┼───────────────────────────────────────┘
        │              │              │
        │              │              │
┌───────┼──────────────┼──────────────┼───────────────────────────────────────┐
│       │         SOP MODULE          │                                       │
│       │              │              │                                       │
│  ┌────▼────┐    ┌────▼────┐    ┌────▼────┐    ┌─────────┐                  │
│  │  ihead  │◄──►│  iline  │◄──►│ ialloc  │    │  iparm  │                  │
│  │Doc Hdr  │    │Doc Lines│    │Allocatn │    │ Options │                  │
│  └────┬────┘    └─────────┘    └─────────┘    └─────────┘                  │
│       │                                                                     │
│       ▼                                                                     │
│  Posts to: stran (Sales Ledger), ntran (Nominal), zvtran/nvat (VAT)        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              POP MODULE                                      │
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│  │ dohead  │◄──►│ doline  │    │ cghead  │◄──►│ cgline  │                  │
│  │PO Header│    │PO Lines │    │GRN Hdr  │    │GRN Lines│                  │
│  └────┬────┘    └────┬────┘    └────┬────┘    └────┬────┘                  │
│       │              │              │              │                        │
│       │              └──────────────┼──────────────┘                        │
│       │                             │                                       │
│       │                             ▼                                       │
│       │                        Updates: cstwh, ctran, cname                 │
│       ▼                                                                     │
│  Posts to: ptran (Purchase Ledger), ntran (Nominal), zvtran/nvat (VAT)     │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              BOM MODULE                                      │
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                                 │
│  │ cstruc  │    │  chead  │◄──►│  cline  │                                 │
│  │Assembly │    │Works Ord│    │WO Lines │                                 │
│  │Structure│    │ Header  │    │Components│                                │
│  └────┬────┘    └────┬────┘    └────┬────┘                                 │
│       │              │              │                                       │
│       │              │              ▼                                       │
│       │              │         Updates: cstwh (CS_WOALLOC, CS_WORKORD)     │
│       │              │                                                      │
│       │              ▼                                                      │
│       │         On completion: ctran for components OUT, assembly IN       │
│       │                                                                     │
│       ▼                                                                     │
│  Can link to: SOP (CX_SOREF), Job Costing (CX_JCSTDOC)                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Transaction Flows

### 1. Sales Order Processing Flow

```
Quote (Q) → Proforma (P) → Order (O) → Delivery (D) → Invoice (I) → Credit (C)
                              │              │              │
                              ▼              ▼              ▼
                         CS_SALEORD↑    CS_INSTOCK↓    stran created
                         CS_ALLOC↑     CS_ALLOC↓      ntran created
                                       ctran created   zvtran/nvat
```

**Document Status (IH_DOCSTAT)**:
- Q = Quote
- P = Proforma
- O = Order
- D = Delivery Note
- I = Invoice
- C = Credit Note
- U = Undefined (cumulative)

**Stock Update Timing (IP_UPDST)**:
- D = Update stock on Delivery Note
- I = Update stock on Invoice

### 2. Purchase Order Processing Flow

```
Requisition → Uncommitted → Purchase Order → GRN → Invoice Match
                                  │           │          │
                                  ▼           ▼          ▼
                             CS_ORDER↑   CS_INSTOCK↑   ptran created
                                         CS_ORDER↓    ntran created
                                         ctran created zvtran/nvat
```

**Tables**:
- `dhhead/dhline` - Requisitions
- `dodoc/doline` - Uncommitted documents
- `dohead/doline` - Committed purchase orders
- `cghead/cgline` - Goods Received Notes
- `cmatch` - GRN to PO matching

### 3. BOM/Works Order Flow

```
Assembly Structure (cstruc) → Works Order Created (chead/cline)
                                        │
                                        ▼
                                   CS_WOALLOC↑ (components)
                                   CS_WORKORD↑ (assembly)
                                        │
                                        ▼ (on completion)
                                   ctran: Components OUT (-)
                                   ctran: Assembly IN (+)
                                   CS_INSTOCK adjusted
```

**Works Order Status (CX_WOSTAT)**:
- (Document specific statuses TBD from Opera analysis)

## Stock Transaction Types (ctran.CT_TYPE)

| Type | Description | Source Module | Stock Effect |
|------|-------------|---------------|--------------|
| R | Receipt | Stock/POP | IN (+) |
| I | Issue | Stock/SOP | OUT (-) |
| T | Transfer | Stock | Neutral (WH to WH) |
| A | Adjustment | Stock | +/- |
| S | Sale | SOP | OUT (-) |
| P | Purchase Return | POP | OUT (-) |
| W | Works Order Issue | BOM | OUT (-) |
| M | Works Order Receipt | BOM | IN (+) |

*Note: Exact type codes need verification against Opera data*

## Critical Integration Points

### Stock ↔ SOP
- `iline.IT_STOCK` → `cname.CN_REF`
- `iline.IT_CWCODE` → `cstwh.CS_WHAR`
- `ialloc` tracks allocation per line
- Invoice creates `ctran` with CT_TYPE='S'

### Stock ↔ POP
- `doline.DO_CNREF` → `cname.CN_REF`
- `doline.DO_CWCODE` → `cstwh.CS_WHAR`
- `cgline.CI_CNREF` → `cname.CN_REF`
- GRN creates `ctran` with CT_TYPE='R'

### Stock ↔ BOM
- `cstruc.CV_ASSEMBL` → `cname.CN_REF` (assembly)
- `cstruc.CV_COMPONE` → `cname.CN_REF` (component)
- `cline.CY_CNREF` → `cname.CN_REF`
- Works order completion creates `ctran` for each component and assembly

### SOP ↔ BOM
- `chead.CX_SOREF` → `ihead.IH_SORDER`
- Works orders can be created from sales orders

### All Modules ↔ Job Costing
- Most line tables have `JCSTDOC`, `JPHASE`, `JCCODE`, `JLINE` fields
- Allows costs to be allocated to jobs

## Shared Fields Across Modules

| Field Pattern | Purpose | Found In |
|---------------|---------|----------|
| `*_CNREF` | Stock reference | Most line tables |
| `*_CWCODE` / `*_WHAR` | Warehouse code | Most tables |
| `*_ACCOUNT` | Customer/Supplier account | Header tables |
| `*_JCSTDOC/JPHASE/JCCODE/JLINE` | Job costing link | Line tables |
| `SQ_CRDATE/CRTIME/CRUSER` | Audit: created | All tables |
| `SQ_AMDATE/AMTIME/AMUSER` | Audit: amended | All tables |
| `SQ_MEMO` | Comments | Most tables |

## Balance Update Rules

When writing to ANY module, these balances may need updating:

| Table | Field | Updated By |
|-------|-------|------------|
| `cname` | CN_INSTOCK | Stock receipts/issues |
| `cname` | CN_FREEST | Stock movements, allocations |
| `cname` | CN_ALLOC | SOP allocation, BOM allocation |
| `cname` | CN_ONORDER | POP order creation/receipt |
| `cname` | CN_SALEORD | SOP order creation/invoice |
| `cstwh` | CS_INSTOCK | Stock receipts/issues (per WH) |
| `cstwh` | CS_FREEST | Movements, allocations (per WH) |
| `cstwh` | CS_ALLOC | SOP allocation (per WH) |
| `cstwh` | CS_ORDER | POP orders (per WH) |
| `cstwh` | CS_SALEORD | SOP orders (per WH) |
| `cstwh` | CS_WOALLOC | BOM allocation (per WH) |
| `cstwh` | CS_WORKORD | BOM works orders (per WH) |

## Next Steps

1. Verify CT_TYPE values against actual Opera data
2. Document exact field mappings for each module
3. Identify any additional integration points
4. Map Opera UI workflows to understand validation rules
