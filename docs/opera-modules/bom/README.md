# Bill of Materials (BOM) Module

**Status**: Phase 1 Complete (Read-Only)
**Priority**: 3
**Dependencies**: Stock module, SOP (optional), POP (optional)

## Overview

BOM manages assembly structures (what components make up a product) and works orders (instructions to manufacture assemblies). It integrates tightly with Stock for component allocation and finished goods receipt.

## Core Concepts

- **Assembly**: A product made from components
- **Component**: An item used to make an assembly (can itself be an assembly)
- **Structure**: The list of components for an assembly
- **Works Order**: An instruction to manufacture a quantity of an assembly
- **Phantom Assembly**: An assembly that is never stocked (exploded during manufacture)

## Document Flow

```
Assembly Structure (cstruc)
        │
        ▼
Works Order Created (chead)
        │
        ├──► Components Allocated (CS_WOALLOC↑)
        │
        ├──► Components Issued (ctran, CS_INSTOCK↓)
        │
        └──► Assembly Completed (ctran, CS_INSTOCK↑)
```

## Core Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `cstruc` | Assembly structure | CV_ASSEMBL+CV_COMPONE |
| `chead` | Works order header | CX_REF (PK), CX_ASSEMBL |
| `cline` | Works order lines | CY_CXREF+CY_LINENO |

## Assembly Structure (cstruc)

Defines what components make up an assembly:

| Field | Description |
|-------|-------------|
| CV_ASSEMBL | Assembly stock code |
| CV_COMPONE | Component stock code |
| CV_COQUANT | Quantity of component per assembly |
| CV_SEQNO | Sequence/position |
| CV_SUBASSM | Is component a sub-assembly? |
| CV_PHASSM | Is it a phantom assembly? |
| CV_CODESC | Component description |
| CV_LOC | Component warehouse |
| CV_COREF | Component reference code |
| CV_NOTES | Notes |

## Works Order Header (chead)

| Field | Description |
|-------|-------------|
| CX_REF | Works order number |
| CX_ASSEMBL | Assembly stock code |
| CX_DESC | Assembly description |
| CX_ORDQTY | Quantity ordered |
| CX_MADEQTY | Quantity made |
| CX_WIPQTY | Quantity in progress |
| CX_ALLOQTY | Quantity allocated |
| CX_DISCQTY | Quantity discarded |
| CX_CWCODE | Assembly warehouse |
| CX_ORDDATE | Order date |
| CX_DUEDATE | Due date |
| CX_CMPDATE | Completed date |
| CX_WOSTAT | Status |
| CX_CANCEL | Cancelled flag |
| CX_KITTING | Kitting flag |
| CX_TOTVAL | Total value |
| CX_MATVAL | Material value |
| CX_LABVAL | Labour value |
| CX_MATCOST | Material cost |
| CX_LABCOST | Labour cost |
| CX_PRICE | Assembly cost price |
| CX_SALEACC | Linked sales account |
| CX_SOREF | Linked sales order |
| CX_SODOC | Sales order document |
| CX_JCSTDOC/JPHASE/JCCODE/JLINE | Job costing link |
| CX_QCWCODE | Quarantine warehouse |
| CX_REVIS | Revision |

## Works Order Lines (cline)

| Field | Description |
|-------|-------------|
| CY_CXREF | Works order number |
| CY_LINENO | Line number |
| CY_CNREF | Component stock code |
| CY_DESC | Description |
| CY_ASSEMBL | Parent assembly (for multi-level) |
| CY_CWCODE | Component warehouse |
| CY_REQQTY | Quantity required |
| CY_ALLOQTY | Quantity allocated |
| CY_WIPQTY | Quantity work in progress |
| CY_CMPLQTY | Quantity completed |
| CY_FROMST | Quantity from stock |
| CY_TOMAKE | Quantity to make (if sub-assembly) |
| CY_ISSDATE | Issue date |
| CY_PRICE | Price |
| CY_VALUE | Line value |
| CY_LABOUR | Is labour item? |
| CY_STOCK | Is stocked item? |
| CY_SUBASSM | Sub-assembly flag (+/-/blank) |
| CY_PHASSM | Phantom assembly flag |
| CY_SHOW | Display in browse |
| CY_CXASSM | Works order assembly |
| CY_WIPVAL | WIP value |
| CY_PATH | Component level path |

## Stock Updates

### On Works Order Creation
- `CS_WORKORD` += order quantity (assembly warehouse)

### On Component Allocation
- `CS_WOALLOC` += allocated quantity (component warehouse)
- `CS_FREEST` -= allocated quantity

### On Component Issue
- `CS_INSTOCK` -= issued quantity
- `CS_WOALLOC` -= allocated quantity
- Create `ctran` record (CT_TYPE='W' - works order issue)

### On Assembly Completion
- `CS_INSTOCK` += completed quantity (assembly)
- `CS_WORKORD` -= completed quantity
- Create `ctran` record (CT_TYPE='M' - manufacture receipt)

## BOM Levels

| Level | Description |
|-------|-------------|
| T (Top) | Only explode top level |
| B (Bottom) | Explode to raw materials |
| H (Highest) | Multi-level with visibility |

From `cfact.CP_BOMLEV`:
- Controls how deep the explosion goes
- Affects works order line generation

## BOM Issue/Allocation (cfact.CP_BMISAL)

- A = Allocate stock on works order creation
- I = Issue stock (immediate reduction)

## Stock Profile BOM Settings

| Field | Description |
|-------|-------------|
| CP_BOMLEV | BOM issue level (T/B/H) |
| CP_BMISAL | Allocate or issue (A/I) |
| CP_BMJCCST | Which cost to use for costing |

## Costing

Works orders can calculate costs from:
1. Stock cost prices (material)
2. Labour rates
3. Direct costs

Cost rolls up through multi-level BOMs.

## Integration Points

### Stock
- Component allocation from cstwh
- Component issue creates ctran
- Assembly receipt creates ctran
- Updates cname and cstwh quantities

### SOP
- Works order can be linked to sales order (CX_SOREF)
- Can auto-create works order from SO for made-to-order items

### POP
- Works order can trigger purchase requisitions for components
- Bought-in components received via GRN

### Job Costing
- Works order can be costed to a job
- CX_JCSTDOC/JPHASE/JCCODE/JLINE fields

## Implementation Phases

### Phase 1: Read-Only (Complete)
- [x] View assembly structures (`GET /api/bom/assemblies/{ref}`)
- [x] List assemblies (`GET /api/bom/assemblies`)
- [x] List works orders (`GET /api/bom/works-orders`)
- [x] View works order detail (`GET /api/bom/works-orders/{ref}`)
- [x] Where-used enquiry (`GET /api/bom/where-used/{ref}`)
- [x] UI: BillOfMaterials.tsx (`/bom`)

### Phase 2: Structure Maintenance
- [ ] Create assembly structure
- [ ] Edit components
- [ ] Multi-level structure view
- [ ] Where-used enquiry

### Phase 3: Works Orders
- [ ] Create works order
- [ ] Component allocation
- [ ] Component issue
- [ ] Assembly completion
- [ ] Part completion

### Phase 4: Advanced
- [ ] Made-to-order from SOP
- [ ] Auto requisition for components
- [ ] Costing integration
- [ ] Kitting orders

## API Endpoints

### Implemented (Read-Only)
```
GET  /api/bom/assemblies                    # List assemblies
GET  /api/bom/assemblies/{ref}              # Assembly structure with components
GET  /api/bom/where-used/{ref}              # Where is this component used?
GET  /api/bom/works-orders                  # List works orders
GET  /api/bom/works-orders/{ref}            # Works order detail with lines
```

### Planned (Write Operations)
```
POST /api/bom/assemblies                    # Create structure
PUT  /api/bom/assemblies/{ref}              # Update structure
POST /api/bom/works-orders                  # Create works order
POST /api/bom/works-orders/{ref}/allocate   # Allocate components
POST /api/bom/works-orders/{ref}/issue      # Issue components
POST /api/bom/works-orders/{ref}/complete   # Complete assembly
```

## UI Screens (Planned)

1. **Assembly Structure** - Tree view of components
2. **Structure Editor** - Add/edit components
3. **Where Used** - Find assemblies using a component
4. **Works Order Entry** - Create WO
5. **Works Order List** - Open orders
6. **Component Allocation** - Allocate stock
7. **Issue/Completion** - Process WO

## Traceability

For batch/serial tracked items:
- `ctrack` table records movements
- `CK_WOREF` links to works order
- `CK_PATH` defines position in BOM

## Notes

- Phantom assemblies (CV_PHASSM) are never stocked - components explode through
- Sub-assembly flag (+/-/blank) controls expansion in works order
- Labour items (CY_LABOUR) don't affect stock
- Multi-level BOMs can get complex - need careful handling
- Kitting is a simplified BOM process (CX_KITTING)
