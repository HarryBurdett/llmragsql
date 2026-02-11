# Sales Order Processing (SOP) Module

**Status**: Phase 1 Complete (Read-Only)
**Priority**: 2
**Dependencies**: Stock module

## Overview

SOP manages the sales document flow from quotation through to invoice and credit note. Documents progress through statuses, with stock allocation and ledger posting at appropriate stages.

## Document Flow

```
Quote (Q) ──► Proforma (P) ──► Order (O) ──► Delivery (D) ──► Invoice (I)
                                                                   │
                                                                   ▼
                                                            Credit Note (C)
```

Each transition is optional - you can go directly from Order to Invoice, for example.

## Core Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `ihead` | Document header | IH_DOC (PK), IH_ACCOUNT, IH_DOCSTAT |
| `iline` | Document lines | IL_DOC+IL_RECNO, IL_STOCK, IL_QUAN |
| `ialloc` | Stock allocations | IT_DOC+IT_RECNO, IT_QTYALLC |
| `iparm` | SOP parameters | Document number sequences, options |

## Document Status (IH_DOCSTAT)

| Status | Description | Stock Effect | Ledger Effect |
|--------|-------------|--------------|---------------|
| Q | Quote | None | None |
| P | Proforma | None | None |
| O | Order | Allocate (CS_SALEORD, CS_ALLOC) | None |
| D | Delivery | Issue stock (if IP_UPDST='D') | None |
| I | Invoice | Issue stock (if IP_UPDST='I') | stran, ntran, zvtran, nvat |
| C | Credit | Return stock | stran, ntran, zvtran, nvat |
| U | Undefined | Cumulative document | Varies |

## Key Header Fields (ihead)

| Field | Description |
|-------|-------------|
| IH_DOC | Document number |
| IH_ACCOUNT | Customer account (→ sname) |
| IH_NAME | Customer name |
| IH_ADDR1-4 | Invoice address |
| IH_DELAD1-5 | Delivery address |
| IH_DATE | Document date |
| IH_DOCSTAT | Current status (Q/P/O/D/I/C/U) |
| IH_LOC | Default warehouse |
| IH_SORDER | Sales order number |
| IH_INVOICE | Invoice number |
| IH_CREDIT | Credit note number |
| IH_DELIV | Delivery note number |
| IH_EXVAT | Value excluding VAT |
| IH_VAT | VAT amount |
| IH_ODISC | Overall discount % |
| IH_FCURR | Foreign currency code |
| IH_FCRATE | Exchange rate |

## Key Line Fields (iline)

| Field | Description |
|-------|-------------|
| IL_DOC | Document number |
| IL_RECNO | Line number |
| IL_STOCK | Stock reference (→ cname) |
| IL_DESC | Description |
| IL_QUAN | Quantity |
| IL_SELL | Selling price |
| IL_VALUE | Line value |
| IL_DISC | Line discount % |
| IL_VATCODE | VAT code |
| IL_VATRATE | VAT rate |
| IL_CWCODE | Warehouse |
| IL_ANAL | Sales analysis code |
| IL_COST | Cost price |

## Allocation Table (ialloc)

Tracks stock allocation for order lines:

| Field | Description |
|-------|-------------|
| IT_DOC | Document number |
| IT_STOCK | Stock reference |
| IT_QUAN | Line quantity |
| IT_QTYALLC | Quantity allocated |
| IT_QTYPICK | Quantity picked |
| IT_CWCODE | Warehouse |
| IT_PRIORTY | Allocation priority |

## Stock Updates

### On Order Entry
- `CS_SALEORD` += order quantity
- `CN_SALEORD` += order quantity

### On Allocation
- `CS_ALLOC` += allocated quantity
- `CS_FREEST` -= allocated quantity
- `CN_ALLOC` += allocated quantity
- `CN_FREEST` -= allocated quantity

### On Delivery/Invoice (stock issue)
- `CS_INSTOCK` -= issued quantity
- `CS_ALLOC` -= allocated quantity
- `CS_SALEORD` -= order quantity
- `CN_INSTOCK` -= issued quantity
- `CN_ALLOC` -= allocated quantity
- `CN_SALEORD` -= order quantity
- Create `ctran` record (CT_TYPE='S')

## Ledger Posting (Invoice)

When invoice is created:
1. `stran` - Sales ledger transaction (debit customer)
2. `ntran` - Nominal entries (credit sales, credit/debit VAT)
3. `zvtran` - VAT analysis
4. `nvat` - VAT return tracking (nv_vattype='S' for sales)

## Implementation Phases

### Phase 1: Read-Only (Complete)
- [x] List orders/invoices (`GET /api/sop/documents`)
- [x] View document detail (`GET /api/sop/documents/{doc}`)
- [x] View document lines
- [x] Order status tracking
- [x] UI: SalesOrders.tsx (`/sop`)

### Phase 2: Order Entry
- [ ] Create quotation
- [ ] Convert quote to order
- [ ] Stock allocation
- [ ] Order amendments

### Phase 3: Fulfilment
- [ ] Delivery note creation
- [ ] Picking list
- [ ] Invoice generation
- [ ] Credit note processing

## API Endpoints

### Implemented (Read-Only)
```
GET  /api/sop/documents                # List documents with status filter
GET  /api/sop/documents/{doc}          # Document detail with lines
GET  /api/sop/orders/open              # Open orders report
```

### Planned (Write Operations)
```
GET  /api/sop/orders/{doc}/allocations # Allocation status
POST /api/sop/quotes                   # Create quote
POST /api/sop/quotes/{doc}/convert     # Convert to order
POST /api/sop/orders/{doc}/allocate    # Allocate stock
POST /api/sop/orders/{doc}/deliver     # Create delivery
POST /api/sop/orders/{doc}/invoice     # Create invoice
POST /api/sop/invoices/{doc}/credit    # Create credit note
```

## UI Screens (Planned)

1. **Order Entry** - Header + lines grid
2. **Order List** - Open orders with status
3. **Allocation Screen** - Stock availability and allocation
4. **Picking List** - Items to pick by warehouse
5. **Invoice Preview** - Before posting
6. **Credit Note Entry** - Against invoice

## Integration Points

- **Stock**: Allocation, issue on delivery/invoice
- **Sales Ledger**: Invoice/credit posting
- **Nominal**: Sales, VAT posting
- **BOM**: Can trigger works orders
- **Job Costing**: Line-level job allocation

## Notes

- IP_UPDST controls when stock is updated (D=Delivery, I=Invoice)
- IP_SAVE controls whether invoices are saved as documents
- Documents can be reprinted (revision letters IH_REV*)
- EDI fields support electronic document interchange
