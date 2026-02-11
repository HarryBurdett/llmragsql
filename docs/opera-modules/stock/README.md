# Stock Module

**Status**: Planning
**Priority**: 1 (Foundation - all other modules depend on this)

## Overview

The Stock module manages products, warehouses, stock levels, and stock movements. It is the foundation for SOP, POP, and BOM modules.

## Core Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `cname` | Product master | CN_REF (PK), CN_DESC, CN_COST, CN_SELL |
| `cstwh` | Stock per warehouse | CS_REF+CS_WHAR (PK), CS_INSTOCK, CS_FREEST |
| `ctran` | Stock transactions | CT_REF, CT_LOC, CT_TYPE, CT_QUAN |
| `cware` | Warehouse master | CW_CODE (PK), CW_DESC |
| `cfact` | Stock profiles | CF_CODE (PK), CF_NAME, CF_STOCK |
| `ccatg` | Stock categories | CG_CODE (PK), CG_DESC |
| `clist` | Price lists | CL_CODE (PK), CL_DESC |
| `cdetl` | Price list details | CD_CODE+CD_REF, CD_SELL |
| `cdisc` | Discount matrix | SD_CODE, SD_DISC |

## Traceability Tables (Batch/Serial)

| Table | Description |
|-------|-------------|
| `cbatch` | Batch numbers |
| `cbatse` | Batch/Serial combined |
| `ctitem` | Traceable items |
| `ctrack` | Item tracking history |

## GRN Tables (Goods Received)

| Table | Description |
|-------|-------------|
| `cghead` | GRN header |
| `cgline` | GRN lines |
| `cmatch` | GRN to PO matching |

## BOM/Assembly Tables

| Table | Description |
|-------|-------------|
| `cstruc` | Assembly structure (components) |
| `chead` | Works order header |
| `cline` | Works order lines |

## Key Relationships

```
cname (Product)
  │
  ├──► cstwh (one per warehouse)
  │      └──► ctran (transaction history)
  │
  ├──► cfact (profile - controls behavior)
  │
  ├──► ccatg (category - grouping)
  │
  ├──► cdetl (prices per price list)
  │
  └──► cstruc (if assembly - components)
```

## Stock Quantity Fields

### cname (Product Level - All Warehouses)
| Field | Description |
|-------|-------------|
| CN_INSTOCK | Total quantity in stock |
| CN_FREEST | Free stock (available) |
| CN_ALLOC | Allocated to sales orders |
| CN_ONORDER | On purchase orders |
| CN_SALEORD | On sales orders |

### cstwh (Warehouse Level)
| Field | Description |
|-------|-------------|
| CS_INSTOCK | Quantity in this warehouse |
| CS_FREEST | Free stock in this warehouse |
| CS_ALLOC | Allocated in this warehouse |
| CS_ORDER | On PO for this warehouse |
| CS_SALEORD | On SO from this warehouse |
| CS_WOALLOC | Allocated to works orders |
| CS_WORKORD | On works orders |

## Transaction Types (ctran.CT_TYPE)

| Type | Description | Effect |
|------|-------------|--------|
| R | Receipt | + |
| I | Issue | - |
| T | Transfer | Neutral |
| A | Adjustment | +/- |
| S | Sale | - |
| P | Purchase Return | - |
| W | Works Order Issue | - |
| M | Works Order Receipt | + |

*Types to be verified against live Opera data*

## Implementation Phases

### Phase 1: Read-Only
- [ ] Stock enquiry API (single product)
- [ ] Stock search API (by ref, description, category)
- [ ] Stock list by warehouse
- [ ] Transaction history
- [ ] Stock valuation report

### Phase 2: Basic Write
- [ ] Stock adjustments (+/-)
- [ ] Stock transfers (warehouse to warehouse)
- [ ] New product creation
- [ ] Product updates

### Phase 3: Advanced
- [ ] Batch/Serial tracking
- [ ] Reorder level alerts
- [ ] Stock take processing

## API Endpoints (Planned)

```
GET  /api/stock/products              # List/search products
GET  /api/stock/products/{ref}        # Single product detail
GET  /api/stock/products/{ref}/stock  # Stock levels by warehouse
GET  /api/stock/products/{ref}/history # Transaction history
GET  /api/stock/warehouses            # List warehouses
GET  /api/stock/warehouses/{code}/stock # All stock in warehouse

POST /api/stock/adjustments           # Stock adjustment
POST /api/stock/transfers             # Inter-warehouse transfer
```

## UI Screens (Planned)

1. **Stock Search/Browse** - Grid with filtering
2. **Stock Card** - Single product with all details
3. **Stock Levels** - Matrix view (product x warehouse)
4. **Transaction History** - Movements for a product
5. **Stock Adjustment** - Enter +/- adjustments
6. **Stock Transfer** - Move between warehouses

## Notes

- Stock profile (cfact) controls decimal places, costing method, traceability
- Must update BOTH cname AND cstwh when quantities change
- Always create ctran record for audit trail
