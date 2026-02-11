# Stock Module

**Status**: Phase 2 Complete (Read-Only + Basic Write)
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
- [x] Stock enquiry API (single product) - `GET /api/stock/products/{ref}`
- [x] Stock search API (by ref, description, category) - `GET /api/stock/products`
- [x] Stock list by warehouse - `GET /api/stock/warehouse/{code}/stock`
- [x] Transaction history - `GET /api/stock/products/{ref}/transactions`
- [x] Warehouse list - `GET /api/stock/warehouses`
- [x] Categories list - `GET /api/stock/categories`
- [x] Profiles list - `GET /api/stock/profiles`
- [ ] Stock valuation report

### Phase 2: Basic Write (Partial)
- [x] Stock adjustments (+/-) - `POST /api/stock/adjustments`
- [x] Stock transfers (warehouse to warehouse) - `POST /api/stock/transfers`
- [ ] New product creation
- [ ] Product updates

### Phase 3: Advanced
- [ ] Batch/Serial tracking
- [ ] Reorder level alerts
- [ ] Stock take processing

## API Endpoints

### Implemented (Read-Only)
```
GET  /api/stock/products                      # List/search products (with pagination)
GET  /api/stock/products/{ref}                # Single product detail + stock by warehouse
GET  /api/stock/products/{ref}/transactions   # Transaction history
GET  /api/stock/warehouses                    # List warehouses
GET  /api/stock/warehouse/{code}/stock        # All stock in a warehouse
GET  /api/stock/categories                    # List stock categories
GET  /api/stock/profiles                      # List stock profiles
```

### Implemented (Write Operations)
```
POST /api/stock/adjustments           # Stock adjustment (+/- with reason)
POST /api/stock/transfers             # Inter-warehouse transfer
```

### Planned (Write Operations)
```
POST /api/stock/products              # Create new product
PUT  /api/stock/products/{ref}        # Update product
```

## UI Screens

### Implemented
1. **Stock Search/Browse** (`/stock`) - Grid with search, category/profile filters, pagination
2. **Stock Card** - Product detail panel with full information
3. **Stock by Warehouse** - Stock levels tab showing per-warehouse breakdown
4. **Transaction History** - History tab showing stock movements
5. **Stock Adjustment Modal** - Enter +/- adjustments with warehouse selection and reason codes
6. **Stock Transfer Modal** - Move between warehouses with quantity validation

## Notes

- Stock profile (cfact) controls decimal places, costing method, traceability
- Must update BOTH cname AND cstwh when quantities change
- Always create ctran record for audit trail
