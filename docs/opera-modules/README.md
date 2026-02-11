# Opera Modules Modernization Project

**Status**: Phase 2 Complete (Read-Only APIs & UIs)
**Started**: 2026-02-11
**Last Updated**: 2026-02-11

## Overview

This project aims to modernize Opera's operational modules (Stock, SOP, POP, BOM) with a contemporary UI while preserving the underlying Opera SQL SE database tables. The core accounting modules (Sales Ledger, Purchase Ledger, Nominal, VAT, Cashbook) remain unchanged - this project focuses on the operational/trading modules.

## Modules in Scope

| Module | Status | Priority | Dependencies |
|--------|--------|----------|--------------|
| **Stock** | **Phase 1 Complete** (Read-Only) | 1 | Foundation - all others depend on this |
| **SOP** (Sales Order Processing) | **Phase 1 Complete** (Read-Only) | 2 | Stock |
| **POP** (Purchase Order Processing) | **Phase 1 Complete** (Read-Only) | 2 | Stock |
| **BOM** (Bill of Materials) | **Phase 1 Complete** (Read-Only) | 3 | Stock, SOP, POP |

## Architecture Principles

1. **Database Compatibility**: All data written must be readable by Opera SE UI
2. **Opera Posting Rules**: Follow established patterns (see CLAUDE.md)
3. **Tight Integration**: Stock/SOP/POP/BOM share data - build as integrated system
4. **Incremental Delivery**: Start with read-only, add write operations progressively
5. **Validation First**: Test each feature against Opera SE behavior

## Key Documents

- [Integration Map](./integration-map.md) - Cross-module data flows (CRITICAL)
- [Migration Strategy](./migration-strategy.md) - Path from Opera to independent system
- [Stock Module](./stock/README.md) - Product master, warehouses, transactions
- [SOP Module](./sop/README.md) - Sales order processing
- [POP Module](./pop/README.md) - Purchase order processing
- [BOM Module](./bom/README.md) - Bill of materials / works orders

## Long-Term Goal

**Full independence from Opera** - The system will initially run on Opera's database tables, but the architecture is designed to migrate to our own schema. See [Migration Strategy](./migration-strategy.md) for the phased approach.

## Technology Stack

- **Backend**: Python/FastAPI (existing `api/main.py`)
- **Frontend**: React/TypeScript (existing `frontend/`)
- **Database**: SQL Server (Opera SQL SE tables)
- **Import Logic**: `sql_rag/opera_sql_import.py` (extend for new modules)

## Development Phases

### Phase 1: Documentation & Analysis (Complete)
- [x] Complete integration map
- [x] Document all table structures
- [x] Identify transaction types and posting patterns
- [x] Map Opera UI workflows to understand business logic

### Phase 2: Read-Only APIs & UIs (Complete)

**Stock Module (Foundation)**
- [x] Read-only stock enquiry API (`/api/stock/products`)
- [x] Stock search/browse UI (`/stock`)
- [x] Stock transactions history
- [x] Warehouse stock levels
- [x] Categories and profiles lists

**SOP Module**
- [x] Documents list with status filters (`/api/sop/documents`)
- [x] Document detail with lines (`/api/sop/documents/{doc}`)
- [x] Open orders report (`/api/sop/orders/open`)
- [x] SalesOrders.tsx UI (`/sop`)

**POP Module**
- [x] Purchase orders list (`/api/pop/orders`)
- [x] PO detail with lines (`/api/pop/orders/{po}`)
- [x] GRNs list (`/api/pop/grns`)
- [x] GRN detail (`/api/pop/grns/{grn}`)
- [x] PurchaseOrders.tsx UI (`/pop`)

**BOM Module**
- [x] Assemblies list (`/api/bom/assemblies`)
- [x] Assembly components (`/api/bom/assemblies/{ref}`)
- [x] Works orders list (`/api/bom/works-orders`)
- [x] Works order detail (`/api/bom/works-orders/{wo}`)
- [x] Where-used report (`/api/bom/where-used/{ref}`)
- [x] BillOfMaterials.tsx UI (`/bom`)

### Phase 3: Write Operations (Pending)

**Stock Module**
- [ ] Stock adjustments (+/-)
- [ ] Stock transfers between warehouses

**SOP Module**
- [ ] Quote entry
- [ ] Order entry
- [ ] Stock allocation
- [ ] Picking/despatch
- [ ] Invoice generation (links to Sales Ledger)

**POP Module**
- [ ] Purchase order entry
- [ ] Goods received notes entry
- [ ] Invoice matching (links to Purchase Ledger)

**BOM Module**
- [ ] Assembly structure maintenance
- [ ] Works order entry
- [ ] Component allocation
- [ ] Assembly completion

## Notes

- This is an **ongoing project** - expect incremental progress over multiple sessions
- Always verify new functionality against Opera SE behavior
- Keep this documentation updated as we learn more about Opera's internals
