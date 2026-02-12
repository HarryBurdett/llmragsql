# Opera Modules Modernization - Completion Project

**Status**: On Hold
**Created**: 2026-02-11
**Last Updated**: 2026-02-11

## Project Overview

Modernize Opera's operational modules (Stock, SOP, POP, BOM) with a contemporary UI while preserving the underlying Opera SQL SE database tables. The system runs on Opera's database but is designed for future migration to independent schema.

## Current Status

| Module | Phase | Status | Next Steps |
|--------|-------|--------|------------|
| **Stock** | 2 | Partial | Read + Adjustments/Transfers complete |
| **SOP** | 2 | Partial | Quote/Order/Allocate/Invoice complete |
| **POP** | 2 | Partial | PO Entry + Goods Receipt complete |
| **BOM** | 1 | Complete | Read-only complete |

## Completed Features

### Stock Module
- [x] Product search and browse
- [x] Stock levels by warehouse
- [x] Stock transaction history
- [x] Stock adjustments (+/-)
- [x] Stock transfers between warehouses

### SOP (Sales Order Processing)
- [x] Document list with status filters
- [x] Document detail with lines
- [x] Customer search (autocomplete)
- [x] Quote entry
- [x] Order entry (direct)
- [x] Quote to Order conversion
- [x] Stock allocation
- [x] Invoice generation (full posting)
  - stran (Sales Ledger)
  - snoml (Transfer File)
  - ntran + nacnt (Nominal Ledger)
  - zvtran + nvat (VAT Tracking)
  - sname (Customer Balance)
  - ctran, cstwh, cname (Stock Issue)

### POP (Purchase Order Processing)
- [x] Purchase orders list with filters
- [x] PO detail with lines
- [x] GRN list
- [x] Supplier search (autocomplete)
- [x] PO entry with line editor
- [x] Goods receipt against PO
- [x] Outstanding lines tracking
- [x] Stock updates on GRN (ctran, cstwh, cname)
- [x] PO on-order updates (cs_order, cn_onorder)

### BOM (Bill of Materials)
- [x] Assemblies list
- [x] Assembly components
- [x] Works orders list
- [x] Works order detail
- [x] Where-used report

## Remaining Work

### SOP - Priority: Medium
- [ ] Picking/despatch workflow
- [ ] Delivery note creation
- [ ] Credit note processing
- [ ] Order amendments

### POP - Priority: Medium
- [ ] Invoice matching (link to Purchase Ledger)
- [ ] RTV (Return to Vendor) processing
- [ ] PO amendments
- [ ] PO cancellation

### BOM - Priority: Low
- [ ] Assembly structure maintenance
- [ ] Works order entry
- [ ] Component allocation
- [ ] Assembly completion (stock build)

### Cross-Module - Priority: High
- [ ] Stock item search during line entry (SOP/POP)
- [ ] Global search functionality
- [ ] Keyboard navigation optimization
- [ ] Smart defaults (last-used values)

## UX Principles

When resuming development, automatically include:

1. **Search/Autocomplete** - All lookup fields (stock, customers, suppliers, nominals)
2. **Keyboard Navigation** - Tab between fields, Enter to submit
3. **Smart Defaults** - Today's date, default warehouse, last-used values
4. **Inline Validation** - Clear error messages at point of entry
5. **Required Field Indicators** - Visual distinction for mandatory fields
6. **Efficient Workflow** - Field order optimized for data entry speed
7. **Quick-Add Options** - Add new records without leaving current screen

## Technical Notes

### Database Tables
- SOP: ihead, itran (iline), ialloc
- POP: dohead, doline, cghead, cgline
- Stock: cname, cstwh, ctran
- BOM: cbom, cwork

### Company Options
- SOP: iparm (ip_forceal, ip_updtran, ip_updst)
- POP: dparm (dp_dcref for next PO)
- Stock: sprfls (sc_warehse)

### Key Files
- Backend: `sql_rag/opera_sql_import.py`
- API: `api/main.py` (lines 23700-24600)
- Frontend: `frontend/src/pages/Stock.tsx`, `SalesOrders.tsx`, `PurchaseOrders.tsx`, `BillOfMaterials.tsx`

## Documentation

- `docs/opera-modules/README.md` - Project overview
- `docs/opera-modules/integration-map.md` - Cross-module data flows
- `docs/opera-modules/stock/README.md` - Stock module details
- `docs/opera-modules/sop/README.md` - SOP module details
- `docs/opera-modules/pop/README.md` - POP module details
- `docs/opera-modules/bom/README.md` - BOM module details

## Resume Instructions

When resuming this project:

1. Review current status in this document
2. Check `docs/opera-modules/README.md` for latest module status
3. Prioritize stock item search in line editors (SOP/POP)
4. Follow UX principles for all new screens
5. Test against Opera SE UI to verify data compatibility
