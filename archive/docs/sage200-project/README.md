# SQL RAG for Sage 200 - Development Project

## Project Overview

Development of SQL RAG integration for Infor Sage 200 Professional and Standard editions, providing automated bank reconciliation, GoCardless import, and AP automation capabilities.

## Project Status

| Phase | Status | Target | Notes |
|-------|--------|--------|-------|
| Phase 1: Foundation | Not Started | TBD | API client, authentication, data provider |
| Phase 2: Bank Reconciliation | Not Started | TBD | Statement import, matching, posting |
| Phase 3: GoCardless | Not Started | TBD | Payment import, customer matching |
| Phase 4: AP Automation | Not Started | TBD | Invoice processing, payments |
| Phase 5: Testing | Not Started | TBD | UAT, documentation |

## Prerequisites

Before development can begin:

- [ ] Sage 200 API credentials (Client ID / Secret)
- [ ] Sage 200 test environment
- [ ] SQL Server read access (optional)
- [ ] Sample data for testing

## Project Documents

| Document | Description |
|----------|-------------|
| [Product Specification](../sage200-sql-rag-specification.md) | Full product spec for clients |
| [API Integration Guide](./api-integration.md) | Technical API documentation |
| [Database Mapping](./database-mapping.md) | Opera to Sage 200 field mapping |
| [Development Tasks](./tasks.md) | Detailed task breakdown |

## Architecture

```
sql_rag/
├── sage200_api.py           # REST API client
├── sage200_auth.py          # OAuth2 authentication
├── sage200_provider.py      # Data provider (read operations)
├── sage200_import.py        # Transaction posting
├── sage200_bank_import.py   # Bank statement processing
└── sage200_gocardless.py    # GoCardless integration

api/
└── main.py                  # Add /api/sage200/* endpoints

frontend/
└── src/pages/
    └── Sage200*.tsx         # Sage 200 specific UI (if needed)
```

## Key Differences from Opera

| Aspect | Opera SE | Sage 200 |
|--------|----------|----------|
| API | COM / Direct SQL | REST API (OAuth2) |
| Authentication | None | OAuth2 tokens |
| Posting | Direct table INSERT | API POST requests |
| Reading | SQL queries | API GET + optional SQL |
| Amounts | Pence (atran) / Pounds (ntran) | Pounds (all) |
| Periods | Custom period table | Standard fiscal periods |
| Dimensions | Cost centres | Analysis codes (T1-T10) |

## API Endpoints to Implement

### Sage 200 REST API Used

```
# Reading Data
GET /nominal_codes
GET /nominal_transactions
GET /bank_accounts
GET /bank_posted_transactions
GET /customers
GET /customer_transactions
GET /suppliers
GET /supplier_transactions
GET /sales_invoices
GET /purchase_invoices

# Posting Data
POST /bank_posted_transactions
POST /sales_receipts
POST /sales_receipt_allocations
POST /purchase_payments
POST /purchase_payment_allocations
POST /nominal_journals
```

### SQL RAG API Endpoints

```
# Sage 200 specific endpoints
GET  /api/sage200/connection-status
GET  /api/sage200/bank-accounts
GET  /api/sage200/nominal-codes
GET  /api/sage200/customers
GET  /api/sage200/suppliers

# Bank Import
POST /api/sage200/bank-import/scan-emails
POST /api/sage200/bank-import/preview
POST /api/sage200/bank-import/post

# GoCardless
GET  /api/sage200/gocardless/payouts
POST /api/sage200/gocardless/import

# Balance Check
GET  /api/sage200/balance-check/cashbook
GET  /api/sage200/balance-check/debtors
GET  /api/sage200/balance-check/creditors
```

## Development Guidelines

### Code Reuse from Opera

Many components can be reused with minimal changes:

| Component | Reuse Level | Changes Needed |
|-----------|-------------|----------------|
| Email monitoring | 100% | None |
| AI extraction | 100% | None |
| Matching algorithms | 90% | Field name mapping |
| GoCardless client | 100% | None |
| Frontend UI | 80% | Data source selection |

### New Components Required

| Component | Description |
|-----------|-------------|
| `sage200_api.py` | REST API client with OAuth2 |
| `sage200_auth.py` | Token management, refresh |
| `sage200_provider.py` | Read accounts, balances, transactions |
| `sage200_import.py` | Post transactions via API |

## Testing Strategy

1. **Unit Tests** - API client functions
2. **Integration Tests** - End-to-end with test Sage 200
3. **Comparison Tests** - Match Opera behaviour
4. **UAT** - Client acceptance testing

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| API rate limits | Medium | Implement caching, batch requests |
| OAuth token expiry | Low | Auto-refresh tokens |
| API versioning | Low | Version lock, monitor updates |
| Field mapping errors | High | Thorough testing, validation |

## Contact

- **Project Lead**: Charlie Burdett
- **Email**: charlieb@intsysuk.com
