# Sage 200 Integration - Development Tasks

## Task Status Legend

- [ ] Not Started
- [~] In Progress
- [x] Completed
- [!] Blocked

---

## Phase 1: Foundation (Weeks 1-3)

### 1.1 API Client Library

| Task | Status | Notes |
|------|--------|-------|
| [ ] Create `sql_rag/sage200_api.py` | | Base API client class |
| [ ] Implement HTTP request wrapper | | With retry, rate limiting |
| [ ] Add error handling | | Parse Sage error responses |
| [ ] Unit tests for API client | | Mock API responses |

### 1.2 OAuth2 Authentication

| Task | Status | Notes |
|------|--------|-------|
| [ ] Create `sql_rag/sage200_auth.py` | | OAuth2 flow |
| [ ] Implement authorization code flow | | Initial token acquisition |
| [ ] Implement token refresh | | Auto-refresh before expiry |
| [ ] Secure token storage | | Encrypted file storage |
| [ ] Token management in API client | | Automatic refresh |

### 1.3 Data Provider

| Task | Status | Notes |
|------|--------|-------|
| [ ] Create `sql_rag/sage200_provider.py` | | Read operations |
| [ ] Implement `get_bank_accounts()` | | List all banks |
| [ ] Implement `get_bank_balance()` | | Current balance |
| [ ] Implement `get_nominal_codes()` | | Chart of accounts |
| [ ] Implement `get_customers()` | | Customer list |
| [ ] Implement `get_suppliers()` | | Supplier list |
| [ ] Implement `get_outstanding_invoices()` | | SL/PL open items |
| [ ] Implement `get_bank_transactions()` | | With date filtering |
| [ ] Caching layer | | Reduce API calls |

### 1.4 Configuration

| Task | Status | Notes |
|------|--------|-------|
| [ ] Add Sage 200 settings storage | | JSON config file |
| [ ] Settings API endpoints | | GET/POST /api/sage200/settings |
| [ ] Frontend settings page | | Configure credentials |
| [ ] Connection test endpoint | | Verify API access |

---

## Phase 2: Bank Reconciliation (Weeks 4-6)

### 2.1 Statement Processing

| Task | Status | Notes |
|------|--------|-------|
| [ ] Reuse existing AI extraction | | No changes needed |
| [ ] Reuse email monitoring | | No changes needed |
| [ ] Adapt balance validation | | Use Sage 200 balances |

### 2.2 Transaction Matching

| Task | Status | Notes |
|------|--------|-------|
| [ ] Create `sql_rag/sage200_reconciler.py` | | Matching engine |
| [ ] Adapt reference matching | | Sage 200 fields |
| [ ] Adapt amount matching | | Amounts in pounds |
| [ ] Adapt fuzzy name matching | | Customer/supplier names |
| [ ] Match against bank transactions | | Query via API |

### 2.3 Transaction Posting

| Task | Status | Notes |
|------|--------|-------|
| [ ] Create `sql_rag/sage200_import.py` | | Post operations |
| [ ] Implement `post_bank_receipt()` | | Money in |
| [ ] Implement `post_bank_payment()` | | Money out |
| [ ] Implement `post_bank_transfer()` | | Between accounts |
| [ ] Implement `post_sales_receipt()` | | Customer payment |
| [ ] Implement `post_purchase_payment()` | | Supplier payment |
| [ ] Implement allocation posting | | Receipt to invoice |

### 2.4 API Endpoints

| Task | Status | Notes |
|------|--------|-------|
| [ ] `GET /api/sage200/bank-accounts` | | List banks |
| [ ] `POST /api/sage200/bank-import/scan-emails` | | Email scan |
| [ ] `POST /api/sage200/bank-import/preview` | | AI extraction |
| [ ] `POST /api/sage200/bank-import/match` | | Match transactions |
| [ ] `POST /api/sage200/bank-import/post` | | Post to Sage 200 |

### 2.5 Frontend

| Task | Status | Notes |
|------|--------|-------|
| [ ] Add Sage 200 data source option | | DataSourceContext |
| [ ] Update Imports.tsx | | Sage 200 endpoints |
| [ ] Update BankStatementReconcile.tsx | | Sage 200 support |

---

## Phase 3: GoCardless Integration (Weeks 7-8)

### 3.1 Customer Matching

| Task | Status | Notes |
|------|--------|-------|
| [ ] Adapt customer matching | | Use Sage 200 customers |
| [ ] Invoice reference lookup | | Query outstanding invoices |
| [ ] Fuzzy name matching | | Customer name comparison |

### 3.2 Posting

| Task | Status | Notes |
|------|--------|-------|
| [ ] Implement batch receipt posting | | Multiple receipts |
| [ ] Implement fee journal posting | | Nominal journal |
| [ ] Implement invoice allocation | | Auto-allocate |

### 3.3 API Endpoints

| Task | Status | Notes |
|------|--------|-------|
| [ ] `GET /api/sage200/gocardless/payouts` | | Fetch from GC API |
| [ ] `POST /api/sage200/gocardless/match-customers` | | Match to Sage |
| [ ] `POST /api/sage200/gocardless/import` | | Post to Sage 200 |

### 3.4 Frontend

| Task | Status | Notes |
|------|--------|-------|
| [ ] Update GoCardlessImport.tsx | | Sage 200 support |

---

## Phase 4: AP Automation (Weeks 9-10)

### 4.1 Supplier Matching

| Task | Status | Notes |
|------|--------|-------|
| [ ] Adapt supplier matching | | Use Sage 200 suppliers |
| [ ] Invoice lookup | | Query PL transactions |

### 4.2 Statement Reconciliation

| Task | Status | Notes |
|------|--------|-------|
| [ ] Adapt statement extraction | | Reuse AI extraction |
| [ ] Match to Sage 200 PL | | Purchase ledger transactions |
| [ ] Generate variance report | | Differences |

### 4.3 Payment Posting

| Task | Status | Notes |
|------|--------|-------|
| [ ] Implement payment posting | | POST /purchase_payments |
| [ ] Implement allocation | | Payment to invoice |

---

## Phase 5: Testing & Documentation (Weeks 11-12)

### 5.1 Testing

| Task | Status | Notes |
|------|--------|-------|
| [ ] Unit tests - API client | | Mock responses |
| [ ] Unit tests - Data provider | | Mock responses |
| [ ] Unit tests - Import functions | | Mock responses |
| [ ] Integration tests | | Real Sage 200 test instance |
| [ ] End-to-end tests | | Full workflow |
| [ ] Performance tests | | API rate limits |

### 5.2 Documentation

| Task | Status | Notes |
|------|--------|-------|
| [ ] User guide | | How to use |
| [ ] Installation guide | | Setup instructions |
| [ ] API documentation | | Endpoint reference |
| [ ] Troubleshooting guide | | Common issues |

### 5.3 Deployment

| Task | Status | Notes |
|------|--------|-------|
| [ ] Deployment scripts | | Installation automation |
| [ ] Configuration templates | | Sample config files |
| [ ] Release notes | | Version documentation |

---

## Prerequisites / Blockers

| Item | Status | Owner | Notes |
|------|--------|-------|-------|
| [ ] Sage 200 API credentials | | Client | Request from Sage |
| [ ] Sage 200 test environment | | Client | Install or access |
| [ ] Sample customer data | | Client | For testing |
| [ ] Sample bank statements | | Client | For testing |

---

## Code Files to Create

| File | Purpose | Phase |
|------|---------|-------|
| `sql_rag/sage200_api.py` | REST API client | 1 |
| `sql_rag/sage200_auth.py` | OAuth2 authentication | 1 |
| `sql_rag/sage200_provider.py` | Data provider (read) | 1 |
| `sql_rag/sage200_import.py` | Transaction posting | 2 |
| `sql_rag/sage200_reconciler.py` | Bank reconciliation | 2 |
| `sql_rag/sage200_gocardless.py` | GoCardless integration | 3 |
| `api/sage200_endpoints.py` | API endpoints | 2 |

---

## Estimated Hours

| Phase | Tasks | Hours |
|-------|-------|-------|
| Phase 1: Foundation | 16 tasks | 40-50 |
| Phase 2: Bank Rec | 18 tasks | 50-60 |
| Phase 3: GoCardless | 8 tasks | 25-30 |
| Phase 4: AP Automation | 6 tasks | 25-30 |
| Phase 5: Testing/Docs | 11 tasks | 30-40 |
| **Total** | **59 tasks** | **170-210 hours** |

---

## Notes

- Hours are estimates and may vary based on API complexity
- Assumes reuse of existing SQL RAG components where possible
- Does not include project management, meetings, or client UAT time
- Prerequisites must be completed before Phase 1 can begin
