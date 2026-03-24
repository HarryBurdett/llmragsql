# SQL RAG for Sage 200
## Product Specification Document

**Version:** 1.0
**Date:** February 2025
**Prepared by:** Carkd.AI / IntSys UK Ltd

---

## Executive Summary

SQL RAG for Sage 200 is an AI-powered financial automation platform that seamlessly integrates with Sage 200 Professional and Standard editions. The solution automates bank reconciliation, payment processing, and accounts payable workflows through intelligent email monitoring, document extraction, and ledger posting.

**Key Value Proposition:**
- **80% reduction** in manual bank reconciliation time
- **Zero-touch processing** for recurring payments (GoCardless, Direct Debits)
- **AI-powered extraction** from any bank statement format (PDF, CSV, OFX)
- **100% Sage 200 integration** via REST API and SQL Server

---

## Product Overview

### What is SQL RAG?

SQL RAG (Retrieval-Augmented Generation) combines artificial intelligence with direct accounting system integration to automate financial workflows that traditionally require significant manual effort.

### Core Capabilities

| Capability | Description |
|------------|-------------|
| **Automated Email Monitoring** | Monitors mailboxes for bank statements, payment notifications, supplier invoices |
| **AI Document Extraction** | Extracts transactions from PDFs, images, and emails using Google Gemini AI |
| **Intelligent Matching** | Matches bank transactions to ledger entries using fuzzy logic and reference parsing |
| **Auto-Allocation** | Automatically allocates receipts to outstanding invoices |
| **Ledger Posting** | Posts transactions directly to Sage 200 via REST API |
| **Balance Validation** | Validates statement sequences and opening balances before import |

---

## Integration Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SQL RAG Platform                             │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Email     │  │     AI      │  │  Matching   │  │   Posting   │ │
│  │  Monitor    │  │ Extraction  │  │   Engine    │  │   Engine    │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │
│         │                │                │                │         │
│         └────────────────┴────────────────┴────────────────┘         │
│                                   │                                   │
└───────────────────────────────────┼───────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │  Sage 200   │ │  Sage 200   │ │  GoCardless │
            │  REST API   │ │  SQL Server │ │     API     │
            └─────────────┘ └─────────────┘ └─────────────┘
                    │               │
                    └───────┬───────┘
                            ▼
                    ┌─────────────┐
                    │  Sage 200   │
                    │  Database   │
                    └─────────────┘
```

### Sage 200 Integration Points

| Component | Method | Purpose |
|-----------|--------|---------|
| **Nominal Ledger** | REST API | Read codes, post journals |
| **Sales Ledger** | REST API | Read customers, post receipts |
| **Purchase Ledger** | REST API | Read suppliers, post payments |
| **Cash Book** | REST API | Read banks, post transactions |
| **Reporting** | SQL Server | Fast queries, balance checks |

### Authentication

- **OAuth 2.0** authentication with Sage 200 API
- **Client ID / Secret** for application registration
- **Access tokens** with automatic refresh
- **SQL Server** connection for read-only reporting queries

---

## Features & Capabilities

### 1. Bank Statement Import & Reconciliation

**Current Sage 200 Process:**
- Download statement from bank
- Import via E-banking or manual entry
- Match transactions manually
- Enter unmatched items separately
- Reconcile and post

**SQL RAG Enhanced Process:**
- Email monitoring detects statement arrival
- AI extracts transactions from any format (PDF, CSV, OFX, MT940)
- Validates opening balance against last reconciled balance
- Auto-matches transactions using intelligent algorithms
- Posts unmatched items with customer/supplier allocation
- Completes reconciliation automatically

**Matching Algorithms:**
1. **Reference Match** - Bank reference to invoice/transaction reference
2. **Amount + Date** - Exact amount within date tolerance
3. **Fuzzy Name** - Customer/supplier name similarity scoring
4. **Invoice Extraction** - Parse invoice numbers from payment references

### 2. GoCardless Integration

**Capability:** Automatically import GoCardless Direct Debit payments as sales receipts.

**Process Flow:**
1. Monitor GoCardless payout notification emails OR poll GoCardless API
2. Extract individual payment details (customer, amount, reference)
3. Match payments to Sage 200 customers
4. Identify outstanding invoices for allocation
5. Post as batched sales receipts
6. Record fees as nominal journal with VAT

**Supported Features:**
- Payout email parsing
- Direct API integration (preferred)
- Customer matching by name/reference
- Invoice auto-allocation
- Fee posting with VAT tracking
- Multi-currency support

### 3. AP Automation

**Capability:** Automate supplier invoice processing and payment reconciliation.

**Features:**
- Monitor mailbox for supplier invoices
- AI extraction of invoice details (supplier, amount, date, references)
- Match to purchase orders
- Three-way matching (PO, GRN, Invoice)
- Supplier statement reconciliation
- Payment proposal generation

### 4. Balance Validation & Control

**Capability:** Continuous validation of ledger balances and control accounts.

**Checks Performed:**
- Sales Ledger vs Debtors Control Account
- Purchase Ledger vs Creditors Control Account
- Cash Book vs Bank Nominal Account
- VAT reconciliation
- Intercompany balance matching

---

## Technical Specifications

### Platform Requirements

| Component | Requirement |
|-----------|-------------|
| **Server OS** | Windows Server 2016+ or Linux |
| **Runtime** | Python 3.9+ |
| **Database** | SQLite (application data) |
| **Memory** | 4GB minimum, 8GB recommended |
| **Storage** | 10GB for application + document cache |

### Sage 200 Requirements

| Component | Requirement |
|-----------|-------------|
| **Version** | Sage 200 Professional or Standard (2020+) |
| **API Access** | REST API enabled with Client ID/Secret |
| **SQL Access** | Read-only SQL Server connection (optional, for reporting) |
| **User Account** | API user with appropriate permissions |

### External Services

| Service | Purpose | Requirement |
|---------|---------|-------------|
| **Google Gemini AI** | Document extraction | API key |
| **GoCardless** | Payment integration | API access token |
| **Email (IMAP)** | Mailbox monitoring | IMAP credentials |
| **Email (SMTP)** | Notifications | SMTP credentials |

### API Endpoints Used

**Sage 200 REST API:**
```
GET  /nominal_codes              - Chart of accounts
GET  /bank_accounts              - Cash book accounts
GET  /customers                  - Sales ledger accounts
GET  /suppliers                  - Purchase ledger accounts
GET  /sales_invoices             - Outstanding invoices
GET  /purchase_invoices          - Outstanding bills
POST /bank_posted_transactions   - Post cash book entries
POST /sales_receipts             - Post customer receipts
POST /purchase_payments          - Post supplier payments
POST /nominal_journals           - Post journal entries
```

---

## Development Roadmap

### Phase 1: Foundation (Weeks 1-3)

| Task | Description | Deliverable |
|------|-------------|-------------|
| 1.1 | Sage 200 API client library | `sage200_api.py` |
| 1.2 | OAuth2 authentication flow | Token management |
| 1.3 | Data provider (read accounts, balances) | `sage200_provider.py` |
| 1.4 | Database schema mapping | Field mapping document |

### Phase 2: Bank Reconciliation (Weeks 4-6)

| Task | Description | Deliverable |
|------|-------------|-------------|
| 2.1 | Statement import engine | PDF/CSV/OFX parser |
| 2.2 | Transaction matching engine | Matching algorithms |
| 2.3 | Cash book posting via API | `sage200_import.py` |
| 2.4 | Reconciliation workflow UI | React components |
| 2.5 | Balance validation | Opening balance checks |

### Phase 3: GoCardless Integration (Weeks 7-8)

| Task | Description | Deliverable |
|------|-------------|-------------|
| 3.1 | GoCardless API client | Existing (reuse) |
| 3.2 | Customer matching for Sage 200 | Name/reference matching |
| 3.3 | Sales receipt posting | API integration |
| 3.4 | Fee journal posting | Nominal posting |

### Phase 4: AP Automation (Weeks 9-10)

| Task | Description | Deliverable |
|------|-------------|-------------|
| 4.1 | Supplier invoice extraction | AI extraction |
| 4.2 | PO/Invoice matching | Three-way match |
| 4.3 | Purchase payment posting | API integration |
| 4.4 | Supplier statement reconciliation | Variance reports |

### Phase 5: Testing & Documentation (Weeks 11-12)

| Task | Description | Deliverable |
|------|-------------|-------------|
| 5.1 | Integration testing | Test suite |
| 5.2 | User acceptance testing | UAT sign-off |
| 5.3 | Documentation | User guide |
| 5.4 | Deployment guide | Installation docs |

---

## Benefits & ROI

### Time Savings

| Process | Manual Time | With SQL RAG | Savings |
|---------|-------------|--------------|---------|
| Daily bank reconciliation | 30-60 mins | 5-10 mins | 80% |
| GoCardless import (per batch) | 15-30 mins | 2 mins | 90% |
| Supplier statement reconciliation | 2-4 hours | 15-30 mins | 85% |
| Month-end bank rec | 2-4 hours | 30 mins | 80% |

### Error Reduction

- **Elimination of manual keying errors**
- **Automatic duplicate detection**
- **Balance validation before posting**
- **Audit trail for all transactions**

### Compliance & Control

- **Segregation of duties** - System posts, humans approve
- **Complete audit trail** - Every action logged
- **Real-time balance monitoring** - Immediate variance detection
- **Document retention** - Original statements stored

---

## Pricing Model Options

### Option A: Perpetual License

| Component | One-Time Cost |
|-----------|---------------|
| Core Platform | £X,XXX |
| Sage 200 Integration Module | £X,XXX |
| GoCardless Module | £X,XXX |
| AP Automation Module | £X,XXX |
| **Annual Support (20%)** | £X,XXX/year |

### Option B: Subscription (SaaS)

| Tier | Monthly Cost | Includes |
|------|--------------|----------|
| Starter | £XXX/month | Bank rec, 1 company |
| Professional | £XXX/month | + GoCardless, 3 companies |
| Enterprise | £XXX/month | + AP Automation, unlimited |

### Option C: Transaction-Based

| Transaction Type | Cost |
|------------------|------|
| Bank statement import | £X.XX |
| GoCardless batch | £X.XX |
| Supplier invoice | £X.XX |

---

## Implementation Requirements

### From Client

1. **Sage 200 Access**
   - API credentials (Client ID/Secret)
   - SQL Server read access (optional)
   - Test environment for development

2. **Email Access**
   - IMAP mailbox for monitoring
   - SMTP for notifications

3. **GoCardless** (if applicable)
   - API access token
   - Payout email forwarding

4. **Documentation**
   - Chart of accounts
   - Bank account details
   - Customer/supplier mappings

### From Development Team

1. **Development environment** with Sage 200 test installation
2. **API credentials** for testing
3. **Sample data** (anonymised) for testing scenarios

---

## Security & Compliance

### Data Security

- **Encryption at rest** - All stored credentials encrypted (AES-256)
- **Encryption in transit** - TLS 1.3 for all API communications
- **No cloud storage** - All data remains on-premises
- **Credential isolation** - API keys stored separately from application

### Access Control

- **Role-based access** - Admin, User, Read-only roles
- **Audit logging** - All actions logged with user/timestamp
- **Session management** - Configurable timeout, single sign-on support

### Compliance

- **GDPR** - No personal data stored beyond transaction requirements
- **SOX** - Audit trail supports compliance requirements
- **PCI-DSS** - No card data processed or stored

---

## Support & Maintenance

### Included Support

- Email support (response within 24 hours)
- Access to knowledge base
- Software updates and patches
- Bug fixes

### Optional Premium Support

- Phone support
- Remote assistance
- Custom development
- On-site training

---

## Next Steps

1. **Discovery Call** - Understand specific requirements
2. **API Access Setup** - Register for Sage 200 developer credentials
3. **Test Environment** - Establish development/test Sage 200 instance
4. **Proof of Concept** - Demonstrate core functionality (2-3 weeks)
5. **Full Implementation** - Complete development (10-12 weeks)
6. **Go Live** - Production deployment and training

---

## Contact

**Carkd.AI / IntSys UK Ltd**

- Email: charlieb@intsysuk.com
- Website: [carkd.ai](https://carkd.ai)

---

*This specification document is confidential and intended for evaluation purposes only.*
