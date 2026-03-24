# GoCardless Payment Requests - Feature Specification

## Overview

Add the ability to **request payments from customers via GoCardless Direct Debit** directly from SQL RAG. This completes the GoCardless cycle:

```
Current:  Payout arrives → Import receipt → Allocate to invoice
New:      Outstanding invoice → Request payment → Payout arrives → Import → Allocate
```

---

## User Stories

1. **As a finance user**, I want to select overdue invoices and request payment via GoCardless, so customers are automatically charged.

2. **As a finance user**, I want to see which customers have GoCardless mandates, so I know who can be charged automatically.

3. **As a finance user**, I want to track pending GoCardless payments, so I know what's been requested but not yet collected.

4. **As a finance user**, I want to set up new customers with GoCardless mandates, so I can collect from them in future.

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Opera Sales Ledger                            │
│                    (Outstanding Invoices)                            │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SQL RAG UI                                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  GoCardless Payment Requests                                 │    │
│  │  ┌───────────────────────────────────────────────────────┐  │    │
│  │  │ Customer    │ Invoice   │ Amount  │ Mandate │ Action  │  │    │
│  │  │ Smith Eng   │ INV-001   │ £1,500  │ ✓       │ Request │  │    │
│  │  │ ABC Ltd     │ INV-002   │ £2,300  │ ✓       │ Request │  │    │
│  │  │ Jones Co    │ INV-003   │ £800    │ ✗       │ Setup   │  │    │
│  │  └───────────────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      GoCardless API                                  │
│  POST /payments  →  Payment scheduled for collection                 │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ (2-4 working days)
┌─────────────────────────────────────────────────────────────────────┐
│                    Payout Received                                   │
│  Existing import flow handles receipt posting & allocation           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

### New Table: `gocardless_mandates` (SQLite - local storage)

```sql
CREATE TABLE gocardless_mandates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opera_account TEXT NOT NULL,           -- Opera customer code (e.g., A001)
    opera_name TEXT,                       -- Customer name from Opera
    gocardless_customer_id TEXT,           -- GC customer ID
    mandate_id TEXT NOT NULL,              -- GC mandate ID (MD000XXX)
    mandate_status TEXT DEFAULT 'active',  -- active, cancelled, expired
    scheme TEXT DEFAULT 'bacs',            -- bacs, sepa_core, etc.
    email TEXT,                            -- Customer email
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT,
    UNIQUE(opera_account, mandate_id)
);

CREATE INDEX idx_mandates_opera ON gocardless_mandates(opera_account);
CREATE INDEX idx_mandates_mandate ON gocardless_mandates(mandate_id);
```

### New Table: `gocardless_payment_requests` (SQLite - local storage)

```sql
CREATE TABLE gocardless_payment_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_id TEXT UNIQUE,                -- GC payment ID (PM000XXX)
    mandate_id TEXT NOT NULL,              -- GC mandate ID
    opera_account TEXT NOT NULL,           -- Opera customer code
    amount_pence INTEGER NOT NULL,         -- Amount in pence
    currency TEXT DEFAULT 'GBP',
    charge_date TEXT,                      -- Scheduled collection date
    description TEXT,                      -- Payment description
    invoice_refs TEXT,                     -- JSON array of invoice references
    status TEXT DEFAULT 'pending',         -- pending, confirmed, paid_out, failed, cancelled
    payout_id TEXT,                        -- GC payout ID when collected
    opera_receipt_ref TEXT,                -- Opera receipt reference after import
    error_message TEXT,                    -- If failed
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT,
    FOREIGN KEY (mandate_id) REFERENCES gocardless_mandates(mandate_id)
);

CREATE INDEX idx_requests_status ON gocardless_payment_requests(status);
CREATE INDEX idx_requests_opera ON gocardless_payment_requests(opera_account);
```

---

## API Endpoints

### Mandate Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/mandates` | GET | List all mandates linked to Opera customers |
| `/api/gocardless/mandates/sync` | POST | Sync mandates from GoCardless API |
| `/api/gocardless/mandates/link` | POST | Link a GC mandate to Opera customer |
| `/api/gocardless/mandates/{mandate_id}` | DELETE | Unlink mandate (doesn't cancel in GC) |
| `/api/gocardless/mandates/setup` | POST | Create billing request for new mandate |

### Payment Requests

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/payment-requests` | GET | List payment requests with status |
| `/api/gocardless/payment-requests` | POST | Create new payment request |
| `/api/gocardless/payment-requests/bulk` | POST | Create multiple payment requests |
| `/api/gocardless/payment-requests/{id}` | GET | Get payment request details |
| `/api/gocardless/payment-requests/{id}/cancel` | POST | Cancel pending payment |
| `/api/gocardless/payment-requests/sync` | POST | Sync payment statuses from GC |

### Invoice Integration

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/collectable-invoices` | GET | Outstanding invoices with GC mandates |
| `/api/gocardless/request-payment` | POST | Request payment for specific invoices |

---

## API Request/Response Examples

### Create Payment Request

**Request:**
```json
POST /api/gocardless/request-payment
{
    "opera_account": "A001",
    "invoices": ["INV-2025-001", "INV-2025-002"],
    "amount": 150000,  // pence - if omitted, uses invoice totals
    "charge_date": "2025-02-20",  // optional - defaults to ASAP
    "description": "Payment for INV-2025-001, INV-2025-002"
}
```

**Response:**
```json
{
    "success": true,
    "payment_request": {
        "id": 123,
        "payment_id": "PM00XXXXXX",
        "mandate_id": "MD00XXXXXX",
        "opera_account": "A001",
        "customer_name": "Smith Engineering Ltd",
        "amount": 150000,
        "amount_formatted": "£1,500.00",
        "charge_date": "2025-02-20",
        "status": "pending",
        "invoices": ["INV-2025-001", "INV-2025-002"],
        "estimated_arrival": "2025-02-24"
    }
}
```

### Get Collectable Invoices

**Request:**
```
GET /api/gocardless/collectable-invoices?overdue_only=true&min_amount=100
```

**Response:**
```json
{
    "success": true,
    "total_collectable": 15750.00,
    "invoices": [
        {
            "opera_account": "A001",
            "customer_name": "Smith Engineering Ltd",
            "invoice_ref": "INV-2025-001",
            "invoice_date": "2025-01-15",
            "due_date": "2025-02-14",
            "amount": 1500.00,
            "days_overdue": 5,
            "has_mandate": true,
            "mandate_id": "MD00XXXXXX",
            "mandate_status": "active"
        },
        {
            "opera_account": "A002",
            "customer_name": "ABC Limited",
            "invoice_ref": "INV-2025-003",
            "amount": 2300.00,
            "has_mandate": false,
            "mandate_id": null
        }
    ]
}
```

---

## UI Design

### New Menu Item

```
Cashbook
├── Bank Reconciliation
├── GoCardless Import        (existing)
└── GoCardless Requests      (NEW)
```

### GoCardless Requests Page

```
┌─────────────────────────────────────────────────────────────────────┐
│  GoCardless Payment Requests                              [Settings]│
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─── Summary ──────────────────────────────────────────────────┐   │
│  │  Collectable: £15,750    Pending: £4,200    This Month: £8,500│   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  [Outstanding Invoices]  [Pending Requests]  [Payment History]      │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ □ │ Customer        │ Invoice    │ Amount   │ Due      │ Action ││
│  ├───┼─────────────────┼────────────┼──────────┼──────────┼────────┤│
│  │ ☑ │ Smith Eng Ltd   │ INV-001    │ £1,500   │ Overdue  │ [DD]   ││
│  │ ☑ │ Smith Eng Ltd   │ INV-002    │ £750     │ Overdue  │ [DD]   ││
│  │ □ │ ABC Limited     │ INV-003    │ £2,300   │ 5 days   │ [DD]   ││
│  │ ○ │ Jones & Co      │ INV-004    │ £800     │ Overdue  │ [Setup]││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                      │
│  [DD] = Has mandate    [Setup] = No mandate - needs setup           │
│                                                                      │
│  Selected: 2 invoices (£2,250)                                      │
│  [Request Payment]  [Select All with Mandate]                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Pending Requests Tab

```
┌─────────────────────────────────────────────────────────────────────┐
│  Pending Payment Requests                              [Sync Status]│
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  │ Customer        │ Amount   │ Requested │ Charge   │ Status      │ │
│  ├─────────────────┼──────────┼───────────┼──────────┼─────────────┤ │
│  │ Smith Eng Ltd   │ £2,250   │ Today     │ 20 Feb   │ ⏳ Pending  │ │
│  │ ABC Limited     │ £1,800   │ Yesterday │ 19 Feb   │ ✓ Submitted │ │
│  │ Tech Solutions  │ £3,400   │ 3 days    │ 17 Feb   │ ✓ Confirmed │ │
│  │ Old Customer    │ £500     │ 5 days    │ 15 Feb   │ ✗ Failed    │ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Foundation (Week 1)

| Task | Description |
|------|-------------|
| 1.1 | Create SQLite tables for mandates and payment requests |
| 1.2 | Add GoCardless API methods: list mandates, create payment |
| 1.3 | Create `/api/gocardless/mandates` endpoints |
| 1.4 | Create mandate sync functionality |

### Phase 2: Payment Requests (Week 2)

| Task | Description |
|------|-------------|
| 2.1 | Create `/api/gocardless/collectable-invoices` endpoint |
| 2.2 | Create `/api/gocardless/request-payment` endpoint |
| 2.3 | Create payment status sync functionality |
| 2.4 | Link payments to payouts when received |

### Phase 3: UI (Week 3)

| Task | Description |
|------|-------------|
| 3.1 | Create `GoCardlessRequests.tsx` page |
| 3.2 | Outstanding invoices tab with mandate indicators |
| 3.3 | Pending requests tab with status tracking |
| 3.4 | Payment history tab |
| 3.5 | Mandate setup flow (link to GC billing request) |

### Phase 4: Integration (Week 4)

| Task | Description |
|------|-------------|
| 4.1 | Auto-match incoming payouts to payment requests |
| 4.2 | Update existing import to mark requests as collected |
| 4.3 | Add notification when payments fail |
| 4.4 | Testing and documentation |

---

## GoCardless API Calls Required

### List Mandates
```python
GET /mandates?status=active&customer={customer_id}
```

### Create Payment
```python
POST /payments
{
    "payments": {
        "amount": 150000,
        "currency": "GBP",
        "charge_date": "2025-02-20",
        "description": "Invoice INV-2025-001",
        "metadata": {
            "opera_account": "A001",
            "invoices": "INV-2025-001,INV-2025-002"
        },
        "links": {
            "mandate": "MD00XXXXXX"
        }
    }
}
```

### Get Payment Status
```python
GET /payments/{payment_id}
```

### Cancel Payment
```python
POST /payments/{payment_id}/actions/cancel
```

---

## Payment Status Flow

```
pending_customer_approval  →  Customer needs to approve (rare for DD)
         │
         ▼
    pending_submission     →  Waiting to be submitted to bank
         │
         ▼
      submitted            →  Sent to bank, awaiting collection
         │
         ▼
      confirmed            →  Bank confirmed collection
         │
         ▼
       paid_out            →  Included in payout to merchant
         │
         ▼
    [Opera Receipt]        →  Imported and allocated in Opera
```

---

## Error Handling

| Error | Handling |
|-------|----------|
| Mandate not found | Show "Setup Mandate" option |
| Mandate cancelled/expired | Prompt to set up new mandate |
| Insufficient funds (failed) | Mark as failed, notify user |
| Payment already exists | Show existing payment status |
| Amount exceeds mandate limit | Show error with limit |

---

## Security Considerations

1. **API Token** - Already stored securely in settings
2. **Mandate Verification** - Verify mandate belongs to customer before charging
3. **Amount Limits** - Validate amounts against invoice totals
4. **Audit Trail** - Log all payment requests with user/timestamp

---

## Future Enhancements

1. **Scheduled Collections** - Auto-request on invoice due date
2. **Recurring Payments** - Set up subscriptions for regular customers
3. **Partial Payments** - Allow requesting less than full invoice amount
4. **Reminders** - Email customer before collection
5. **Bulk Operations** - "Collect all overdue" button

---

## Dependencies

- Existing GoCardless API client (`sql_rag/gocardless_api.py`)
- Existing settings storage (`gocardless_settings.json`)
- Opera Sales Ledger access (existing)

---

## Estimated Effort

| Phase | Time |
|-------|------|
| Phase 1: Foundation | 1 week |
| Phase 2: Payment Requests | 1 week |
| Phase 3: UI | 1 week |
| Phase 4: Integration | 1 week |
| **Total** | **4 weeks** |

---

## Success Criteria

1. ✓ Can view outstanding invoices with mandate status
2. ✓ Can request payment for invoices with mandates
3. ✓ Payment status tracked from request to receipt
4. ✓ Imported receipts linked to original request
5. ✓ Failed payments clearly visible with reason
