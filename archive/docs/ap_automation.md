# AP Automation Guide (Supplier Statement Processing)

## Overview

AP (Accounts Payable) Automation processes supplier statements received by email, reconciles them against Opera's Purchase Ledger, and generates intelligent responses. The system minimizes manual intervention while keeping suppliers informed.

### What Does It Do?

1. **Monitors inbox** for supplier statement emails
2. **Validates sender** against approved supplier contacts
3. **Extracts data** from PDF statements using AI (Claude Vision)
4. **Reconciles** statement lines against Opera Purchase Ledger
5. **Applies business rules** to identify discrepancies
6. **Generates responses** with queries and payment notifications
7. **Queues for approval** before sending to suppliers

---

## Workflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. RECEIVE          Supplier emails statement (PDF attachment)         │
├─────────────────────────────────────────────────────────────────────────┤
│  2. VALIDATE         Verify sender email matches supplier record        │
├─────────────────────────────────────────────────────────────────────────┤
│  3. ACKNOWLEDGE      Auto-reply confirming receipt                      │
├─────────────────────────────────────────────────────────────────────────┤
│  4. EXTRACT          Parse PDF statement using Claude Vision API        │
├─────────────────────────────────────────────────────────────────────────┤
│  5. RECONCILE        Match statement lines against ptran in Opera       │
├─────────────────────────────────────────────────────────────────────────┤
│  6. ANALYSE          Identify differences, apply business rules         │
├─────────────────────────────────────────────────────────────────────────┤
│  7. GENERATE         Auto-draft response with queries & payment info    │
├─────────────────────────────────────────────────────────────────────────┤
│  8. QUEUE            Response held for approval                         │
├─────────────────────────────────────────────────────────────────────────┤
│  9. APPROVE          Authorised user reviews and releases               │
├─────────────────────────────────────────────────────────────────────────┤
│ 10. SEND             Response sent to supplier                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Email Monitoring

### Statement Detection

System identifies supplier statements by:
- PDF attachment present
- Email subject/content indicates "statement"
- Sender email matches known supplier

### Sender Validation (Two-Factor)

1. **Email Address** - Must match supplier record (`pn_email` or approved contacts)
2. **Content Verification** - Statement details cross-referenced with `ptran`

### Validation Failure

If sender doesn't match supplier:
- Flagged for manual verification
- Alert: "Statement from unknown email [address]"
- Not auto-processed until verified

### Approved Senders

Maintain per-supplier:
- Approved email addresses
- Approved email domains
- First-time senders flagged, added after verification

---

## Statement Extraction

### AI-Powered Extraction

Claude Vision API extracts from PDF:
- Supplier name and account reference
- Statement date and period
- Opening/closing balance
- Line items:
  - Date
  - Reference/Invoice number
  - Description
  - Debits (invoices)
  - Credits (payments/credit notes)

### Verification

- Cross-reference supplier details against `pname`
- Match account reference to `pn_account`

---

## Reconciliation

### Match Categories

| Statement | Opera | Status | Action |
|-----------|-------|--------|--------|
| Invoice found | Invoice found | **Matched** | No action |
| Invoice found | Not found | **Not in Opera** | Request copy |
| Not found | Invoice exists | **Not on Statement** | Our record only |
| Payment found | Payment found | **Matched** | No action |
| Payment found | Not found | **Query** | Investigate |
| Not found | Payment made | **Payment Info** | Advise supplier |
| Amount differs | - | **Mismatch** | Query difference |

### Match Process

1. Query `ptran` for supplier account
2. Match by:
   - Invoice/reference number (exact or fuzzy)
   - Amount (exact match)
   - Date (±7 day tolerance)
3. Calculate variance between statement and Opera balance

---

## Business Rules

### Critical Rule: Only Query When NOT in Our Favour

**Auto-Query (supplier's favour or neutral):**
- Invoice on statement, not in our system → Request copy
- Their amount higher than ours → Query difference
- Credit note they claim, we don't have → Request copy
- Payment they claim made, we didn't receive → Query

**Stay Quiet (in our favour):**
- Overpayment from supplier → Do NOT remind them
- Credit we have that they forgot → Keep quiet
- Their amount lower than ours → Do NOT correct them
- Invoice we have that they haven't billed → Keep quiet

### Old Statement Handling

Statements older than 30 days:
- Flag as "old statement"
- May require different response approach
- Consider supplier communication lag

---

## Response Generation

### Auto-Generated Response

System drafts response including:

1. **Acknowledgment** - Confirm statement received
2. **Balance Summary** - Statement vs Opera balance
3. **Queries** - Items requiring clarification
4. **Payment Notifications** - Recent payments made

### Response Template

```
Subject: Re: Statement - [Supplier Name] - [Date]

Dear [Contact Name],

Thank you for your statement dated [date].

Our records show a balance of £[opera_balance] compared to your
statement balance of £[statement_balance], a variance of £[variance].

QUERIES:
- Invoice [ref]: Not found in our records. Please send copy.
- Invoice [ref]: Our records show £X vs your £Y. Please clarify.

PAYMENT INFORMATION:
- Payment of £[amount] made on [date] via BACS (ref: [ref])

Please advise on the above queries at your earliest convenience.

Regards,
Accounts Department
```

---

## Dashboard

### Overview Statistics

| Metric | Description |
|--------|-------------|
| Pending Review | Statements awaiting approval |
| Processed Today | Statements completed today |
| Queries Outstanding | Open queries with suppliers |
| Variance Total | Total discrepancy amount |

### Statement List

Each statement shows:
- Supplier name
- Statement date
- Statement balance
- Opera balance
- Variance (colour-coded)
- Status (New, Processed, Approved, Sent)

---

## Processing States

| Status | Description |
|--------|-------------|
| **Received** | Email detected, awaiting extraction |
| **Extracting** | AI parsing statement |
| **Reconciling** | Matching against Opera |
| **Pending Review** | Awaiting user approval |
| **Approved** | Ready to send response |
| **Sent** | Response sent to supplier |
| **Query Open** | Awaiting supplier response |
| **Resolved** | All queries resolved |

---

## API Endpoints

### Dashboard & List

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/supplier-statements/dashboard` | GET | Dashboard statistics |
| `/api/supplier-statements` | GET | List all statements |
| `/api/supplier-statements/reconciliations` | GET | List with reconciliation data |
| `/api/supplier-statements/history` | GET | Historical statements |

### Processing

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/supplier-statements/extract-from-email/{id}` | POST | Extract from email |
| `/api/supplier-statements/extract-from-file` | POST | Extract from uploaded file |
| `/api/supplier-statements/extract-from-text` | POST | Extract from pasted text |
| `/api/supplier-statements/reconcile/{id}` | POST | Reconcile statement |

### Actions

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/supplier-statements/{id}` | GET | Get statement details |
| `/api/supplier-statements/{id}/lines` | GET | Get statement lines |
| `/api/supplier-statements/{id}/process` | POST | Process statement |
| `/api/supplier-statements/{id}/approve` | POST | Approve response |
| `/api/supplier-statements/{id}/send-updated-status` | POST | Send response |

---

## Data Storage

### Statement Database

Statements stored in `supplier_statements.db` (SQLite):
- Separate from Opera database
- Contains extraction results
- Tracks processing status
- Stores generated responses

### Opera Integration

Read-only access to:
- `pname` - Supplier master
- `ptran` - Purchase transactions
- `palloc` - Payment allocations

---

## File Structure

```
llmragsql/
├── api/main.py                           # API endpoints (~line 3584)
├── sql_rag/
│   ├── supplier_statement_extract.py     # AI extraction logic
│   ├── supplier_statement_reconcile.py   # Reconciliation logic
│   └── supplier_statement_db.py          # Database operations
├── frontend/src/pages/
│   └── SupplierReconciliations.tsx       # UI component
├── supplier_statements.db                # Local database
└── docs/
    ├── supplier_statement_automation_spec.md  # Detailed spec
    └── ap_automation.md                  # This guide
```

---

## Troubleshooting

### Statement Not Detected

- Check email is in monitored inbox
- Verify PDF attachment exists
- Check sender address format

### Extraction Failed

- PDF may be scanned/image-based (try OCR)
- Unusual statement format
- Check API key configuration

### Supplier Not Matched

- Sender email not in `pn_email`
- Add to approved senders list
- Manual verification required

### Large Variance

- Check statement date (may be old)
- Verify correct supplier matched
- Review payment timing differences

### Response Not Generating

- Check reconciliation completed
- Verify business rules applied
- Review query/payment counts

---

## Best Practices

1. **Review Before Sending** - Always approve responses manually
2. **Update Contacts** - Keep supplier email addresses current
3. **Timely Processing** - Process statements within 2-3 days
4. **Document Queries** - Keep records of outstanding items
5. **Follow Up** - Chase suppliers on old queries
6. **Maintain Rules** - Review business rules periodically
