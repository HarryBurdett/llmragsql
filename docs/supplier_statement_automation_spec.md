# Supplier Statement Automation

## Overview

Automated system for processing supplier statements received by email, reconciling against Opera purchase ledger, and managing supplier communications with minimal manual intervention.

**Goal:** Keep suppliers informed, reduce queries, minimise need for phone calls.

**Future Integration:** Sentinel AI voice assistant for handling supplier phone queries.

---

## Workflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. RECEIVE          Supplier emails statement (PDF attachment)         │
├─────────────────────────────────────────────────────────────────────────┤
│  2. VALIDATE         Verify sender email matches supplier record        │
├─────────────────────────────────────────────────────────────────────────┤
│  3. ACKNOWLEDGE      Auto-reply confirming receipt (parameterised)      │
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

## 1. Email Monitoring

### Source
- Monitor designated accounts department email inbox
- Identify supplier statements by:
  - PDF attachment present
  - Email content/subject indicating statement
  - Sender email matching known supplier

### Supplier Identification (Two-Factor)
1. **Sender email** matches `pname` supplier record (`pn_email` or approved contacts)
2. **Statement content** verified against `ptran` transactions

### Validation Failure
- If sender email doesn't match supplier record → Flag for manual verification
- Alert: "Statement claims to be from [Supplier] but sent from unknown email [address]"
- Do NOT auto-process until verified

### Approved Senders
- Maintain list of approved email addresses per supplier
- Approved domains per supplier
- First-time sender flagged, can be added to approved list after verification

---

## 2. Auto-Acknowledgment

### Timing
- Parameterised delay (default: immediate, configurable per supplier or globally)

### Content
```
Subject: Statement Received - [Supplier Name] - [Date]

Dear [Contact Name],

Thank you for sending your statement dated [date].

We have received it and are currently processing. You will receive
a detailed reconciliation response within [X working days].

Regards,
Accounts Department
[Company Name]
```

---

## 3. Statement Extraction

### Method
- Use Claude Vision API to extract data from PDF (similar to bank statement extraction)

### Data Extracted
- Supplier name and account reference
- Statement date and period
- Opening balance
- Transactions:
  - Date
  - Reference/Invoice number
  - Description
  - Debit (invoices)
  - Credit (payments/credit notes)
- Closing balance

### Verification
- Cross-reference extracted supplier details against Opera `pname`
- Match account reference to `pn_account`

---

## 4. Reconciliation

### Match Against Opera
- Query `ptran` for supplier account
- Match statement lines to `ptran` entries by:
  - Invoice/reference number
  - Amount
  - Date (with tolerance)

### Match Categories

| Statement | Opera | Status |
|-----------|-------|--------|
| Invoice found | Invoice found | ✓ Matched |
| Invoice found | Not found | Query - request copy |
| Not found | Invoice exists | Our record only |
| Payment found | Payment found | ✓ Matched |
| Payment found | Not found | Query - we haven't received? |
| Not found | Payment made | Advise supplier of payment |
| Amount mismatch | - | Query difference |

---

## 5. Business Rules for Queries

### CRITICAL: Only query when NOT in our favour

**Auto-Query (in supplier's favour or neutral):**
- Invoice on their statement, not in our system → Request copy
- Their amount higher than ours → Query the difference
- Credit note they claim sent, we don't have → Request copy
- Payment they claim made, we haven't received → Query

**Stay Quiet (in our favour):**
- Overpayment from supplier → Do NOT remind them
- Credit we have that they've forgotten → Keep quiet
- Their amount lower than ours → Do NOT correct them
- Invoice we have that they haven't billed → Keep quiet

**Always Notify (helpful):**
- Payments we've made that aren't on their statement → Advise details

**Flag for Internal Review:**
- Large discrepancies (configurable threshold)
- Unusual patterns
- Items where "in our favour" determination is unclear

---

## 6. Payment Forecast

### Calculation
- Based on payment terms from `pterm` / `pname`
- Consider invoice date and terms (e.g., 30 days from invoice)
- Factor in payment run schedule if configured

### Information to Include
- Individual invoice payment dates
- Or: "Your account balance of £X is scheduled for payment on [date]"
- Next payment run date

---

## 7. Response Generation

### Auto-Generated Response Structure

```
Subject: Statement Reconciliation - [Supplier Name] - [Statement Date]

Dear [Contact Name],

Thank you for your statement dated [date]. Please find our reconciliation below.

MATCHED ITEMS
=============
[List of items that match our records]
Total: £X

QUERIES - YOUR RESPONSE REQUIRED
================================
1. Invoice [number] dated [date] for £[amount]
   We do not have this invoice on our system.
   Please send a copy to [email address].

2. Invoice [number] - Amount Discrepancy
   Your statement shows: £[their amount]
   Our records show: £[our amount]
   Please clarify the difference of £[diff].

PAYMENT INFORMATION
===================
We have made the following payment(s) not yet reflected on your statement:
- [Date]: £[amount] - Ref: [reference] - Method: [BACS/Cheque]

PAYMENT FORECAST
================
Based on our payment terms, the following invoices are scheduled for payment:
- Invoice [number] (£[amount]) - Due: [date]
- Invoice [number] (£[amount]) - Due: [date]

Your current account balance of £[balance] is scheduled for payment on [date].

Please respond to any queries above at your earliest convenience.

Regards,
Accounts Department
[Company Name]
```

---

## 8. Approval Queue

### Queue Interface
- List of pending supplier responses
- Status: Ready / Queries Pending / On Hold
- Preview of auto-generated response
- Ability to edit before sending
- Bulk approve option

### Approval Process
- Authorised user reviews response
- Optional: Password protection for send action
- Click "Approve & Send" to release

### Audit Trail
- Who approved
- When approved
- What was sent (snapshot)
- Any edits made before sending

---

## 9. Security Features

### CRITICAL: Never Request/Send Bank Details by Email

**Automated emails must NEVER:**
- Request supplier bank details
- Include our bank details
- Discuss payment method changes
- Action any bank detail change requests

**If supplier mentions bank details changed:**
- Do NOT auto-respond about this
- Flag for manual phone verification
- Alert designated security contact

### Supplier Record Monitoring

**Monitor `pname` for changes to sensitive fields:**
- Bank account number (`pn_bank`, `pn_acno`, `pn_sort`)
- Payment method
- Remittance email address
- Possibly: address, contact details

**When sensitive field changed:**
- Log: Who changed, when, old value, new value
- Auto-email alert to defined person(s) (e.g., Finance Director)
- Consider: Require approval before change takes effect

### Configuration
- Define alert recipients in Settings
- Define which fields trigger alerts
- Configurable per-change-type actions

---

## 10. Configuration Parameters

### Global Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `acknowledgment_delay_minutes` | Delay before sending receipt acknowledgment | 0 (immediate) |
| `processing_sla_hours` | Target time to process statement | 24 |
| `query_response_days` | Expected supplier response time | 5 |
| `follow_up_reminder_days` | Days before sending follow-up | 7 |
| `large_discrepancy_threshold` | Amount requiring manual review | £500 |
| `security_alert_recipients` | Emails for bank detail change alerts | [config] |

### Per-Supplier Overrides
- Custom timing parameters
- Priority level (affects queue ordering)
- Approved sender email addresses
- Custom payment terms override

---

## 11. Data Model

### New Tables Required (Local SQLite - NOT in Opera)

```sql
-- Processed statements
CREATE TABLE supplier_statements (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL,
    statement_date DATE,
    received_date DATETIME,
    sender_email TEXT,
    pdf_path TEXT,
    status TEXT,  -- received, processing, reconciled, queued, sent
    acknowledged_at DATETIME,
    processed_at DATETIME,
    approved_by TEXT,
    approved_at DATETIME,
    sent_at DATETIME
);

-- Statement line items (extracted)
CREATE TABLE statement_lines (
    id INTEGER PRIMARY KEY,
    statement_id INTEGER REFERENCES supplier_statements(id),
    line_date DATE,
    reference TEXT,
    description TEXT,
    debit DECIMAL(10,2),
    credit DECIMAL(10,2),
    match_status TEXT,  -- matched, query, unmatched, in_our_favour
    matched_ptran_id TEXT,
    query_type TEXT,
    query_sent_at DATETIME,
    query_resolved_at DATETIME
);

-- Approved sender emails per supplier
CREATE TABLE supplier_approved_emails (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL,
    email_address TEXT NOT NULL,
    email_domain TEXT,
    added_by TEXT,
    added_at DATETIME,
    verified BOOLEAN DEFAULT FALSE
);

-- Communication audit trail
CREATE TABLE supplier_communications (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL,
    statement_id INTEGER,
    direction TEXT,  -- inbound, outbound
    type TEXT,  -- statement, acknowledgment, reconciliation, query, response
    email_subject TEXT,
    email_body TEXT,
    sent_at DATETIME,
    sent_by TEXT,
    approved_by TEXT
);

-- Supplier field change audit
CREATE TABLE supplier_change_audit (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_by TEXT,
    changed_at DATETIME,
    alert_sent BOOLEAN DEFAULT FALSE,
    alert_sent_to TEXT,
    verified BOOLEAN DEFAULT FALSE,
    verified_by TEXT,
    verified_at DATETIME
);
```

---

## 12. API Endpoints

### For UI and Future Sentinel Integration

```
# Statement Processing
GET  /api/supplier-statements                    # List all statements
GET  /api/supplier-statements/{id}               # Get statement details
GET  /api/supplier-statements/{id}/reconciliation # Get reconciliation results
POST /api/supplier-statements/{id}/approve       # Approve and send response

# Approval Queue
GET  /api/supplier-statements/queue              # Pending approvals
POST /api/supplier-statements/queue/bulk-approve # Bulk approve

# Supplier Account Status (for Sentinel)
GET  /api/suppliers/{code}/account-status        # Current balance, aged debt
GET  /api/suppliers/{code}/payment-forecast      # When invoices will be paid
GET  /api/suppliers/{code}/open-queries          # Outstanding queries
GET  /api/suppliers/{code}/communications        # Communication history

# Security
GET  /api/suppliers/change-alerts                # Pending security alerts
POST /api/suppliers/{code}/verify-change         # Mark change as verified

# Configuration
GET  /api/settings/supplier-automation           # Get settings
PUT  /api/settings/supplier-automation           # Update settings
```

---

## 13. UI Components

### Top-Level Menu: Supplier

```
Supplier (top-level menu)
├── Dashboard          Overview, KPIs, alerts
├── Statements         Incoming statements & processing
│   ├── Queue          New/processing statements
│   ├── Reconciliations Review & approve responses
│   └── History        Processed statements archive
├── Queries
│   ├── Open           Awaiting supplier response
│   ├── Overdue        Past response deadline
│   └── Resolved       Completed queries
├── Communications     All supplier correspondence
├── Directory          Supplier list with status/health
├── Security
│   ├── Alerts         Bank detail changes pending
│   ├── Audit Log      All sensitive field changes
│   └── Approved Senders Manage verified emails
└── Settings           Automation parameters
```

### Dashboard
- **KPIs:**
  - Statements received (today/week/month)
  - Pending approvals count (with aging)
  - Open queries count (with overdue highlight)
  - Average processing time
  - Reconciliation match rate %
- **Alerts:**
  - Security alerts requiring attention
  - Overdue queries
  - Failed processing (extraction errors)
- **Recent Activity:**
  - Latest statements received
  - Latest responses sent
  - Recent queries raised/resolved

### Statement Queue
- List view of incoming statements
- Status indicators (received, processing, ready, sent)
- Filter by status, supplier, date, priority
- Quick actions: View, Process, Prioritise

### Reconciliation Review
- Side-by-side: Statement vs Opera records
- Match status highlighting (matched, query, in our favour)
- Query type indicators
- Edit response before sending
- Approve / Hold / Reject actions

### Open Queries
- Grouped by supplier
- Age indicator (days outstanding)
- Query type (invoice request, amount discrepancy, etc.)
- Expected response date
- Follow-up reminder status
- Quick action: Mark resolved, Send reminder

### Communications
- Full history per supplier
- Filter: Inbound/Outbound, Type, Date range
- Search by content
- View original attachments

### Supplier Directory
- All suppliers with automation status
- Health indicators:
  - Last statement received
  - Open query count
  - Average response time
  - Payment status (on time/overdue)
- Quick access to supplier account in Opera
- Approved sender email management

### Security Alerts
- Change notifications requiring verification
- Verification workflow (phone confirmation checkbox)
- Alert history with resolution status
- Escalation for unverified changes

### Settings
- **Timing Parameters:**
  - Acknowledgment delay
  - Processing SLA
  - Query response deadline
  - Follow-up reminder frequency
- **Thresholds:**
  - Large discrepancy amount
  - Auto-hold thresholds
- **Alert Recipients:**
  - Security alert emails
  - Escalation contacts
- **Email Templates:**
  - Acknowledgment template
  - Reconciliation response template
  - Query templates
  - Follow-up reminder template
- **Per-Supplier Overrides:**
  - Custom timing
  - Priority level
  - Special handling flags

---

## 14. Future: Sentinel Integration

When Sentinel AI voice assistant is implemented, it will have access to:

- Supplier account status via API
- Reconciliation history and current status
- Open queries and resolution status
- Payment forecasts
- Communication history

**Example Sentinel responses:**

> "I can see we received your statement on Tuesday. We've queried invoice 12345 and are awaiting your copy. Your remaining balance of £5,000 is scheduled for payment on the 28th."

> "Our records show we paid £2,500 on the 15th by BACS, reference 98765. This may not have appeared on your statement yet."

> "I can see there's a query outstanding on invoice 6789 - we don't have a copy. Could you arrange for that to be sent to accounts@company.com?"

---

## 15. Implementation Phases

### Phase 1: Foundation
- [ ] Email monitoring setup
- [ ] Supplier email verification
- [ ] PDF extraction using Claude Vision
- [ ] Basic reconciliation against ptran

### Phase 2: Automation
- [ ] Auto-acknowledgment
- [ ] Business rules engine (in our favour logic)
- [ ] Response generation
- [ ] Approval queue UI

### Phase 3: Security
- [ ] Supplier record change monitoring
- [ ] Security alerts
- [ ] Audit trail
- [ ] Approved sender management

### Phase 4: Polish
- [ ] Per-supplier configuration
- [ ] Bulk operations
- [ ] Reporting and analytics
- [ ] Performance optimisation

### Future: Sentinel
- [ ] Voice AI integration
- [ ] API extensions for real-time queries

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2026-02-08 | Claude | Initial specification |

---

*This specification may be modified at any time as requirements evolve.*
