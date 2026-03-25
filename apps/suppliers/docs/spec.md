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
| `old_statement_threshold_days` | Days after which statement is considered old | 14 |
| `payment_notification_days` | Only notify payments made within this period | 90 |
| `security_alert_recipients` | Emails for bank detail change alerts | [config] |

### External Procurement System Integration (e.g., Zahara)

Some organisations use procurement management systems like **Zahara** for invoice approval workflows. Invoices may exist in the procurement system awaiting authorisation before appearing in Opera.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `procurement_api_enabled` | Enable external procurement system check | false |
| `procurement_api_url` | Base URL for procurement system API | null |
| `procurement_api_key` | API key for authentication | null |
| `procurement_system_type` | Type of system (zahara, coupa, etc.) | null |

**Workflow with Procurement Integration:**
1. When an invoice is "not found" in Opera, check procurement system first
2. If found in procurement system:
   - Status: "Pending Approval" → Don't query supplier, inform them it's in our workflow
   - Status: "Rejected" → Query supplier with rejection reason
   - Status: "On Hold" → Inform supplier of hold status
3. Include procurement status in response:
   ```
   Invoice 12345: Currently in our approval workflow (Status: Pending Director Approval)
   Expected processing: Within 5 working days
   ```

This prevents unnecessary queries to suppliers when invoices are simply awaiting internal approval.

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

## 14. Aged Creditors Analysis

### Dashboard View
- Aging buckets: Current, 30 days, 60 days, 90 days, 120+ days
- Per-supplier breakdown with drill-down
- Total outstanding by aging band
- Trend chart (month-on-month aging movement)

### Data Source
- Query `ptran` for outstanding balances (`pt_trbal != 0`)
- Group by supplier and age from `pt_trdate`
- Include credit notes and unallocated payments

### Alerts
- Suppliers with increasing aged debt (trending worse)
- Suppliers approaching credit limit
- Unusual patterns (e.g., sudden spike in invoicing)

---

## 15. Remittance Advice

### Trigger
- After Opera payment run completes
- Manual trigger for ad-hoc payments
- Configurable: auto-send on payment or queue for approval

### Content
```
Subject: Remittance Advice - [Company Name] - [Date]

Dear [Contact Name],

Please find below details of payment made to your account:

Payment Date: [date]
Payment Method: [BACS/Cheque/Faster Payment]
Payment Reference: [reference]
Total Amount: £[total]

INVOICES PAID
=============
Invoice [number] dated [date]    £[amount]
Invoice [number] dated [date]    £[amount]
Less: Credit Note [number]       -£[amount]
                                 =========
Total:                           £[total]

Regards,
Accounts Department
[Company Name]
```

### Data Source
- `ptran` where `pt_trtype = 'P'` and `pt_paid = 'A'` (allocated payments)
- `palloc` for payment-to-invoice allocation detail
- Contact email from `zcontacts` or `pname`

### Configuration
| Parameter | Description | Default |
|-----------|-------------|---------|
| `remittance_auto_send` | Send automatically after payment | false |
| `remittance_format` | Email or PDF attachment | email |
| `remittance_cc` | CC internal address | null |

---

## 16. Supplier Onboarding

### New Supplier Checklist
When a new supplier is created in Opera (`pname` record detected):
1. **Verify bank details** — flag for phone verification before first payment
2. **Set up approved senders** — add known email addresses
3. **Confirm payment terms** — verify `pterm` settings
4. **Assign category/priority** — for statement processing queue ordering
5. **First statement handling** — enhanced verification on first statement received

### Monitoring
- Detect new `pname` records (compare snapshot or watch for records with no `ptran` history)
- Auto-create supplier profile in automation database
- Flag for manual review if bank details present but unverified

### Data Model Addition
```sql
-- Supplier onboarding status
CREATE TABLE supplier_onboarding (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL UNIQUE,
    detected_at DATETIME,
    bank_verified BOOLEAN DEFAULT FALSE,
    bank_verified_by TEXT,
    bank_verified_at DATETIME,
    terms_confirmed BOOLEAN DEFAULT FALSE,
    senders_configured BOOLEAN DEFAULT FALSE,
    category TEXT DEFAULT 'standard',
    priority INTEGER DEFAULT 5,
    notes TEXT,
    completed_at DATETIME
);
```

---

## 17. Contact Management

### Source
- Primary: Opera `zcontacts` table (read/write)
- Extended: Local `supplier_contacts` table for automation-specific fields

### Opera zcontacts Fields
- Contact name, role, email, phone, mobile
- Linked to supplier via account code

### Extended Local Fields
```sql
CREATE TABLE supplier_contacts_ext (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT NOT NULL,
    zcontact_id TEXT,  -- links to Opera zcontacts if applicable
    name TEXT,
    role TEXT,          -- accounts, director, procurement, general
    email TEXT,
    phone TEXT,
    is_statement_contact BOOLEAN DEFAULT FALSE,
    is_payment_contact BOOLEAN DEFAULT FALSE,
    is_query_contact BOOLEAN DEFAULT FALSE,
    preferred_contact_method TEXT DEFAULT 'email',
    notes TEXT,
    created_at DATETIME,
    updated_at DATETIME
);
```

### Rules
- Read contact info from `zcontacts` first
- Local extensions for automation-specific roles (who gets statements, who gets queries)
- Changes to contact details can optionally write back to Opera `zcontacts`
- Security: contact detail changes logged in audit trail

---

## 18. Contact Attributes

### Purpose
Control which contacts can communicate with us and what they're authorised to do.

### Attribute Fields (zc_attr1 to zc_attr6)
Opera provides 6 attribute fields per contact (4 chars each). We define their meaning:

| Attribute | Usage | Values |
|-----------|-------|--------|
| `zc_attr1` | Communication role | STMT (statements), PYMT (payments), QURY (queries), GNRL (general) |
| `zc_attr2` | Security clearance | STD (standard), ELVT (elevated), DIR (director) |
| `zc_attr3` | Verified status | VRFD (verified), PEND (pending), NONE |
| `zc_attr4` | Contact preference | EMAL (email), PHON (phone), POST (postal), PORT (portal) |
| `zc_attr5` | Available | (reserved) |
| `zc_attr6` | Available | (reserved) |

### Security Extension Fields (Local)
Stored in `supplier_contacts_ext`, not Opera:
- `verified_sender` — email address confirmed as legitimate
- `verified_by` — who performed verification
- `verified_date` — when verified
- `authorised_bank_changes` — can request payment method changes
- `security_clearance` — standard / elevated / director
- `verification_phone` — callback number for sensitive requests
- `last_verified` — last confirmation this person is still at the company

---

## 19. Supplier Portal

### Purpose
Self-service portal for suppliers to manage their own information. Reduces manual data entry and puts verification burden on the supplier.

### Incentive
No completed registration = no payment setup. Suppliers are motivated to provide accurate, up-to-date information.

### Portal Features
1. **Contact Management** — suppliers add/update their own contacts
2. **Bank Details** — submit bank detail changes (triggers verification workflow, not auto-applied)
3. **Certifications** — upload insurance, certificates, compliance documents
4. **Query Response** — respond to reconciliation queries directly
5. **Statement Upload** — upload statements directly instead of email
6. **Payment Status** — view payment forecasts and remittance history

### Security
- Unique portal link per supplier (token-based, time-limited)
- All changes queued for internal approval before applying to Opera
- Bank detail changes ALWAYS require phone verification
- Audit trail of all portal activity

---

## 20. Supplier Questionnaire (Onboarding)

### Purpose
Structured onboarding form sent to new suppliers. Captures all required information in one step.

### Questionnaire Sections
1. **Company Details** — registered name, trading name, company number, VAT number
2. **Contacts** — primary contact, accounts contact, director/authorised signatory
3. **Bank Details** — bank name, sort code, account number, IBAN (for FC)
4. **Payment Terms** — agreed terms, preferred payment method
5. **Certifications** — insurance details, accreditations, compliance
6. **Tax Status** — CIS, domestic reverse charge, VAT status

### Workflow
1. System generates questionnaire link for new supplier
2. Supplier completes online form
3. Submission triggers verification workflow:
   - Bank details → phone verification required
   - Contacts → auto-approved (but logged)
   - Certifications → document review required
4. Once verified, data flows into Opera (`pname`, `zcontacts`) and local system
5. Supplier marked as onboarded, payment setup enabled

### Delivery
- Email with secure link
- Form accessible without login (token-based)
- Auto-save progress (supplier can complete over multiple sessions)
- Confirmation email on submission

---

## 21. Future: Sentinel Integration

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

## 19. Implementation Phases

### Phase 1: Foundation & Data Model
- [ ] Database schema (supplier_statements.db) — all tables from spec
- [ ] Contact management (zcontacts read + local extensions)
- [ ] Supplier directory with Opera PL data
- [ ] Approved sender management
- [ ] Settings infrastructure

### Phase 2: Statement Processing
- [ ] Email monitoring for supplier statements
- [ ] Supplier email verification (two-factor)
- [ ] AI extraction from PDF (Gemini Vision)
- [ ] Reconciliation against ptran
- [ ] Business rules engine (only query when not in our favour)

### Phase 3: Communications & Workflow
- [ ] Auto-acknowledgment (parameterised)
- [ ] Response generation with queries + payment info
- [ ] Approval queue with edit-before-send
- [ ] Payment forecast calculation
- [ ] Remittance advice generation
- [ ] Query management with follow-ups

### Phase 4: Security & Monitoring
- [ ] Supplier record change monitoring (bank details, payment method)
- [ ] Security alerts with verification workflow
- [ ] Full audit trail
- [ ] Supplier onboarding checklist

### Phase 5: Aged Creditors & Reporting
- [ ] Aged creditors analysis (30/60/90/120 day buckets)
- [ ] Per-supplier aging drill-down
- [ ] Dashboard KPIs and trend charts
- [ ] Reporting and analytics

### Phase 6: Frontend
- [ ] Supplier Dashboard
- [ ] Statement Queue and Reconciliation Review
- [ ] Open Queries management
- [ ] Communications log
- [ ] Supplier Directory with health indicators
- [ ] Security Alerts
- [ ] Settings page
- [ ] Aged Creditors view

### Phase 7: Contact Attributes & Security
- [x] Contact read/write to Opera zcontacts
- [x] Security extension fields (verified, clearance, bank auth)
- [ ] Contact attribute mapping (zc_attr1-6)
- [ ] Contact-based communication routing

### Phase 8: Supplier Portal
- [ ] Token-based portal access
- [ ] Self-service contact management
- [ ] Bank detail submission (with verification queue)
- [ ] Query response via portal
- [ ] Statement upload via portal

### Phase 9: Supplier Questionnaire
- [ ] Questionnaire template design
- [ ] Online form (token-based, no login)
- [ ] Auto-save progress
- [ ] Submission → verification workflow
- [ ] Data flow to Opera on approval

### Future: Sentinel
- [ ] Voice AI integration
- [ ] API extensions for real-time queries

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2026-02-08 | Claude | Initial specification |
| 0.2 | 2026-03-24 | Claude | Added aged creditors, remittance advice, supplier onboarding, contact management |
| 0.3 | 2026-03-25 | Claude | Added contact attributes, supplier portal, questionnaire, security fields, menu restructure |

---

*This specification may be modified at any time as requirements evolve.*
