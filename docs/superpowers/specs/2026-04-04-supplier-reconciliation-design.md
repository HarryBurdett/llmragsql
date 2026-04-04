# Supplier Statement Reconciliation — Phase 1 Design

## Goal

Automate supplier statement reconciliation against Opera purchase ledger. Receive statements, compare outstanding transactions, identify differences, respond to suppliers with minimum communication — only when they need to act. Every penny of difference must be accounted for. No "unexplained" amounts.

## Architecture

The system sits between the email inbox and Opera. It receives supplier statements as PDF attachments, extracts transaction data using AI, reconciles against the purchase ledger by comparing two lists of outstanding items, and responds automatically where configured.

**Tech stack**: Python FastAPI backend, React frontend, SQLite for local state, Opera SQL SE for accounting data, Gemini Vision for PDF extraction.

**Key principle**: The reconciliation is a list comparison, not a balance calculation. Both balances are just the sum of their respective transactions. Match by reference — what's left on each side IS the reconciliation.

---

## 1. Reconciliation Engine

### Input

**Their side**: Outstanding transactions extracted from the supplier's statement PDF. Each line has: date, reference, type (invoice/credit/payment), amount.

**Our side**: Outstanding transactions from Opera `ptran` where `pt_trbal <> 0` (outstanding only). Each has: `pt_trref`, `pt_trdate`, `pt_trtype`, `pt_trbal` (outstanding balance: positive = invoice we owe, negative = credit/overpayment reducing what we owe).

**Why outstanding only**: Opera's `pn_currbal = SUM(pt_trbal)`. A fully-paid invoice has `pt_trbal = 0` — it's settled and wouldn't appear on a supplier's statement either. Both sides compare like with like.

### Matching

Reference-only. Case-insensitive. Trimmed of whitespace and AI extraction artefacts (e.g. `*OVERDUE*` suffixes stripped).

No amount-based guessing. No fuzzy matching. No date matching. If the reference doesn't match, the item is unmatched. This is correct — unmatched items ARE the reconciliation output.

### Output

Three lists:

1. **Agreed** — reference exists on both sides. Sub-statuses:
   - *Agreed*: amounts match
   - *Amount difference*: same reference, different amount (flag for investigation)

2. **On their statement, not on ours** — items the supplier shows that we don't have. Typical causes:
   - Invoice we haven't received yet
   - Credit note we haven't received
   - Payment they've recorded that we can't match by reference

3. **On our account, not on theirs** — items we have that the supplier didn't include. Typical causes:
   - Payment we've made that they haven't posted
   - Invoice or credit they've omitted from the statement

### The Mathematical Guarantee

```
Their balance = Net of their outstanding items (from statement)
Our balance   = SUM(pt_trbal) where pt_trbal <> 0 (= pn_currbal)

For agreed items (same reference, same amount): they cancel out in the difference.
For agreed items with different amounts: the difference contributes.

Therefore:
Difference = Net of "their only" items
           - Net of "our only" items (using pt_trbal as signed value)
           + Net of amount differences on agreed items

This ALWAYS holds. If it doesn't, there's a bug.
```

In practice, amount differences on agreed items should be rare — same reference usually means same amount. The primary reconciling items are the two "only" lists.

### Sign Convention

Opera `pn_currbal` is used as-is:
- Negative `pn_currbal` = we owe the supplier (normal creditor balance)
- Positive `pn_currbal` = we are in credit (supplier owes us)

Opera `pt_trbal` on individual transactions:
- Positive `pt_trbal` = outstanding invoice (we owe)
- Negative `pt_trbal` = outstanding credit/overpayment (reduces what we owe)

Supplier statement closing balance is used as-is from extraction. The supplier's convention typically matches: positive = we owe them, negative = we're in credit.

### Opera Transactions

Query `ptran` records where `pt_trbal <> 0` (outstanding items only). These are the transactions that make up `pn_currbal`. Fully settled transactions (`pt_trbal = 0`) are excluded — they don't affect the balance and wouldn't appear on a supplier's statement.

Empty-reference transactions in Opera are included in the "our only" list — they can never match a statement line, so they're always reconciling items.

### Duplicate References

If multiple Opera transactions share the same `pt_trref`, group them under that reference. When matching, compare the aggregate against the statement line. If the aggregate doesn't match cleanly, flag for manual review.

### Transaction Type Matching

References match regardless of transaction type. A payment on the statement with ref "INV-1234" would match an invoice in Opera with the same ref. The type difference is noted but doesn't prevent the match — it's the same underlying transaction reference.

### Implementation Requirements

- All Opera queries use `WITH (NOLOCK)` to avoid locking
- All queries use parameterised SQL — never f-string interpolation with user/AI-derived values
- All local state (SQLite tables, settings, background processing) is company-scoped per the existing `company_data` pattern

---

## 2. Supplier Records

### Table: `supplier_config` (SQLite)

Synced one-way from Opera `pname`. We never write back to Opera.

**From Opera (read-only, synced):**
| Field | Source | Description |
|-------|--------|-------------|
| `account_code` | `pn_account` | Primary key |
| `name` | `pn_name` | Supplier name |
| `balance` | `pn_currbal` | Current balance (synced on demand) |
| `payment_terms_days` | `pterms` | From profile or account override |
| `payment_method` | `pn_paymeth` | B=BACS, C=Cheque, H=Other |

**Our flags (local):**
| Field | Default | Description |
|-------|---------|-------------|
| `reconciliation_active` | true | Process statements for this supplier |
| `auto_respond` | false | Send responses without human approval when balances agree |
| `never_communicate` | false | Reconcile silently, never send emails (Amazon-type) |
| `statements_contact_position` | null | Opera contact position/role to use for statement emails |

**Sync behaviour:**
- Runs on startup and every 30 minutes
- New suppliers detected automatically
- Balance refreshed each time a statement is reconciled
- Our flags are preserved across syncs (never overwritten)
- Dormant suppliers (`pn_dormant = 1`) excluded

### Contact Resolution

Contacts are NOT duplicated locally. When sending an email:
1. Look up `statements_contact_position` on the supplier config
2. Query Opera contacts for that supplier with matching position
3. Use their email address
4. If no contact flagged, fall back to the sender email from the original statement

---

## 3. Automated Workflow

### Background Processing

Runs every 5 minutes via `EmailSyncManager` post-sync callback.

**Step 1 — Detect**: Find new emails with PDF attachments. Skip: auto-replies, delivery notifications, bank statements (by sender name check).

**Step 2 — Deduplicate**: Check `source_email_id` in `supplier_statements` table. If already processed, skip. This check is a database query, not in-memory — survives restarts.

**Step 3 — Extract**: Send PDF to Gemini Vision. Extract supplier name, account reference, statement date, closing balance, and transaction lines (date, reference, type, debit, credit).

**Step 4 — Identify supplier**: Match extracted name/account ref to Opera `pname`. Try exact account code match first, then fuzzy name match. If no match, park statement with status `unmatched_supplier` for manual review.

**Step 4b — Verify sender**: Check that the sender email address and/or sender name matches a known contact for that supplier in Opera. This is a fraud prevention measure — prevents processing fake statements from unknown senders.

- Match sender email against Opera contacts for the identified supplier
- Match sender name against Opera contact names
- If neither matches: park statement with status `unverified_sender` for manual review. Do NOT auto-process.
- If matched: proceed. Log the verified contact for audit.
- First-time senders for a supplier can be approved manually and added to the approved list for future auto-processing.

**Step 5 — Check flags**: Read `supplier_config`. If `reconciliation_active` is false, skip. Save PDF to disk for record.

**Step 6 — Reconcile**: Pull ALL `ptran` for the supplier. Build reference lookup. Run the three-list comparison. Calculate balance difference. Verify the math (assert the guarantee holds).

**Step 7 — Math check**: If net of unmatched items does not equal balance difference (within £0.01 tolerance), flag as `reconciliation_error`. Do NOT auto-respond. Log the discrepancy for investigation.

**Step 8 — Decide response**:

| Condition | Action |
|-----------|--------|
| `never_communicate` = true | Save reconciliation, update dashboard. No email. |
| Balances agree | Auto-send confirmation (regardless of `auto_respond` — there's nothing to review) |
| Balances differ + `auto_respond` = true | Auto-send with queries and payment info |
| Balances differ + `auto_respond` = false | Hold for human review |

**Step 9 — Send**: Generate HTML email from configurable template. Attach original PDF. Record in communications log.

**Step 10 — Update**: Save reconciliation results, update statement status, update dashboard.

### Audit Trail

Every communication is logged in `supplier_communications` — no exceptions:

| Event | Direction | What's recorded |
|-------|-----------|----------------|
| Statement received | Inbound | Sender email, sender name, subject, timestamp, PDF stored |
| Sender verified/rejected | System | Verification result, matched contact, reason if rejected |
| Acknowledgement sent | Outbound | Recipient, subject, body, timestamp, sent by (system/user) |
| Reconciliation completed | System | Result summary, balance comparison, items matched/unmatched |
| Response held for review | System | Reason held, assigned reviewer if applicable |
| Response approved | Outbound | Approved by (user), timestamp |
| Response sent | Outbound | Recipient, subject, body, attachments, timestamp |
| Manual override | System | What was changed, by whom, reason |

The audit trail is immutable — records are never updated or deleted. New events are appended. This provides a complete history of every interaction with every supplier, accessible from the supplier directory and statement detail pages.

---

## 4. Communication Strategy

### Principle

Only communicate when the supplier needs to act. Minimise emails. Pre-empt follow-up calls.

### Email Content

**When balances agree:**
> Thank you for your statement dated [date]. We confirm the balance of [amount] is agreed.
>
> [If outstanding invoices exist with upcoming payment dates:]
> The following invoices are scheduled for payment on [next payment run date]:
> - [ref] — [amount] — due [date]

**When there are differences:**
> Thank you for your statement dated [date].
>
> Balance per your statement: [their balance]
> Balance per our records: [our balance]
> Difference: [amount]
>
> **Items requiring your attention:**
> [Only items WE need from THEM — e.g. "Invoice [ref] for [amount] — not on our records, please send a copy"]
>
> **Payments made:**
> [List payments we've made that they haven't posted — date, amount, reference, bank details]
>
> **Payment schedule:**
> [Outstanding invoices with estimated payment dates]

**What NOT to include:**
- Pricing errors in our favour (don't flag their mistakes that benefit us financially)
- Internal status codes or technical details
- Lengthy explanations — keep it factual

**Exception**: DO query for missing credit notes even if they benefit us — we need them on our account for accurate records. The principle is: don't flag errors that save us money, but DO request documents we need.

### Template Configuration

All text blocks configurable in Supplier Settings:
- Greeting (with `{supplier_name}` merge field)
- Sign-off
- Company name
- Agreed text
- Query introduction text
- Query footer text
- Payment notification text

---

## 5. Payment Estimation

### Data Sources

- `pt_dueday` on each `ptran` invoice record — the calculated due date at posting time
- Company parameter: `next_payment_run_date` — when the next batch payment will be processed

### Logic

For each outstanding invoice on the supplier account:
- If `pt_dueday` <= `next_payment_run_date` → "Included in next payment run on [date]"
- If `pt_dueday` > `next_payment_run_date` → "Due [date], payment scheduled for [following run]"

This gives suppliers a definitive answer to "when will you pay us?"

### Parameter

`next_payment_run_date` is a company-level setting in `supplier_automation_config`. Updated manually or could be calculated from a schedule (e.g. "last Friday of month") in a future enhancement.

---

## 6. Frontend

### Statement Queue (`/supplier/statements/queue`)

List of received statements with:
- Supplier name and account code
- Statement date
- Their balance vs our balance
- Status (received / reconciled / approved / sent)
- **Review** button — opens detail page

### Statement Detail (`/supplier/statements/:id`)

**Reconciliation panel** (top):
- Balance per their statement
- Balance per our records
- Difference
- Represented by: items on their statement not ours / items on ours not theirs
- Total = difference (must always match)

**Their statement lines** (middle table):
- Date, reference, type, debit, credit
- Exists in Opera: Yes / No
- Status: Agreed / Query / Amount Difference

**On our account, not on their statement** (lower table):
- Date, reference, type, debit, credit
- Shows all Opera transactions not matched to any statement line

**Actions**:
- View PDF — embedded viewer with auth
- Approve & Send Response — with spinner, success confirmation, button changes to "Response Sent"

### Supplier Directory (`/supplier/directory`)

List of all suppliers from `supplier_config` with:
- Account, name, balance
- Flags (active, auto-respond, never-communicate)
- Last statement received date
- Editable flags inline or via detail page

### Dashboard (`/supplier/dashboard`)

- Statements received this period
- Pending review count
- Balances agreed vs differences found
- Overdue responses

---

## 7. What to Rebuild vs Keep

### Rebuild from scratch
- `supplier_statement_reconcile.py` — new two-list comparison engine
- Reconciliation panel in frontend — new layout matching this spec
- `statement_opera_only` table — replace with proper signed-value storage

### Keep and update
- `supplier_statement_extract.py` — add reference cleanup (strip artefacts)
- `supplier_statement_db.py` — add `supplier_config` table, fix schema
- `background.py` — plug in new reconciler, fix duplicate prevention
- Response email templates — update content per Section 4
- Frontend queue, detail page structure — update display components

### New
- `supplier_config` table + sync from Opera
- Supplier directory page with flag management
- Company-level `next_payment_run_date` parameter
- Math verification assertion in reconciler

---

## 8. Multi-Company and Opera 3

**Multi-company**: All local state (SQLite tables, settings, background jobs) is company-scoped using the existing `company_data` pattern. Each company has its own `supplier_config`, `supplier_statements`, and automation settings. Background processing runs in the context of the logged-in company.

**Opera 3 parity**: Phase 1 targets Opera SQL SE only. Opera 3 (FoxPro) parity is deferred to Phase 2 — the reconciliation engine is the same, only the data provider changes. The design separates data access from reconciliation logic to make this straightforward.

## 9. Future Phases (Not in Scope)

- **Phase 2**: Supplier onboarding, aged creditors management, payment scheduling
- **Phase 3**: Sentinel AI voice — verbal handling of supplier queries. Capabilities include answering "when will invoice X be paid?" using the same payment estimation logic from Phase 1. Identity verification before disclosing account information: match caller's telephone number to supplier contact, verify caller name, and challenge with partial password (e.g. "please give me the 2nd and 5th characters of your security word"). All voice interactions logged to the same audit trail (transcript, caller ID, supplier matched, verification result, outcome, timestamp). The audit trail schema is designed to accommodate verbal communications alongside email from Phase 1.
- **Supplier portal**: Self-service web portal for suppliers to maintain details, respond to RFIs, view their account status
- **CRM integration**: Migration of supplier records to a central CRM
- **Opera 3 parity**: FoxPro data source support (same reconciliation engine, different data provider)

---

## 10. Success Criteria

1. Every reconciliation balances to the penny — no "unexplained" amounts
2. Automated end-to-end: statement arrives → response sent (for auto-respond suppliers)
3. Human review queue works: differences flagged, one-click approve
4. Supplier flags respected: never-communicate suppliers get no emails
5. Payment dates included in responses — reduces inbound calls
6. No duplicate processing — survives API restarts
7. Response emails are professional, brief, and actionable
