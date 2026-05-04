# Bank Statement Reconciliation — Instruction Manual

> **Location:** Cashbook > Cashbook Reconcile
> **Purpose:** Import bank statement transactions into Opera and reconcile the cashbook against the bank statement

---

## How It Works

This routine takes a bank statement (PDF from email or uploaded file), extracts the transactions using AI, matches them against Opera's cashbook, imports any missing entries, and reconciles the statement — all in one workflow.

---

## Before You Start

- The **Opera bank account** must exist with correct sort code and account number (`nbank` record)
- The **opening balance** on the statement must match Opera's last reconciled balance (within £0.01)
- Statements must be imported **in sequence** — you can't skip a statement

---

## 5-Stage Workflow

### Stage 1: Select Statement

**From Email:**
- Click **Scan Emails** to search your mailbox for bank statement PDFs
- Statements are listed in import order (chained by opening/closing balances)
- Already-processed statements are hidden
- Missing statements in the sequence are flagged

**From File:**
- Click **Upload PDF** and select a bank statement file
- The system validates the sort code, account number, and opening balance

**If extraction is pending:** Occasionally the AI service hits its quota. Affected
statements show a "Pending" badge and the Process button is disabled for the
whole bank until every statement is extracted (this prevents processing
statements out of sequence). Wait 1–2 minutes and press Scan again; the system
retries the failed statements automatically. No data is lost.

### Stage 2: Review & Match

The AI extracts every transaction from the PDF. Each line is then auto-matched:

| Match Type | What Happens |
|------------|-------------|
| **Existing Opera entry** | Matched to an unreconciled cashbook entry (ready to reconcile) |
| **Repeat entry** | Matched to a recurring entry in Opera (e.g. standing orders) |
| **Customer receipt** | Matched to a customer by name (will post as Sales Receipt) |
| **Supplier payment** | Matched to a supplier by name (will post as Purchase Payment) |
| **Bank transfer** | Detected by account number/sort code in the description |
| **Unmatched** | No match found — use the dropdown to assign manually |

For unmatched lines, you can:
- **Select a customer** (posts as Sales Receipt)
- **Select a supplier** (posts as Purchase Payment)
- **Select a nominal account** (posts as Nominal Entry)
- **Mark as Ignore** (skip — won't post or reconcile)
- **Defer** (awaiting manual entry in Opera)

**Defer (Awaiting manual entry):** When a bank-statement row reflects a transaction that you know is being entered into Opera manually outside this routine, click **Defer** instead of Assign or Ignore. The row is not posted to Opera, is not added to the permanent ignore list, and does not block you from completing the rest of the reconciliation. On the next scan the row reappears — once the manual entry has been made in Opera it will auto-match like any other transaction. Use Defer (rather than Ignore) so the row is not lost; use Ignore only for genuine non-postings (bank fees, internal transfers handled elsewhere).

**Working through statements while waiting on a deferred row:** Once you have decided every row in the current statement (imported, ignored, or deferred), you can move on to the next statement straight away — no need to wait for the deferred row to be resolved. Stages 1–3 (extract, match, import) run on the next statement and Opera's customer/supplier ledgers stay up to date. The only thing that has to wait is the *reconciliation* of the next statement; that's blocked until the prior one fully reconciles, which happens automatically once you resolve the deferred row.

### Stage 3: Import

Click **Import** to post all assigned transactions to Opera:
- Each transaction posts with the correct cashbook entry type
- Bank references from the statement are preserved in Opera
- All Opera posting rules apply (entry numbers, journal numbers, nominal entries, bank balances, customer/supplier balances)
- Statement line numbers are auto-assigned during import

**Auto-Allocate** (optional): When enabled, receipts are automatically allocated to invoices if:
- The receipt amount matches a single outstanding invoice exactly, OR
- The receipt amount clears the customer's entire outstanding balance

### Stage 4: Reconcile

After import, the reconciliation view shows:
- Statement lines in PDF order with a running balance
- Each line shows whether it's matched to an Opera entry
- Tick entries to mark them as reconciled
- The **difference** (statement balance minus Opera reconciled balance) updates in real-time

**Toggle a line in or out of the rec batch:** Each row's status indicator (green ✓ when matched, red ✗ when unmatched) is clickable. Click it to flip — auto-matched lines drop out of this rec batch, manually-matched rows can be excluded, and either can be re-included by clicking again. Lines flipped to ✗ stay open in Opera (`ae_reclnum=0`) and the operator handles them in Opera the same as any other outstanding entry. Use this when the auto-matcher paired a line with the wrong Opera entry, or when an entry needs to stay open for a manual correction in Opera. It's a rare circumstance — most batches reconcile clean — but if you toggle by mistake, completing the rec for that one line in Opera is trivial.

### Stage 5: Complete

When the difference reaches **zero**, all statement lines are accounted for. The system writes both halves of the reconciliation in a single transaction:

**Per cashbook entry (Stage A):**
- Stamped with the rec batch number (taken from Opera's bank counter, not derived)
- Reconciliation date set
- Running reconciled balance recorded after each entry (in statement-line order)
- Statement line position assigned (10, 20, 30, ...)
- Statement number stamped on entry

**Per bank account (Stage B):**
- Reconciled balance (`nk_recbal`) updated to the statement closing balance
- Last-rec batch number, last-rec date, last-rec line all advanced
- Statement number, statement date, statement period (from/to) updated
- Carried-forward balance cleared

If a reconciliation needs to be reversed (wrong statement, edge-case bug, etc.) the operator should contact support — there is a dedicated reversal tool that cleanly undoes both halves of the rec while preserving the underlying postings and writing a JSON audit trail.

---

## How Matching Works

The system matches statement lines to Opera in this order:

### 1. Repeat Entries (Standing Orders, Direct Debits)
- Checks Opera's repeat entry schedule (`arhead`/`arline`)
- Matches by amount (within 10p) and date (within ±5 days of next posting date)
- Also checks description keywords against entry descriptions
- Once matched, the system learns the alias for faster matching next time

### 2. Bank Transfers
- Scans the transaction description for another Opera bank's account number or sort code
- If found, posts as a bank transfer between the two banks (paired entries)

### 3. Alias Lookup
- Checks if this bank description has been seen before (stored in `bank_aliases.db`)
- Aliases are direction-aware: the same name can be a customer for receipts and a supplier for payments
- Match confidence: 100% (definitive)

### 4. Fuzzy Name Matching
- Extracts the payee/payer name from the bank description (strips prefixes like "DD Direct Debit to", dates, references)
- Fuzzy-matches against all Opera customers (for receipts) or suppliers (for payments)
- Threshold: 60% match score to suggest, 85% to auto-assign and learn alias

### 5. Refund Detection
- If a **payment** matches a customer (not a supplier), checks for unallocated credit notes in Opera
- If credit note found: posts as **Sales Refund** instead of a regular payment
- Same logic for receipts matching suppliers: checks for **Purchase Refund**
- Refund detection is based **only on credit note lookup** — never on keywords like "refund" in the description

### 6. Ambiguity Resolution
- If both a customer AND supplier match, the system compares scores
- If the difference is < 15%, neither is used (too ambiguous — user must choose)
- If one clearly wins, it's used

### Correction-pair-matched entries

Entries that have been matched in Opera (via Opera's own matching facility — used to link a mistaken posting with its reversing entry) are **automatically excluded** from new-statement matching. They show neither as "in Opera" nor as "needs posting" — they're settled and out of the rec process. This mirrors Opera's own reconciliation behaviour.

---

## Pattern Learning

The system learns from your decisions:

- **Aliases** — When you confirm a match (or the fuzzy match scores 85%+), the bank description is saved as an alias. Next time the same description appears, it matches instantly.
- **Patterns** — Normalised descriptions (stripped of dates, references, amounts) are stored with their assigned accounts. Suggested for future imports with similar descriptions.

This means matching improves over time — common transactions eventually match automatically.

---

## Duplicate Prevention

The system prevents double-posting at multiple levels:

1. **Fingerprint** — Each imported transaction gets a unique fingerprint stored in the entry reference. If the same fingerprint appears again, it's flagged as a duplicate.
2. **Amount + Date** — Checks for existing entries with the same amount within ±3 days.
3. **Cross-period** — Catches the same transaction posted on a different date (within 7 days).

Duplicates are flagged during review. You can override if you're certain it's a new transaction (e.g. two genuine payments of the same amount on the same day).

---

## Statement Validation Rules

A statement is only valid for import if ALL conditions are met:

| Check | What It Validates |
|-------|-------------------|
| **Sort code** | Statement sort code matches Opera bank's `nk_sort` |
| **Account number** | Statement account number matches Opera bank's `nk_number` |
| **Opening balance** | Must equal Opera's reconciled balance within £0.01 |
| **Sequence** | Must follow on from the last reconciled statement (no gaps) |

If any check fails, the statement is rejected with a clear error message explaining what doesn't match.

---

## Partial Recovery

If an import fails partway through (e.g. network error, database lock):
- The system tracks which lines were successfully posted
- On retry, it skips already-posted lines and continues from where it stopped
- No need to reverse partially-imported entries

---

## Transaction Types Posted

| Statement Line | Opera Action | Cashbook Type |
|----------------|-------------|---------------|
| Credit (money in) matching customer | Sales Receipt | at_type = 4 |
| Debit (money out) matching supplier | Purchase Payment | at_type = 5 |
| Credit matching supplier with credit note | Purchase Refund | at_type = 6 |
| Debit matching customer with credit note | Sales Refund | at_type = 3 |
| Transfer to/from another bank | Bank Transfer | at_type = 8 |
| Debit/Credit to nominal account | Nominal Entry | at_type = 1 or 2 |
| Repeat entry (standing order etc.) | Repeat Entry Post | Uses existing arhead config |

---

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|------------|
| "Opening balance doesn't match" | Opera's reconciled balance differs from statement | Check last reconciled statement in Opera — you may have missed one |
| "Statement already processed" | This statement was imported previously | Check statement history; skip if already done |
| Customer/supplier not matching | Name on bank statement doesn't match Opera | Select manually from dropdown; system learns the alias |
| "Bank account locked" | Another user is importing to the same bank | Wait for them to finish, then retry |
| Statement lines missing from PDF | AI extraction missed some transactions | Check the PDF is clear and legible; re-extract if needed |
| Reconciliation difference not zero | Some entries not matched or amounts differ | Check for unmatched lines; verify amounts match statement |

---

*Last updated: 2026-05-04 — Stage 4 click-to-toggle Match status documented*
