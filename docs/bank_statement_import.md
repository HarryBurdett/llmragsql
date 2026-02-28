# Bank Statement Import Guide

## Overview

The Bank Statement Import feature automates the process of recording bank transactions into Opera SQL SE. It reads bank statement files, matches transactions to customers and suppliers, and creates the appropriate Cashbook entries (Sales Receipts and Purchase Payments).

### What Does It Do?

1. **Parses bank statement files** in multiple formats (CSV, OFX, QIF, MT940)
2. **Matches transactions** to Opera customers/suppliers using intelligent fuzzy matching
3. **Detects duplicates** to prevent double-posting
4. **Creates Cashbook entries** - Sales Receipts for credits, Purchase Payments for debits
5. **Preserves bank references** - Opera entries mirror the bank statement for easy reconciliation
6. **Learns from corrections** - Manual assignments improve future matching

---

## Supported File Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| **CSV** | `.csv` | Standard CSV with Date, Amount, Memo, Subcategory columns |
| **OFX** | `.ofx` | Open Financial Exchange (XML-based, common for UK banks) |
| **QIF** | `.qif` | Quicken Interchange Format (older but still used) |
| **MT940** | `.sta`, `.mt940` | SWIFT/ISO format (corporate banking) |

### Auto-Detection

The system automatically detects the file format by examining:
- File extension
- Content structure (headers, XML tags, field markers)
- Transaction patterns

---

## Import Workflow

### Step 1: Select Bank Statement File

1. Navigate to **Imports > Bank Statement Import**
2. Enter the file path or use "Browse" to select
3. System auto-detects the file format
4. Select the Opera bank account (e.g., BC010)

### Step 2: Preview Transactions

1. Click "Preview" to parse the file
2. System displays transactions in categorized tabs:
   - **Receipts** - Credits matched to customers (Sales Receipts)
   - **Payments** - Debits matched to suppliers (Purchase Payments)
   - **Refunds** - Credit notes/refunds detected
   - **Repeat Entries** - Matched to standing order templates
   - **Already Posted** - Duplicates detected
   - **Unmatched** - No customer/supplier match found
   - **Skipped** - Direct debits, card payments, etc.

### Step 3: Review and Adjust Matches

For each transaction:
- **Green badge** = High confidence match (80%+)
- **Amber badge** = Review required (50-80%)
- **Red badge** = Unmatched or duplicate

**Manual Override:**
- Click the customer/supplier dropdown to change assignment
- Search by name or account code
- Selection is remembered for import

### Step 4: Import to Opera

1. Select transactions to import (checkbox)
2. Click "Import Selected" or "Import All"
3. System creates:
   - **Sales Receipt** (type 1) for customer payments
   - **Purchase Payment** (type 2) for supplier payments
   - Associated ledger entries (stran/ptran, ntran)

---

## Transaction Matching Flow (`_match_transaction`)

The `_match_transaction()` method in both `bank_import.py` (SQL SE) and `bank_import_opera3.py` (Opera 3) follows an identical matching priority sequence. Every transaction passes through these steps in order:

### Step 0: Repeat Entry Detection

**Checked first** — matches bank transactions to Opera's recurring entry templates (arhead/arline).

Matching criteria:
- Bank account matches (`ae_acnt`)
- Amount matches within 10p tolerance (`at_value` in pence)
- Date is within +/- 5 days of next posting date (`ae_nxtpost`)
- Repeat entry is not fully posted (`ae_posted < ae_topost` or `ae_topost = 0` for unlimited)

If matched: `action = 'repeat_entry'`, no further matching performed.

### Step 0.5: Bank Transfer Detection

Checks if the transaction is a transfer to/from another Opera bank account by searching the transaction text (name + reference + memo) for:
1. **Account number match** (score: 1.0) — another bank's account number found in text
2. **Sort code match** (score: 0.9) — another bank's sort code found in text

Search text is **normalized** (uppercase, dashes and spaces removed) for robust matching. Bank accounts loaded from `nbank` table, excluding the current bank.

If matched: `action = 'bank_transfer'`, `match_source = 'bank_account_number'` or `'bank_sort_code'`.

### Step 1: Alias Lookup (Fast Path)

Checks `bank_aliases.db` for a saved mapping. The expected ledger type is determined by transaction direction:
- **Receipts** → look for customer aliases (type `'C'`)
- **Payments** → look for supplier aliases (type `'S'`)

Two lookups are tried:
1. **Full name** — the raw bank description (e.g., `"Giro Direct Credit From ABC Limited Ref: INV001"`)
2. **Clean name** — after payee name extraction (e.g., `"ABC Limited"`) — only tried if different from full name

If alias found AND the account exists in the loaded customer/supplier list:
- `match_score = 1.0`, `match_source = 'alias'`
- Action set to `'sales_receipt'` or `'purchase_payment'`

### Step 2: Fuzzy Matching

Uses the shared `BankMatcher` (from `bank_matching.py`) to find the best customer and supplier match:

1. **Try full name** — match against both customer and supplier lists
2. **Try clean name** — if full name didn't match either list and clean name differs, try again with the extracted payee name. Use the better score.

**Fuzzy matching algorithms** (combined scoring in `bank_matching.py`):
- Token-based matching (word order independent)
- Abbreviation normalization (LTD → LIMITED, CO → COMPANY, etc.)
- Word containment (handles truncated bank names)
- Phonetic matching (Metaphone algorithm)
- Levenshtein distance (handles typos)
- N-gram similarity (partial matches)
- Search key matching (`sn_key1`-`sn_key4` / `pn_key1`-`pn_key4`)

**Minimum match threshold**: `0.6` (configurable via `min_match_score`)

### Step 2.5: Ambiguity Resolution

If BOTH customer AND supplier match above threshold:

1. **Score difference < 0.15** → `action = 'skip'` (truly ambiguous, user must resolve)
2. **Receipt with customer score higher** → fall through to receipt handling
3. **Receipt with supplier score higher** → `action = 'skip'` (wrong direction)
4. **Payment with supplier score higher** → fall through to payment handling
5. **Payment with customer score higher** → check for sales refund via credit note lookup (see Step 3)

### Step 3: Direction-Based Match Assignment

#### Receipts (amount > 0)

| Condition | Result |
|-----------|--------|
| Customer match found | `action = 'sales_receipt'` |
| No customer but supplier match >= 0.8 AND unallocated credit note in ptran | `action = 'purchase_refund'` |
| No customer but supplier match >= 0.8 AND NO credit note | `action = 'skip'` with reason |
| No match | `action = 'skip'` |

#### Payments (amount < 0)

| Condition | Result |
|-----------|--------|
| Supplier match found | `action = 'purchase_payment'` |
| No supplier but customer match >= 0.8 AND unallocated credit note in stran | `action = 'sales_refund'` |
| No supplier but customer match >= 0.8 AND NO credit note | `action = 'skip'` with reason |
| No match | `action = 'skip'` |

### Step 4: Alias Learning

After a successful fuzzy match with score >= `0.85` (learn threshold), the mapping is automatically saved to `bank_aliases.db` for future fast-path lookups.

---

## Payee Name Extraction

Bank descriptions from AI extraction or CSV parsing often contain prefixes and suffixes that interfere with matching. Two extraction functions clean these:

### `extract_payee_name(description)` → 20-char truncated name
Used for Opera `ae_comment` field (limited width).

### `extract_payee_name_full(description)` → full clean name
Used for matching. Strips:

**Prefixes removed:**
- `"Giro Direct Credit From"`, `"Direct Credit From"`
- `"DD Direct Debit to"`, `"Direct Debit to"`
- `"Card Payment to"`, `"Bill Payment to"`, `"Faster Payment to"`
- `"Standing Order to"`, `"Transfer to"`, `"Transfer from"`
- `"Cheque"`, `"ATM"`, `"POS"`

**Suffixes removed:**
- `"Ref: ..."` and everything after
- `"On DD Mon"` date patterns (e.g., `"On 15 Jan"`)
- Trailing `"*"` characters
- Trailing date-like numbers

**Example:**
```
Input:  "Giro Direct Credit From ABC Limited Ref: INV001 On 15 Jan"
Output: "ABC Limited"
```

---

## Refund Detection

Refunds are detected by checking Opera's ledger for unallocated credit notes — NOT by keywords in the bank description.

### Purchase Refund Detection (`_check_purchase_refund`)

**Trigger:** A receipt (credit) strongly matches a supplier (score >= 0.8).

**Logic:** Query `ptran` for the matched supplier where:
- `pt_trtype IN ('C', 'P')` — credit notes or payment-type credits
- `pt_trbal > 0` — has an unallocated positive balance

If found: `action = 'purchase_refund'`, stores credit note reference and amount.
If not found: `action = 'skip'` with reason "Receipt matches supplier but no unallocated credit note found".

### Customer Refund Detection (`_check_customer_refund`)

**Trigger:** A payment (debit) strongly matches a customer (score >= 0.8).

**Logic:** Query `stran` for the matched customer where:
- `st_trtype IN ('C', 'R')` — credit notes or refund-type credits
- `st_trbal < 0` — has an unallocated negative balance

If found: `action = 'sales_refund'`, stores credit note reference and amount.
If not found: `action = 'skip'` with reason "Payment matches customer but no unallocated credit note found".

### Why Not Keywords?

Bank descriptions like "REFUND" are unreliable — the same word appears in legitimate receipts and descriptions. The credit note lookup is the **only reliable** way to determine if a transaction is truly a refund, because:
1. A refund MUST have a corresponding credit note in Opera
2. Without a credit note, there's nothing to allocate against
3. The credit note amount confirms the refund value

---

## Duplicate Detection

### Detection Methods (in priority order)

1. **Fingerprint Match** (definitive) — exact same transaction hash previously imported
2. **High Confidence Match** (>= 0.9) — strong match on multiple criteria
3. **Cashbook Match** — same bank account + date + amount (±1p) in atran
4. **Purchase Ledger Match** — same supplier + date + amount (±0.01) in ptran (for payments)
5. **Sales Ledger Match** — same customer + date + amount (±0.01) in stran (for receipts)

### Duplicate Handling

- Duplicates flagged with `is_duplicate = True`
- Shown in "Already Posted" tab in the UI
- Can override and import if the user confirms it's not a duplicate

---

## Transaction Routing (API)

After matching, the API categorises transactions into tabs for the UI:

| Condition | Tab | User Action |
|-----------|-----|-------------|
| `action = 'sales_receipt'` | Receipts | Review, import |
| `action = 'purchase_payment'` | Payments | Review, import |
| `action = 'sales_refund'` | Refunds | Review, import |
| `action = 'purchase_refund'` | Refunds | Review, import |
| `action = 'repeat_entry'` | Repeat Entries | Info only (Opera auto-posts) |
| `action = 'bank_transfer'` | Bank Transfers | Review, import |
| `is_duplicate = True` or skip reason contains "Already" | Already Posted | Info only |
| All other (no match, ambiguous, etc.) | Unmatched | Manual assignment via dropdown |

**Important:** Non-matched transactions go to **Unmatched** (with dropdown for manual assignment), NOT to Skipped. This ensures the user can always assign them.

---

## Transaction Types

### Imported Transaction Types

| Bank Transaction | Opera Action | Cashbook Type |
|-----------------|--------------|---------------|
| Customer payment (credit) | Sales Receipt | Type 1 |
| Supplier payment (debit) | Purchase Payment | Type 2 |
| Customer refund (debit) | Sales Refund | Type 3 |
| Supplier refund (credit) | Purchase Refund | Type 6 |

### Skipped Transaction Types

These are typically not imported automatically:
- Direct Debits (DD)
- Standing Orders (STO)
- Card Payments
- Bank Charges
- Internal Transfers

---

## What Gets Posted to Opera

| Opera Table | Entry Type | Description |
|-------------|------------|-------------|
| `aentry` | Cashbook Header | Batch entry with bank reference |
| `atran` | Cashbook Lines | Transaction details (amount, account) |
| `stran` | Sales Ledger | Receipt allocation to customer |
| `ptran` | Purchase Ledger | Payment allocation to supplier |
| `ntran` | Nominal Ledger | Control account postings |
| `nacnt` | NL Balances | Updated period/YTD balances |
| `nbank` | Bank Balance | Updated bank current balance |

### Reference Preservation

Bank references are preserved in Opera entries:
- `ae_entref` / `at_refer` - Bank reference from statement
- Enables easy bank reconciliation later
- Statement and cashbook entries match exactly

---

## Alias Learning System

### How Aliases Are Created

Aliases are created from **two sources**:

1. **Manual assignment** — user selects a customer/supplier for an unmatched transaction in the UI. The mapping `bank_name → account_code` is saved immediately.
2. **Automatic learning** — when a fuzzy match scores >= `0.85` (learn threshold), the system automatically saves the alias for future fast-path lookups.

### How Aliases Are Used

During matching (Step 1 of the matching flow), the alias table is checked **before** fuzzy matching:

1. Look up the raw bank description (e.g., `"Giro Direct Credit From ABC Limited Ref: INV001"`)
2. If no match, look up the clean payee name (e.g., `"ABC Limited"`)
3. Alias lookups are **direction-aware** — receipts only match customer aliases (type `'C'`), payments only match supplier aliases (type `'S'`)
4. The matched account must still exist in the loaded customer/supplier list (prevents stale aliases from creating errors)

### Alias Storage

Aliases stored in `bank_aliases.db` (SQLite), per-company in `data/{company_id}/`:
- Separate from Opera database
- Portable across installations (can be imported via Company page)
- Each alias records: bank_name, ledger_type (C/S), account_code, match_score, account_name, source (FUZZY_MATCH/MANUAL_IMPORT), timestamp

### Managing Aliases

- Created automatically from manual assignments and high-confidence fuzzy matches
- Can be edited/deleted via API (`/api/bank-import/aliases`)
- Supports bulk import from another installation (Company page > Import Learned Data)

---

## Email Source (Optional)

Bank statements can also be imported from email:

1. **Scan Emails** - Search inbox for bank statement attachments
2. **Preview from Email** - Parse attached CSV/OFX file
3. **Import from Email** - Same workflow as file upload
4. **Track Processed** - Avoid reimporting same email

### Supported Email Patterns

- Attachments with `.csv`, `.ofx`, `.qif` extensions
- Subject lines containing "statement", "transactions"
- Sender addresses from known banks

---

## Configuration

### Bank Account Setup

Ensure Opera bank accounts have:
- Correct sort code (`nk_sort`)
- Account number (`nk_acntno`)
- Current balance (`nk_curbal`)

### Period Settings

Transactions must fall within an open posting period:
- Check `nparm` for current period settings
- Blocked periods reject imports
- Can override posting date if needed

---

## API Endpoints

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/bank-import/preview-multiformat` | POST | Parse and preview file |
| `/api/bank-import/import-with-overrides` | POST | Import selected transactions |
| `/api/bank-import/detect-format` | POST | Detect file format |
| `/api/bank-import/detect-bank` | POST | Detect bank from file |

### Account Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/bank-import/accounts/customers` | GET | Get customer list |
| `/api/bank-import/accounts/suppliers` | GET | Get supplier list |
| `/api/bank-import/config` | GET | Get import configuration |

### Utility Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/bank-import/check-duplicates` | POST | Check for duplicates |
| `/api/bank-import/correction` | POST | Save alias correction |
| `/api/bank-import/validate-csv` | POST | Validate bank account |
| `/api/bank-import/repeat-entries` | GET | Get repeat entry templates |

---

## File Structure

```
llmragsql/
├── api/main.py                    # API endpoints (~line 13053)
├── sql_rag/
│   ├── bank_import.py             # Main import logic (Opera SQL SE)
│   ├── bank_import_opera3.py      # Opera 3 version
│   ├── bank_parsers.py            # Multi-format parsers
│   ├── bank_matching.py           # Fuzzy matching algorithms
│   ├── bank_duplicates.py         # Duplicate detection
│   ├── bank_aliases.py            # Alias management
│   └── opera_sql_import.py        # Opera posting logic
├── frontend/src/pages/
│   └── Imports.tsx                # Import page UI
└── bank_aliases.db                # Alias database (git-ignored)
```

---

## Troubleshooting

### "No transactions found"

- Check file format is supported
- Verify file encoding (UTF-8, Latin-1)
- Check for header row format

### "No match found"

- Try adding an alias via manual selection
- Check customer/supplier exists in Opera
- Verify name spelling matches

### "Period is blocked"

- Transaction date outside current period
- Open the period in Opera or change date

### "Duplicate detected"

- Same transaction may have been imported
- Check Opera cashbook for existing entry
- Override if certain it's not duplicate

### Poor Match Quality

- Add search keys to customer/supplier records
- Create aliases for commonly mismatched names
- Check bank statement name truncation

---

## Best Practices

1. **Regular Imports** - Import frequently to keep up-to-date
2. **Review Matches** - Always review amber/unmatched items
3. **Create Aliases** - Build up alias database for better matching
4. **Preserve References** - Don't modify bank references in Opera
5. **Reconcile Promptly** - Use preserved references for bank rec
