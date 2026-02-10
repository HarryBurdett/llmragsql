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

## Matching Logic

### Customer Matching (Receipts)

The system matches bank statement names to Opera customers using multiple strategies:

1. **Alias Lookup** (Highest Priority)
   - Checks `bank_aliases.db` for saved mappings
   - Previous manual assignments are remembered

2. **Exact Name Match** (Score: 100%)
   - Direct match to `sn_name` (customer name)

3. **Fuzzy Matching** (Score: 50-99%)
   - Token-based matching (word order independent)
   - Abbreviation normalization (LTD → LIMITED)
   - Word containment (handles truncated bank names)
   - Phonetic matching (Metaphone algorithm)
   - Levenshtein distance (handles typos)
   - N-gram similarity (partial matches)

4. **Search Key Match**
   - Matches against `sn_key1` through `sn_key4`

### Supplier Matching (Payments)

Same strategies applied to suppliers:
- Primary name (`pn_name`)
- Payee name (`pn_payee`)
- Search keys (`pn_key1-4`)

### Match Sources

Each match indicates its source:
- `alias` - From saved alias database
- `fuzzy` - Fuzzy name matching
- `enhanced` - Enhanced matching with phonetics
- `ai` - AI-assisted categorization (if enabled)

---

## Duplicate Detection

### Fingerprint System

Each imported transaction generates a unique fingerprint:
```
BKIMP:{hash8}:{YYYYMMDD}
```
- `BKIMP` - Prefix identifying bank import origin
- `hash8` - 8-character MD5 hash of name|amount|date
- `YYYYMMDD` - Import date

### Detection Methods

1. **Fingerprint Match** - Exact same transaction previously imported
2. **Amount + Date Match** - Same value within ±1 day
3. **Reference Match** - Same bank reference in existing entries

### Duplicate Handling

- Duplicates shown in "Already Posted" tab
- Warning displays matched entry details
- Can override and import anyway if needed

---

## Transaction Types

### Imported Transaction Types

| Bank Transaction | Opera Action | Cashbook Type |
|-----------------|--------------|---------------|
| Customer payment (credit) | Sales Receipt | Type 1 |
| Supplier payment (debit) | Purchase Payment | Type 2 |
| Customer refund (debit) | Sales Refund | Type 2 (contra) |
| Supplier refund (credit) | Purchase Refund | Type 1 (contra) |

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

### How It Works

1. User manually assigns a customer/supplier to an unmatched transaction
2. System saves the mapping: `bank_name → opera_account`
3. Future imports with same bank name auto-match

### Alias Storage

Aliases stored in `bank_aliases.db` (SQLite):
- Separate from Opera database
- Portable across installations
- Can be backed up/exported

### Managing Aliases

- Aliases created automatically from manual assignments
- Can be edited/deleted via API
- Supports bulk import/export

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
