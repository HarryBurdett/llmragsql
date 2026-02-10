# GoCardless Integration Guide

## Overview

The GoCardless integration enables automatic import of Direct Debit payments into Opera SQL SE. GoCardless is a payment processor that collects Direct Debit payments from customers and pays them out to your bank account in batches. This integration automates the process of recording these payments as Sales Receipts in Opera.

### What Does It Do?

1. **Fetches payment data** from GoCardless (via API or email notifications)
2. **Matches payments** to Opera customers using invoice references and fuzzy name matching
3. **Creates Sales Receipts** in Opera's Cashbook for each customer payment
4. **Posts fees** as a separate nominal payment to a designated fees account
5. **Handles foreign currency** payouts (EUR, USD, etc.)

---

## Data Sources

The integration supports two data sources for retrieving GoCardless payments:

### 1. GoCardless API (Recommended)

Direct connection to GoCardless API for retrieving payout and payment data.

**Advantages:**
- Real-time access to all payouts
- Complete payment history
- More reliable than email parsing
- Includes detailed payment metadata

**Requirements:**
- GoCardless API access token (from GoCardless dashboard)
- Network access to GoCardless API endpoints

**Configuration:**
- Navigate to **Settings > GoCardless Settings**
- Select "GoCardless API" as data source
- Enter your API access token
- Choose environment (Live or Sandbox for testing)

### 2. Email Scanning

Parses GoCardless payout notification emails from your inbox.

**Advantages:**
- Works without API access
- Can process historical emails

**Requirements:**
- Email provider configured (IMAP, Gmail, or Microsoft)
- GoCardless notification emails in your inbox

**Note:** Email parsing is less reliable as email formats may change. API is preferred when available.

---

## Configuration

### GoCardless Settings

Access via **Settings > GoCardless Settings** or the settings icon on the GoCardless Import page.

| Setting | Description | Example |
|---------|-------------|---------|
| **Data Source** | API or Email scanning | API (recommended) |
| **API Access Token** | GoCardless API token | `live_xxxxx...` |
| **API Environment** | Live or Sandbox | Live |
| **Default Batch Type** | Cashbook batch type for receipts | R4 (Sales Receipts) |
| **Default Bank Account** | Opera bank account code | BC010 |
| **Fees Nominal Account** | Account for GoCardless fees | GA030 |
| **Fees VAT Code** | VAT code for fees | 2 (20%) |
| **Fees Payment Type** | Payment type for fees entry | P4 |
| **Company Reference** | Filter emails by company (optional) | INTSYSUKLTD |
| **Archive Folder** | Email folder for processed emails | Archive/GoCardless |

### Settings Storage

Settings are stored in `gocardless_settings.json` in the application root. This file is excluded from version control (.gitignore) as it contains the API token.

---

## Import Workflow

### Step 1: Scan for Payments

**API Mode:**
1. Click "Scan for Payments"
2. System connects to GoCardless API
3. Retrieves payouts from the last 30 days
4. Displays list of payouts with status indicators

**Email Mode:**
1. Click "Scan Emails"
2. System searches inbox for GoCardless notification emails
3. Parses email content to extract payment data
4. Displays list of batches found

### Step 2: Review and Match Customers

1. Click on a payout/batch to expand it
2. System automatically matches payments to Opera customers using:
   - **Invoice reference matching** (highest priority) - extracts INV numbers from description
   - **Amount matching** - finds outstanding invoices with matching amounts
   - **Name matching** - fuzzy matches customer names
3. Review matches and manually select customers for unmatched payments
4. Use the searchable dropdown to find customers by name or account code

### Step 3: Import to Opera

1. Set the posting date (defaults to payout arrival date)
2. Verify all payments have matched customers (required)
3. Click "Import to Opera"
4. System creates:
   - **Sales Receipts** for each customer payment
   - **Nominal Payment** for GoCardless fees
   - **Cashbook entry** with the payout reference

### What Gets Posted to Opera

| Opera Table | Entry Type | Description |
|-------------|------------|-------------|
| `aentry` | Cashbook Header | Batch entry with payout reference |
| `atran` | Cashbook Lines | Individual payment lines (type 1 = Receipt) |
| `stran` | Sales Ledger | Receipt allocation to customer account |
| `ntran` | Nominal Ledger | Control account and fee postings |
| `nacnt` | NL Balances | Updated account balances |
| `nbank` | Bank Balance | Updated bank current balance |

---

## Customer Matching

### Matching Priority

1. **Invoice Reference** (Score: 100%)
   - Extracts references like `INV26388` from payment description
   - Looks up in `stran` (Sales Transactions) to find customer
   - Patterns recognized:
     - `INV26388` or `INV 26388`
     - `Invoice #12345`
     - `#26388` (4+ digit numbers)
     - `SI12345` (Sales Invoice prefix)

2. **Amount Matching** (Score: 90%)
   - Finds outstanding invoices matching the exact payment amount
   - Useful when description has no invoice reference

3. **Name Matching** (Score: 50-100%)
   - Fuzzy matches GoCardless customer name to Opera customer names
   - Exact match: 100%
   - Contains match: Variable based on overlap
   - Word match: Based on common words

4. **Description Matching** (Score: 50%+)
   - Extracts company name from description
   - Matches against Opera customer names
   - Helps when customer_name is "Unknown"

### Manual Customer Selection

For unmatched payments:
1. Click the customer dropdown
2. Type to search by name or account code
3. Scroll through results
4. Select the correct customer
5. Customer is highlighted green when matched

---

## Foreign Currency Handling

GoCardless can process payments in multiple currencies (EUR, USD, CAD, AUD) and convert them to your payout currency (typically GBP).

### How Foreign Currency is Detected

- **API Mode:** Currency code from payout data
- **Email Mode:** Currency codes in payment lines (e.g., "615.00 EUR")

### Foreign Currency Display

- Payouts in foreign currency show a currency badge (e.g., "EUR")
- Warning message indicates foreign currency
- Gross amounts shown in original currency

### Duplicate Detection for Foreign Currency

Foreign currency payouts cannot be matched by amount (comparing EUR to GBP values doesn't work). The system:
- Only checks by **payout reference** for foreign currency
- Does NOT compare amounts across currencies
- Warning message clarifies: "note: foreign currency, GBP equivalent"

---

## Duplicate Detection

The system checks if a payout may have already been imported:

### Detection Methods

1. **By Payout Reference**
   - Checks `ae_entref` (entry reference) in Cashbook
   - Matches the unique payout reference (e.g., `R2VB7P`)

2. **By Gross Amount** (GBP only)
   - Finds cashbook entries within £1 of the payout gross
   - Only for same-currency payouts

3. **By Batch Pattern**
   - Identifies batches with matching payment counts and totals
   - Catches manual imports without proper reference

4. **By Individual Payment Amount**
   - Checks if individual payments were posted separately
   - Looks for entries with "GC" or "GoCardless" in reference

### Duplicate Indicators

- **Orange/Amber highlight** on potentially duplicate payouts
- **Warning message** showing matched entry date and amount
- **Confirmation required** before importing duplicates

---

## Fees Handling

GoCardless charges fees for payment processing:

| Fee Type | Description |
|----------|-------------|
| GoCardless Fees | Per-transaction fees |
| App Fees | Partner/integration fees (if applicable) |
| VAT on Fees | UK VAT on fees (typically 20%) |

### Fees Posting

- Fees are posted as a **separate Nominal Payment** (negative amount)
- Posted to the configured **Fees Nominal Account**
- VAT posted based on configured **VAT Code**
- Reduces the net amount received in bank

### Fees Calculation

```
Net Amount = Gross Amount - GoCardless Fees - App Fees - VAT on Fees
```

---

## API Endpoints

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/api-payouts` | GET | Fetch payouts from GoCardless API |
| `/api/gocardless/scan-emails` | GET | Scan inbox for GoCardless emails |
| `/api/gocardless/match-customers` | POST | Match payments to Opera customers |
| `/api/gocardless/import` | POST | Import batch to Opera |
| `/api/gocardless/import-from-email` | POST | Import from specific email |

### Configuration Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/settings` | GET/POST | Get/Save settings |
| `/api/gocardless/test-api` | POST | Test API connection |
| `/api/gocardless/batch-types` | GET | Get available batch types |
| `/api/gocardless/bank-accounts` | GET | Get Opera bank accounts |
| `/api/gocardless/nominal-accounts` | GET | Get nominal accounts |
| `/api/gocardless/vat-codes` | GET | Get VAT codes |
| `/api/gocardless/payment-types` | GET | Get payment types |

### Utility Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/gocardless/import-history` | GET | Get import history |
| `/api/gocardless/archive-email` | POST | Archive processed email |
| `/api/gocardless/validate-date` | GET | Validate posting period |
| `/api/gocardless/ocr` | POST | OCR image to extract data |

---

## File Structure

```
llmragsql/
├── api/main.py                    # API endpoints (GoCardless section ~line 15596)
├── sql_rag/
│   ├── gocardless_api.py          # GoCardless API client
│   ├── gocardless_parser.py       # Email content parser
│   └── opera_sql_import.py        # Opera posting logic
├── frontend/src/pages/
│   ├── GoCardlessImport.tsx       # Main import page
│   └── Settings.tsx               # Settings page
├── gocardless_settings.json       # Configuration (git-ignored)
└── docs/
    └── gocardless_integration.md  # This document
```

---

## Troubleshooting

### "No payouts found"

- **API Mode:** Check API token is valid and not expired
- **Email Mode:** Verify GoCardless emails are in inbox (not spam)
- Check date range - default is last 30 days

### Customer Not Matching

- Verify customer exists in Opera Sales Ledger
- Check invoice reference format matches Opera
- Try manual search using account code or name

### "Period is blocked"

- Posting date falls outside current accounting period
- Change posting date to current period
- Or open the required period in Opera

### Duplicate Detection False Positives

- The system may flag legitimate new imports as duplicates
- Review the warning message carefully
- Check the referenced entry in Opera Cashbook
- Proceed if you're certain it's not a duplicate

### Foreign Currency Amount Mismatch

- Foreign currency payouts show amounts in original currency
- Opera receives the GBP equivalent after conversion
- Duplicate detection only uses reference matching for foreign currency

---

## Security Notes

- **API Token:** Keep your GoCardless API token secure
- **Settings File:** `gocardless_settings.json` is excluded from git
- **Email Access:** Only emails matching GoCardless patterns are processed
- **Opera Credentials:** Standard Opera SQL SE authentication is used

---

## Version History

| Date | Changes |
|------|---------|
| Feb 2026 | Improved foreign currency duplicate detection |
| Feb 2026 | Enhanced customer matching with description keywords |
| Feb 2026 | Added API as default data source |
| Jan 2026 | Initial GoCardless integration |
