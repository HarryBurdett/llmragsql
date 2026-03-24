# Cashbook Reconciliation Guide

## Overview

Cashbook Reconciliation (Bank Statement Reconciliation) is the process of matching your bank statement against Opera's Cashbook entries. This ensures that the cashbook accurately reflects what has actually cleared through the bank.

### What Does It Do?

1. **Extracts transactions** from bank statement PDFs/images using AI (Claude Vision)
2. **Matches statement transactions** to unreconciled Opera cashbook entries
3. **Marks entries as reconciled** when confirmed
4. **Tracks reconciliation status** with statement date and closing balance
5. **Highlights discrepancies** between cashbook and bank

---

## Key Concepts

### Reconciled vs Unreconciled

| Status | Meaning |
|--------|---------|
| **Unreconciled** | Entry exists in Opera but not yet confirmed on bank statement |
| **Reconciled** | Entry confirmed as appearing on bank statement |

### Reconciliation Fields in Opera

| Field | Table | Description |
|-------|-------|-------------|
| `ae_stmtdate` | aentry | Statement date when reconciled |
| `ae_complet` | aentry | Entry complete flag |
| `at_stmtno` | atran | Statement sequence number |

---

## Reconciliation Workflow

### Step 1: Select Bank Account

1. Navigate to **Cashbook > Cashbook Reconcile**
2. Select the bank account from dropdown (e.g., BC010 - Current Account)
3. System displays reconciliation status and unreconciled entries

### Step 2: Enter Statement Details

Enter from your bank statement:
- **Statement Date** - Date printed on statement
- **Closing Balance** - End balance shown on statement

### Step 3: Match Entries

Two modes available:

#### Manual Mode
1. Review list of unreconciled cashbook entries
2. Tick entries that appear on your bank statement
3. Each tick marks that entry for reconciliation

#### Auto-Match Mode
1. Upload bank statement (PDF or image)
2. AI extracts transactions from statement
3. System auto-matches to cashbook entries
4. Review and confirm suggested matches
5. Manually match any unmatched items

### Step 4: Verify and Complete

1. Check the **Difference** amount
   - Should be **£0.00** when fully reconciled
   - Difference = Closing Balance - (Opening Balance + Reconciled Items)
2. Click **Complete Reconciliation** when difference is zero
3. System marks selected entries as reconciled

---

## Auto-Match (AI-Powered)

### How It Works

1. **Upload Statement** - PDF, PNG, or JPG of bank statement
2. **AI Extraction** - Claude Vision reads the statement and extracts:
   - Transaction dates
   - Descriptions
   - Amounts (debits/credits)
   - Running balances
   - Statement metadata (account number, period, etc.)
3. **Intelligent Matching** - System matches by:
   - Amount (exact match)
   - Date (same day or ±1 day)
   - Reference similarity
   - Description keywords
4. **Review Matches** - User confirms or adjusts matches

### Match Scoring

| Score | Confidence | Action |
|-------|------------|--------|
| 90-100% | High | Auto-match (green) |
| 70-89% | Medium | Review suggested (amber) |
| Below 70% | Low | Manual match required |

### Match Reasons

The system explains why matches were made:
- "Exact amount match"
- "Date within 1 day"
- "Reference contains 'INV12345'"
- "Description similarity 85%"

---

## Statement Processing

### Supported Formats

| Format | Notes |
|--------|-------|
| **PDF** | Multi-page supported, any bank format |
| **PNG/JPG** | Single page scanned statements |
| **Email** | Scan inbox for statement attachments |

### Bank Detection

System auto-detects bank from:
- Sort code / Account number
- Bank logo and letterhead
- Statement format patterns

Matches to Opera bank account via `nbank` table.

---

## Reconciliation Status

### Status Display

For each bank account:
- **Total Unreconciled Entries** - Count of items not yet reconciled
- **Unreconciled Value** - Sum of unreconciled amounts
- **Last Statement Date** - Most recent reconciliation date
- **Oldest Unreconciled** - Date of oldest pending item

### Entry List

Each unreconciled entry shows:
| Field | Description |
|-------|-------------|
| Date | Transaction date |
| Reference | Entry reference |
| Details | Transaction description |
| Debit | Payment amount (if applicable) |
| Credit | Receipt amount (if applicable) |
| Select | Checkbox to mark for reconciliation |

---

## Completing Reconciliation

### Pre-Completion Checks

Before completing, verify:
1. **Difference is zero** - All items accounted for
2. **Statement date is correct** - Matches bank statement
3. **Closing balance matches** - Agrees with statement

### What Happens on Complete

1. Selected entries marked with statement date
2. `ae_stmtdate` updated in aentry
3. `at_stmtno` incremented in atran
4. Entries no longer appear as unreconciled

### Unreconciling Entries

If an entry was reconciled in error:
1. Find the entry in reconciliation history
2. Click "Unreconcile"
3. Entry returns to unreconciled list

---

## Handling Discrepancies

### Common Causes

| Issue | Cause | Solution |
|-------|-------|----------|
| Missing entry | Transaction not recorded | Create cashbook entry |
| Extra entry | Duplicate or error | Investigate and correct |
| Amount difference | Data entry error | Correct the entry |
| Date difference | Posted wrong date | Adjust if material |

### Unmatched Statement Items

If bank statement shows transactions not in Opera:
1. Check if entry exists with different date/amount
2. Create new cashbook entry if missing
3. May indicate missing import or manual entry needed

### Unmatched Opera Entries

If Opera has entries not on statement:
- Transaction may not have cleared yet
- Leave unreconciled until it appears
- Check if entry was posted in error

---

## API Endpoints

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/banks` | GET | List all bank accounts |
| `/api/reconcile/bank/{code}` | GET | Get bank details |
| `/api/reconcile/bank/{code}/status` | GET | Reconciliation status |
| `/api/reconcile/bank/{code}/unreconciled` | GET | List unreconciled entries |

### Processing Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/process-statement` | POST | Extract from PDF/image |
| `/api/reconcile/process-statement-unified` | POST | Extract with auto-matching |
| `/api/reconcile/bank/{code}/import-from-statement` | POST | Import statement file |

### Action Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/reconcile/bank/{code}/mark-reconciled` | POST | Mark entries reconciled |
| `/api/reconcile/bank/{code}/unreconcile` | POST | Unreconcile an entry |
| `/api/reconcile/bank/{code}/confirm-matches` | POST | Confirm auto-matches |
| `/api/reconcile/bank/{code}/complete-batch` | POST | Complete entry batch |

---

## File Structure

```
llmragsql/
├── api/main.py                           # API endpoints (~line 10098)
├── sql_rag/
│   ├── statement_reconcile.py            # Reconciliation logic (SQL SE)
│   └── statement_reconcile_opera3.py     # Opera 3 version
├── frontend/src/pages/
│   └── BankStatementReconcile.tsx        # Reconciliation UI
└── docs/
    └── cashbook_reconcile.md             # This document
```

---

## Troubleshooting

### "No unreconciled entries found"

- All entries may already be reconciled
- Check bank account selection is correct
- Verify entries exist in the date range

### AI Extraction Failed

- Check image quality (300+ DPI recommended)
- Ensure statement is clearly visible
- Try uploading as PDF instead of image
- Check API key is configured

### Match Score Too Low

- Bank description may be truncated
- Try manual matching
- Check if amounts match exactly

### Difference Not Zero

- Missing entries need to be created
- Check for entries posted to wrong bank
- Review any items marked incorrectly

### Statement Date Issues

- Use date from printed statement
- Ensure format is YYYY-MM-DD
- Check period is not closed

---

## Best Practices

1. **Reconcile Regularly** - Weekly or monthly at minimum
2. **Import First** - Process bank statement imports before reconciling
3. **Match Carefully** - Review all matches before confirming
4. **Zero Difference** - Don't complete until difference is zero
5. **Keep Statements** - Archive processed statements for audit
6. **Check Old Items** - Investigate entries unreconciled for >30 days
