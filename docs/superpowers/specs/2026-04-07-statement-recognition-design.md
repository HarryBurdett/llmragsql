# Reliable Bank Statement Recognition — Design Spec

## Goal

Reliably determine the opening balance of any bank statement PDF, regardless of bank format. The opening balance must be mathematically verified, not trusted from AI extraction. This is fundamental — without a correct opening balance, the system can't determine which statement to process next.

## Problem

AI extraction of opening balances is unreliable. Different banks have different layouts:
- Some label the opening balance explicitly
- Some only have running balances per transaction
- Some have summary sections with totals
- Some show transactions newest-first, others oldest-first
- Some have no running balances at all
- The AI frequently picks up wrong numbers from summaries, other accounts, or misinterprets the first transaction's balance

## Approach

**Full extraction on every scan** with **code-verified opening balance calculation**. The AI extracts raw data. The code does the maths. Never trust the AI's opening balance figure.

---

## 1. Full Extraction on Scan

Every statement detected during the bank scan gets a full transaction extraction — not the lightweight info-only scan. The AI extracts all transactions with dates, amounts, descriptions, and running balances.

- Takes 5-10 seconds per statement (vs 2-3 for info-only)
- Results are cached by PDF content hash — only extracted once per PDF
- Subsequent views use the cache
- Accuracy is non-negotiable — speed is secondary

The `extract_statement_info_only` method is deprecated for bank scan purposes. The scan calls `extract_transactions_from_pdf` directly.

---

## 2. Statement Format Detection

The AI identifies which format the statement uses before extracting:

### Format Types

| Format | Description | How to determine opening |
|--------|-------------|-------------------------|
| `running_balance` | Each transaction has a running balance column | Dual-interpretation chain validation |
| `summary_and_transactions` | Summary section with opening/closing + transaction list with balances | Use summary opening, verify with chain |
| `summary_only` | Summary totals only, no individual transactions | Use summary, verify: opening + in - out = closing |
| `no_balance` | Transactions have amounts but no running balance or summary | Must use Opera nk_recbal + sum of transactions |

### Transaction Order

| Order | Description |
|-------|-------------|
| `oldest_first` | Earliest transaction at top (most common) |
| `newest_first` | Latest transaction at top (some online banks) |

---

## 3. Opening Balance Calculation

### For `running_balance` format (most common):

**Step 1 — Sort transactions chronologically** (earliest date first, regardless of PDF order)

**Step 2 — Find the first real transaction**: Skip balance-only lines ("Start Balance", "Balance brought forward" with no amount). The first line with a date, an amount (money_in or money_out > 0), AND a running balance.

**Step 3 — Try both interpretations**:

- **Interpretation A**: Balance on first line INCLUDES the transaction
  `opening = first_balance + money_out - money_in`

- **Interpretation B**: Balance on first line IS the opening (transaction hasn't been applied)
  `opening = first_balance`

**Step 4 — Chain validation**: For each interpretation, walk forward through every transaction chronologically:
  `next_expected = current + money_in - money_out`
  Compare to actual balance on each line. If chain reaches closing balance within £0.01, that interpretation is correct.

**Step 5 — Result**:
- One interpretation chains → use it (correct opening)
- Both chain (identical values) → no transaction on first line, use either
- Neither chains → flag as "unverified"

### For `summary_and_transactions` format:

- Read opening balance from summary section
- Verify: walk transactions from summary opening to closing
- If chain validates, use summary opening
- If not, fall back to dual-interpretation on transactions

### For `summary_only` format:

- Opening and closing from summary
- Verify: opening + total_in - total_out = closing
- If maths checks out, use summary opening

### For `no_balance` format:

- No balance data available from the statement
- Opening = Opera's `nk_recbal` (only option)
- Calculate closing: opening + sum(money_in) - sum(money_out)
- Compare calculated closing to any closing figure found on statement
- Flag if they differ

---

## 4. AI Extraction Prompt

The AI prompt must:

1. **Identify format first** — scan the entire document before extracting
2. **Extract everything raw** — all transactions, all summary data, all balance figures
3. **Report format indicator** — so code knows which validation to apply
4. **NOT calculate opening balance** — report what's on the statement, let code calculate
5. **Report transaction order** — oldest_first or newest_first

### Return Structure

```json
{
    "statement_info": {
        "bank_name": "Monzo",
        "account_number": "39913585",
        "sort_code": "04-00-04",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD",
        "period_end": "YYYY-MM-DD",
        "format": "running_balance",
        "transaction_order": "oldest_first",
        "summary": {
            "opening_balance": null,
            "closing_balance": 88626.78,
            "total_in": null,
            "total_out": null
        },
        "opening_balance": null,
        "closing_balance": 88626.78
    },
    "transactions": [
        {
            "date": "YYYY-MM-DD",
            "description": "HISCOX",
            "money_out": 32.64,
            "money_in": null,
            "balance": 88626.78,
            "type": "DD"
        }
    ]
}
```

The `opening_balance` in statement_info is ONLY populated if explicitly labelled on the statement. Otherwise null — the code calculates it.

---

## 5. Scan Queue Display

After extraction and validation, the scan queue shows:

- **Opening balance**: calculated by code, verified by chain
- **Closing balance**: from statement (last transaction balance or summary)
- **Status**:
  - "Ready" — opening matches Opera's `nk_recbal` within £0.01
  - "Pending" — a prior statement must be processed first
  - "Unverified" — chain validation failed, needs manual review
- **Order**: Ready statements first, sorted by date

The user always processes the top "Ready" statement. No manual sequence checking needed.

---

## 6. What Changes

### Remove
- `stmt['opening_balance'] = rec_bal` override in scan-all-banks (wrong fix that hid mismatches)
- Reliance on `extract_statement_info_only` for bank scan balance display

### Replace
- Info-only scan with full extraction during bank scan
- AI-provided opening balance with code-calculated opening balance

### Add
- Format detection in AI prompt (`running_balance` / `summary_and_transactions` / `summary_only` / `no_balance`)
- Dual-interpretation chain validation in `_parse_extraction_result`
- Transaction order detection and chronological sorting
- "Unverified" status for statements where chain doesn't validate

### Keep
- PDF content hash caching (extracted once, cached forever)
- Bank matching by sort code + account number
- Opera `nk_recbal` comparison for Ready/Pending status
- All existing transaction extraction quality

---

## 7. Success Criteria

1. Monzo statement opening balance calculated correctly (£88,659.42 not £108,377.38)
2. Barclays, NatWest, HSBC statements continue to extract correctly
3. Every opening balance verified by chain validation — never trust AI's figure
4. Scan queue clearly shows which statement to process next
5. Unverified statements flagged, not silently wrong
6. Cache means re-scanning is instant after first extraction
