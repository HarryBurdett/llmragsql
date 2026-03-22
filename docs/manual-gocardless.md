# GoCardless Import — Instruction Manual

> **Location:** Cashbook > GoCardless Import
> **Purpose:** Import GoCardless Direct Debit payouts as Sales Receipts in Opera

---

## How It Works

GoCardless collects Direct Debit payments from your customers and pays them out to your bank in batches. Each payout contains one or more customer payments minus GoCardless fees. This routine imports those payments into Opera as individual Sales Receipts.

---

## Before You Start

### 1. Configure Settings (GoCardless > Settings)

| Setting | What It Does |
|---------|--------------|
| **API Token** | Your GoCardless API key (live or sandbox) |
| **Default Bank Code** | The Opera bank account where payouts arrive |
| **Batch Type** | Cashbook type for receipts (e.g. R4) |
| **Fees Nominal Account** | Where GoCardless fees are posted (required if fees > 0) |
| **Fees VAT Code** | VAT code for fees (default: code 2 = 20%) |
| **GC Control Bank** | Optional intermediate clearing bank (see below) |
| **Subscription Tag** | Analysis code for repeat documents (default: SUB) |

### 2. Link Customers to GoCardless Mandates

Go to **GoCardless > Payment Requests** to set up mandates. A mandate authorises you to collect Direct Debits from a customer. Each customer needs an active mandate before you can collect payments.

---

## Import Workflow

### Step 1: Scan for Payouts
- Click **Scan** to fetch payouts from GoCardless API (or email inbox)
- Each payout shows: date, gross amount, fees, net amount, payment count
- Colour coding:
  - **Green** — ready to import
  - **Amber** — possible duplicate (check before importing)
  - **Grey** — already imported

### Step 2: Review Customer Matches
- Select a payout to see individual payments
- The system auto-matches payments to Opera customers by:
  1. **Mandate lookup** — definitive match via stored mandate link
  2. **Invoice reference** — if payment description contains an invoice number
  3. **Name matching** — fuzzy matching against Opera customer names
- Unmatched payments show a searchable dropdown — select the correct customer manually
- Toggle **Auto-Allocate** per payment to control invoice allocation

### Step 3: Import
- Set the **posting date** and confirm **bank code** and **batch type**
- Enter **GoCardless fees** and **VAT on fees** if applicable
- Click **Import** — the system posts:
  - One Sales Receipt per customer payment
  - One Nominal Payment for fees (with VAT tracking)
  - Bank balance update
  - All nominal ledger entries and transfer files

---

## Invoice Allocation

When **Auto-Allocate** is enabled, the system tries to match receipts to outstanding invoices:

| Priority | Method | When It Applies |
|----------|--------|-----------------|
| **1st** | Payment Request Invoices | If a payment request was raised for specific invoices, those exact invoices are allocated (see below) |
| **2nd** | Invoice Reference Match | If the payment description contains "INV12345" or similar, and amounts match exactly |
| **3rd** | Clear Whole Account | If the receipt amount equals the total outstanding balance on the customer's account |

If none of these apply, the receipt is posted **on account** (unallocated) for manual allocation in Opera.

---

## Payment Requests — Precise Invoice Allocation

The Payment Requests feature (GoCardless > Payment Requests) lets you collect specific invoices:

1. **Select invoices** — choose outstanding invoices for a customer with an active mandate
2. **Create payment request** — GoCardless collects the total via Direct Debit
3. **Payout arrives** — the system knows exactly which invoices were collected
4. **Auto-allocate** — allocates to those specific invoices automatically

### Important: Manual Payment Check

Between creating the payment request and the payout arriving (2-4 working days), someone may have paid those invoices manually in Opera. The system handles this:

- **Invoice still outstanding** — allocated normally
- **Invoice already paid** — skipped (not double-allocated)
- **Some paid, some outstanding** — allocates only to outstanding invoices
- **Excess amount** — any remainder stays on account as an unallocated receipt

This prevents double-allocation even when manual receipts overlap with GoCardless collections.

---

## The "SUB" Analysis Code — Preventing Duplicate Collections

### The Problem
If a customer has a GoCardless **subscription** (recurring Direct Debit) that automatically collects invoices, you don't want to also raise a **payment request** for those same invoices — that would collect them twice.

### The Solution
Tag repeat documents in Opera with the **SUB** analysis code so you can identify which invoices are already being collected by subscription.

### How to Apply SUB Tags
1. Go to **GoCardless > Settings**
2. Scroll to **Subscription Settings**
3. Set the tag (default: "SUB") and frequencies to include (Weekly/Monthly/Annual)
4. Click **Update Opera Documents**
5. Choose **Preview** to see what will be tagged, then **Apply** to update

### What It Does
- Finds all active repeat documents (`ihead` with status 'U') matching the selected frequencies
- Sets `ih_analsys = 'SUB'` on those documents
- Two modes:
  - **Non-overwrite** — only tags documents with no existing analysis code
  - **Overwrite** — tags all matching documents (replaces any existing code)

### How to Use It
When raising payment requests, check whether a customer's invoices come from a SUB-tagged repeat document. If they do, a subscription is already collecting those invoices — don't raise a separate payment request.

---

## GC Control Bank (Optional)

If configured, the system uses an intermediate "GC Control" bank account:

1. All receipts and fees post to the **GC Control bank** first
2. An automatic **bank transfer** moves the net payout amount to the **destination bank**
3. The GC Control bank should net to zero after each batch

This is useful if your GoCardless payouts arrive in a different bank account than where you want the receipts posted, or if you want a clearing account for audit purposes.

---

## Fees Handling

GoCardless charges fees on each payout. The system posts these as:

- **Nominal Payment** to your configured fees account
- **VAT tracking** — creates `zvtran` and `nvat` records for your VAT return
- Fees VAT is **input VAT** (reclaimable from HMRC)

You must configure `Fees Nominal Account` in settings before importing any payout with fees > 0.

---

## Duplicate Prevention

The system checks for duplicates at multiple levels:

1. **GoCardless reference** — exact match on the payout reference (most reliable)
2. **Net amount** — similar amount in the cashbook within recent date range
3. **Gross amount** — similar total in bank transactions within 14 days
4. **Batch pattern** — multiple payments matching the payout breakdown

Duplicates show an **amber warning**. You can override if you're certain it's not a duplicate.

---

## Foreign Currency Payouts

- EUR, USD, and other non-GBP payouts are **detected and flagged**
- A currency badge shows on the payout
- **Import is blocked** for foreign currency payouts — these must be processed manually
- Duplicate checking only uses reference (not amount) for foreign currency

---

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|------------|
| "Period is blocked" | Posting date falls in a closed period | Change posting date to an open period |
| "Fees Nominal Account not configured" | Fees > 0 but no account set | Configure in GoCardless Settings |
| Customer not matching | No mandate linked, name doesn't match | Use the dropdown to select manually; system learns for next time |
| "Duplicate detected" warning | Similar amount/reference found in Opera | Check the existing entry — override only if certain |
| Receipt not allocating to invoices | No matching invoice ref, or amount doesn't clear account | Allocate manually in Opera Cashbook > Allocate |

---

*Last updated: 2026-03-12*
