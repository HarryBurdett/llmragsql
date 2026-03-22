# GoCardless Partner App Registration

## Overview

We need to register our OAuth app with GoCardless so that new customers can sign up for GoCardless directly from within Crakd.ai. We're already a GoCardless Partner — this is just creating the OAuth app credentials.

## Steps

### 1. Log into GoCardless Dashboard

- **Sandbox (test first):** https://manage-sandbox.gocardless.com
- **Live:** https://manage.gocardless.com

### 2. Create an OAuth App

1. Go to **Developers** in the left sidebar
2. Click **Create** > **OAuth app**
3. Fill in:
   - **App name:** `Crakd.ai DD`
   - **Description:** `Direct Debit management for Pegasus Opera — automated payment collection, posting, and reconciliation`
   - **Redirect URI:** `http://localhost:5173/cashbook/gocardless-callback`
     *(Update this to the production URL when deployed, e.g. `https://app.crakd.ai/cashbook/gocardless-callback`)*
4. Click **Create**
5. Copy the **Client ID** and **Client Secret** — you'll need both

### 3. Enter Credentials in Crakd.ai

1. Open Crakd.ai and go to **Cashbook > GoCardless Settings**
2. If testing, tick **Sandbox mode**
3. Scroll down to the **Partner Referral** section
4. Enter:
   - **Client ID** — from step 2
   - **Client Secret** — from step 2
   - **Redirect URI** — `http://localhost:5173/cashbook/gocardless-callback` (should be pre-filled)
5. Click **Save**

### 4. Test the Signup Flow

1. Go to **Cashbook > GoCardless Signup** (or click the GoCardless Signup tile on the Home page)
2. Enter a test company name and email
3. Click **Continue to GoCardless** — it should open the GoCardless signup page in a new tab
4. Complete the registration there
5. Return to Crakd.ai — the wizard should detect completion automatically

### 5. Go Live

Once tested in sandbox:
1. Repeat step 2 on the **live** GoCardless dashboard (https://manage.gocardless.com)
2. Enter the live credentials in GoCardless Settings with sandbox mode **off**
3. Update the Redirect URI to the production URL

## What This Enables

Once registered, new customers visiting the GoCardless Signup page will be able to:
- Create a GoCardless account directly from Crakd.ai
- Authorise our app to manage their Direct Debits
- Start collecting payments immediately — all linked to their Opera accounting system

## Partner Application Pitch (for reference)

**Product Name:** Crakd.ai DD — Direct Debit Management for Pegasus Opera

Crakd.ai is an AI-powered automation layer for Pegasus Opera, one of the UK's most established SME accounting systems. Our GoCardless integration enables Opera users to collect Direct Debit payments with full end-to-end automation — from mandate setup through to posting, allocation, and reconciliation.

**Integration features:**
- Onboard new merchants via OAuth Connect (Partner flow)
- Create and manage mandates against Sales Ledger accounts
- Raise payment requests linked to outstanding invoices
- Import payouts automatically — matching to Opera customers, posting as Sales Receipts
- Track fees with VAT for accurate HMRC returns
- Auto-allocate payments to invoices

**Target market:** UK SMEs running Pegasus Opera SQL SE or Opera 3 (estimated 10,000+ active installations)

**Contact:** Charlie Burdett — charlieb@intsysuk.com
