# GoCardless Direct Debit Promotion — Campaign & Materials Design

**Date**: 2026-04-16
**Author**: Charlie Burdett / Claude
**Status**: Draft

---

## Overview

A promotional campaign and materials package for Crakd.ai's GoCardless Direct Debit collection solution. The campaign sells two layers — why Direct Debit collection is better than chasing payments, then how Crakd.ai makes the entire process effortless from invoice to reconciliation.

### Business Model

- **GoCardless fees**: Standard GoCardless DD rates — no markup to the customer. Crakd.ai earns commission from GoCardless on every collection.
- **Crakd.ai subscription**: Monthly fee for the automation platform (GoCardless app is one module within the broader Crakd.ai offering).
- **GoCardless signup**: New users register via a dedicated page on crakd.ai, which routes through the GoCardless partner OAuth flow.

### Phased Approach

- **Phase 1** (this spec): Opera users — they may or may not already use Direct Debit or GoCardless.
- **Phase 2** (future): Prospects — businesses not on Opera, where the pitch is full automation and Opera runs quietly underneath.

---

## Target Audience — Phase 1

**Existing Pegasus Opera users** (SQL SE and Opera 3). Finance teams at UK SMEs: financial controllers, bookkeepers, office managers, FDs.

These users already know Opera. The message is that DD collection isn't a bolt-on from a third party — it's a natural extension of what they already use. Receipts appear in their cashbook, allocations happen against their invoices, bank reconciliation matches. Nothing new to learn, just less to do.

Some of these users may already use GoCardless (or another DD provider). The campaign doesn't assume their starting point — it sells the automation story regardless, with a self-selection fork at the CTA stage:
- **"New to GoCardless?"** → Sign up via Crakd portal
- **"Already use GoCardless?"** → Subscribe to Crakd.ai to add automation

---

## Two-Layer Narrative

### Layer 1 — Why Direct Debit Collection

Sell the concept before the product. Many Opera users still rely on BACS payments, cheques, or card payments initiated by the customer. The first job is to shift their thinking.

**Key messages:**
- You're chasing payments. Chasing costs time, delays cash, ties up your team.
- Direct Debit flips the model: you collect on your schedule, not when customers remember.
- 97% of UK businesses use DD for regular payments. It's the most trusted payment method in the UK.
- Predictable cash flow, fewer bad debts, lower admin cost.
- Customers sign a mandate once (2 minutes, online). After that, you collect when invoices are due.

### Layer 2 — How Crakd.ai Makes It Effortless

Once they accept the DD concept, show how the entire cycle is automated.

**Key messages:**
- Select invoices due, click collect. GoCardless handles the payment.
- When the money lands, everything posts automatically — receipts, invoice allocations, fees with VAT, bank reconciliation.
- Zero manual data entry. Your books are up to date before you've opened Opera.
- Works with your existing Opera system (SQL SE or Opera 3). Nothing to migrate.
- Full audit trail — every receipt linked to its GoCardless reference, every fee tracked.

---

## Campaign Channels

### Digital
- Landing page on crakd.ai
- Email campaign sequence
- LinkedIn content (posts, potentially ads)
- Interactive demo (refreshed from existing materials)

### Direct Sales
- Sales email templates for outreach
- Product one-pager (PDF leave-behind)
- Detailed "how it works" guide

---

## Deliverables — Phase 1

### 1. Product Landing Page (crakd.ai/gocardless)

The central destination. All email campaigns, social posts, and sales outreach point here.

**Structure:**
- **Hero**: Headline + subheadline capturing the two-layer message. Primary CTA.
- **The problem**: What finance teams deal with today (chasing, manual posting, reconciliation headaches).
- **The solution**: Direct Debit collection + full automation. Visual step-by-step (4 steps).
- **How it works**: Mandate signup → invoice selection → collection → automatic posting. With screenshots or illustrations.
- **The numbers**: ROI proof points (see below).
- **Pricing clarity**: GoCardless fees (standard, transparent) + Crakd.ai subscription. No hidden costs.
- **Demo embed**: Interactive demo or video walkthrough.
- **Two CTAs at bottom**:
  - "Sign up to GoCardless" → Crakd portal registration
  - "Already use GoCardless?" → Crakd.ai subscription / contact

**Tone**: Professional, precise, confident. Numbers and specifics, not vague promises. Written for finance people.

### 2. Product One-Pager (PDF)

Leave-behind for sales conversations and email attachments. Self-contained — someone should understand the full proposition from this single page.

**Front:**
- The problem (2-3 bullet points)
- The solution (headline + 4-step visual)
- Key stats (hours saved, receipts automated, VAT reclaimed)

**Back:**
- How it works (more detail, with screenshot thumbnails)
- Getting started (3 steps: sign up → set up mandates → collect)
- Pricing overview
- Contact / CTA

**Format**: A4 PDF, print-friendly, branded to Crakd.ai. Both Opera SE and Opera 3 mentioned.

### 3. Detailed "How It Works" Guide (PDF + web version)

Comprehensive walkthrough covering every step from signup to first collection. Written for finance people who want to understand exactly what happens before they commit.

**Sections:**
1. **Signing up to GoCardless** — What it is, what it costs, how to register via Crakd portal. What information is needed, how long it takes, what happens next.
2. **Setting up mandates** — How to invite customers to sign a DD mandate. What the customer sees. How long it takes. What "active mandate" means.
3. **Requesting payments** — Selecting invoices, combining multiple invoices per customer, clicking collect. What happens if an invoice is part-paid or disputed.
4. **Collection and payout** — GoCardless collects from customers (2-4 working days). Payout arrives as a single bank credit. Timeline explained.
5. **Automatic import and posting** — One click to import the payout. What gets created: sales receipts, invoice allocations, fee entries with VAT, nominal postings, bank balance update. All automatic.
6. **Reconciliation** — How the bank statement matches (control bank method explained). Statement line = one Opera entry.
7. **Subscriptions and recurring collections** — Setting up repeat collections for monthly customers. SUB tags. Automatic amount sync.
8. **Fees and VAT** — GoCardless fees separated automatically. VAT tracked as input tax (reclaimable). Posted to configured nominal account.
9. **Settings and configuration** — One-time setup: API token, bank code, batch type, fees account, VAT code, control bank.
10. **FAQ** — Common questions: what if a DD fails, what about refunds, can I cancel a mandate, what does the customer see on their bank statement.

**Includes**: Screenshots from the actual application at each step.

### 4. Email Sequence — Opera Users (4 emails)

Sent to known Opera user contacts. Spaced 4-5 days apart.

**Email 1 — "The collection problem"**
- Subject line: something around the time/effort of chasing payments
- Body: The daily reality — remittances, phone calls, manual posting. There's a better way.
- CTA: "See how Direct Debit changes this" → landing page

**Email 2 — "How it works inside Opera"**
- Subject line: something around seamless Opera integration
- Body: The 4-step process. Screenshot of the app inside Opera. Emphasis on zero manual entry, automatic posting, instant reconciliation.
- CTA: "Watch the 2-minute demo" → demo or landing page

**Email 3 — "The numbers"**
- Subject line: something around concrete time/cost savings
- Body: ROI proof points. "What would your team do with 150 extra hours a year?" Breakdown of manual vs automated time. VAT reclaim angle.
- CTA: "Calculate your savings" or "Get started" → landing page

**Email 4 — "Getting started"**
- Subject line: something around simplicity of getting started
- Body: Three steps to get going. Sign up takes minutes. First collection within a week. Nothing to install, nothing to migrate.
- CTA: "Sign up to GoCardless" → Crakd portal

### 5. Sales Email Templates

**Cold outreach** (for direct sales to Opera users):
- Initial approach email (short, problem-focused)
- Follow-up with one-pager attached
- Demo offer email
- "Already use GoCardless?" variant

**Follow-up templates:**
- Post-demo follow-up
- Post-one-pager follow-up
- Re-engagement (no response after 2 weeks)

### 6. Interactive Demo (refreshed)

Update the existing `gocardless-marketing-demo.html` to align with the campaign narrative.

**Changes from existing:**
- Align slide messaging to the two-layer narrative
- Update proof points to current figures
- Ensure CTAs point to crakd.ai/gocardless
- Add "Already use GoCardless?" messaging
- Review for tone consistency (professional, precise, no hype)

Existing demo already has 12 slides with narration, auto-advance, and speaker notes — this is a refresh, not a rebuild.

### 7. Social Media Content Pack (LinkedIn)

8-10 posts for LinkedIn. Mix of:

- **Stat posts**: Single compelling number + brief context (e.g., time saved, collections automated)
- **Problem/solution posts**: Describe a common pain point, show the automated alternative
- **Feature highlights**: One feature per post with screenshot (mandate setup, payment requests, auto-posting, fee tracking)
- **Demo teasers**: Short clips or screenshots driving to the full demo
- **"Did you know?"**: Direct Debit facts relevant to SME finance teams

**Format**: Text + image (branded card or screenshot). No video required initially.

### 8. ROI Content / Proof Points

Consistent numbers used across all materials:

| Metric | Value | Basis |
|--------|-------|-------|
| Receipts posted automatically per year | 600+ | Based on ~50 collections/month |
| Hours saved per year | 150+ | Manual posting, allocation, reconciliation eliminated |
| Credit control effort reduction | 80% | Collections replace chasing |
| Fee VAT reclaimed | 100% | Automatic tracking, input tax |
| Monthly time: manual vs automated | 12-16 hours → under 30 minutes | End-to-end process comparison |
| Customer mandate signup time | 2 minutes | Online, one-time |
| Time to first collection | Within 1 week of signup | Setup + first mandate + first request |

These figures should be reviewed and adjusted based on real customer data as it becomes available. Until then, they are modelled estimates based on typical Opera user volumes.

---

## Messaging Framework

### Tone

- Professional but not corporate-stiff
- Precise — real numbers, specific steps, exact outcomes
- Confident without being pushy — let the product speak
- Empathetic to the daily reality of finance teams
- No hype, no vague promises — finance people work in exact science

### Messaging Pillars

| Pillar | Layer 1 (DD Concept) | Layer 2 (Crakd.ai Product) |
|--------|----------------------|----------------------------|
| Control | Collect on your terms, not when customers remember | Select invoices, click collect — done |
| Time | Your team spends hours chasing and posting | Zero manual data entry — receipts, allocations, fees, all automatic |
| Cash Flow | Predictable income, fewer bad debts | Money in your bank, posted to your books, before you start the day |
| Precision | Direct Debit — the UK's most trusted payment method | Every receipt allocated to the right invoice, every fee tracked with VAT |
| Simplicity | Customers sign a mandate in 2 minutes | Sign up through our portal, connect in minutes, collect the same week |

### What We Avoid

- Technical jargon in customer-facing materials (no "OAuth", "API", "nominal ledger", "anoml")
- Hype or unsupported claims — everything backed by specifics
- Aggressive sales language — finance people see through it
- Overcomplicating the message — the product is genuinely simple, let that show

---

## Getting Started Flow (as described in materials)

```
1. Sign up to GoCardless (via crakd.ai portal — 5 minutes)
   └─ OR: "I already use GoCardless" → subscribe to Crakd.ai
        
2. Configure in Crakd.ai (one-time setup — 5 minutes)
   └─ API token, bank account, batch type, fees account
        
3. Invite customers to sign mandates (email link — 2 minutes each)
   └─ Customer clicks link, enters bank details, confirms
   └─ Mandate active — ready to collect
        
4. Select invoices due → click Collect
   └─ GoCardless collects via Direct Debit (2-4 working days)
        
5. Payout arrives → one click Import
   └─ Receipts posted, invoices allocated, fees tracked, bank reconciled
   └─ Zero manual entry
```

---

## Phase 2 — Prospects (Future)

Not in scope for this spec, but planned. Will require:

- Landing page variant with Opera de-emphasised ("Crakd.ai — automated finance collection" rather than "GoCardless for Opera")
- Separate email sequence focused on the automation package
- Broader SEO content targeting "automate invoice collection", "direct debit for small business"
- Core assets (demo, how-it-works guide, one-pager) adapted with prospect-friendly language

The core materials from Phase 1 carry forward with minimal rework — the product story is the same, only the framing changes.

---

## Existing Assets to Leverage

These already exist in the project and should be incorporated or refreshed rather than rebuilt:

| Asset | Location | Action |
|-------|----------|--------|
| Marketing demo (12 slides) | `marketing/demos/gocardless-marketing-demo.html` | Refresh messaging to align with campaign |
| Commercial presentation | `marketing/demos/gocardless-commercial-presentation.html` | Review for reuse |
| Opera SE demo | `marketing/demos/gocardless-opera-se-demo.html` | Review for reuse |
| Opera 3 demo | `marketing/demos/gocardless-opera3-demo.html` | Review for reuse |
| UI screenshots (11) | `marketing/demos/gocardless-overview/` | Use in guides and one-pager |
| User manual | `marketing/manuals/manual-gocardless.md` | Source for "how it works" guide content |
| Workflow diagram | `marketing/demos/gocardless-overview/GoCardless-Workflow-Overview.html` | Use on landing page |

---

## Success Metrics

Phase 1 success measured by:

- **Signup rate**: GoCardless registrations via Crakd portal
- **Email engagement**: Open rates, click-through rates on the 4-email sequence
- **Landing page conversion**: Visitors → CTA clicks
- **Demo views**: Interactive demo engagement
- **Sales pipeline**: Conversations initiated from outreach templates
- **Time to first collection**: How quickly new signups make their first DD collection
