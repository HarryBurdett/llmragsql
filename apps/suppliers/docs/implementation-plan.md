# Supplier Statement Automation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a comprehensive bought ledger automation system that monitors supplier emails, extracts statement data via AI, reconciles against Opera PL, generates smart responses, manages queries, and provides aged creditors analysis — all with full audit trail and security.

**Architecture:** FastAPI backend with SQLite for automation state, Opera SQL SE/Opera 3 for PL data, Gemini Vision for PDF extraction. Frontend in React/TypeScript. Existing skeleton in `apps/suppliers/api/routes.py` (34 endpoints) to be extended. All data under `data/{company}/suppliers/`.

**Tech Stack:** Python/FastAPI, SQLite, Opera SQL SE (pyodbc), Opera 3 (FoxPro/DBF), Gemini Vision API, React/TypeScript/TailwindCSS

**Spec:** `apps/suppliers/docs/spec.md`

**Rules:**
- Opera 3 parity on every change
- No hardcoded account codes, company IDs, or supplier-specific values
- Knowledge base updated with any Opera-related learnings
- All business logic in backend — frontend is display only
- Instruction manual created and maintained

---

## File Structure

```
apps/suppliers/
  docs/
    spec.md                          — Full specification
    implementation-plan.md           — This file
    manual-suppliers.md              — User instruction manual (create)
  api/
    routes.py                        — Existing 34 endpoints (extend)
    routes_aged.py                   — Aged creditors endpoints (create)
    routes_remittance.py             — Remittance advice endpoints (create)
    routes_onboarding.py             — Supplier onboarding endpoints (create)
    routes_contacts.py               — Contact management endpoints (create)
  logic/                             — (future: move sql_rag files here)

sql_rag/
  supplier_statement_db.py           — Existing DB class (extend schema)
  supplier_statement_extract.py      — Existing AI extraction (extend)
  supplier_statement_reconcile.py    — Existing reconciler (extend with business rules)

frontend/src/pages/
  SupplierDashboard.tsx              — Main dashboard (create)
  SupplierStatementQueue.tsx         — Statement processing queue (create)
  SupplierReconciliation.tsx         — Statement reconciliation review (create)
  SupplierQueries.tsx                — Query management (create)
  SupplierCommunications.tsx         — Communications log (create)
  SupplierDirectory.tsx              — Supplier directory with health (create)
  SupplierSecurity.tsx               — Security alerts (create)
  SupplierSettings.tsx               — Settings page (create)
  SupplierAgedCreditors.tsx          — Aged creditors analysis (create)
  SupplierOnboarding.tsx             — New supplier checklist (create)
  SupplierContacts.tsx               — Contact management (create)
```

---

## Phase 1: Foundation & Data Model

### Task 1.1: Extend Database Schema

**Files:**
- Modify: `sql_rag/supplier_statement_db.py`

- [ ] **Step 1:** Read existing schema in `supplier_statement_db.py` and understand current tables
- [ ] **Step 2:** Add `supplier_onboarding` table to `_init_db()`
- [ ] **Step 3:** Add `supplier_contacts_ext` table to `_init_db()`
- [ ] **Step 4:** Add `supplier_remittance_log` table to `_init_db()`
- [ ] **Step 5:** Add methods: `get_onboarding_status()`, `update_onboarding()`, `get_contacts()`, `upsert_contact()`, `log_remittance()`
- [ ] **Step 6:** Verify DB initialises cleanly: restart server, check `data/intsys/suppliers/supplier_statements.db` has new tables
- [ ] **Step 7:** Commit

### Task 1.2: Contact Management API

**Files:**
- Create: `apps/suppliers/api/routes_contacts.py`
- Modify: `api/main.py` (register router)

- [ ] **Step 1:** Create `routes_contacts.py` with APIRouter
- [ ] **Step 2:** Add `GET /api/supplier-contacts/{account}` — reads from Opera `zcontacts` + local extensions
- [ ] **Step 3:** Add `POST /api/supplier-contacts/{account}` — create/update local contact extension
- [ ] **Step 4:** Add `DELETE /api/supplier-contacts/{account}/{contact_id}` — remove local extension
- [ ] **Step 5:** Add `PUT /api/supplier-contacts/{account}/{contact_id}/roles` — set statement/payment/query contact flags
- [ ] **Step 6:** Register router in `api/main.py`
- [ ] **Step 7:** Test endpoints via curl
- [ ] **Step 8:** Commit

### Task 1.3: Supplier Onboarding API

**Files:**
- Create: `apps/suppliers/api/routes_onboarding.py`
- Modify: `api/main.py` (register router)

- [ ] **Step 1:** Create `routes_onboarding.py` with APIRouter
- [ ] **Step 2:** Add `GET /api/supplier-onboarding/pending` — suppliers without completed onboarding
- [ ] **Step 3:** Add `GET /api/supplier-onboarding/{account}` — onboarding status for one supplier
- [ ] **Step 4:** Add `POST /api/supplier-onboarding/{account}/verify-bank` — mark bank details verified
- [ ] **Step 5:** Add `POST /api/supplier-onboarding/{account}/complete` — mark onboarding complete
- [ ] **Step 6:** Add `POST /api/supplier-onboarding/detect-new` — scan Opera `pname` for new suppliers not in onboarding table
- [ ] **Step 7:** Register router in `api/main.py`
- [ ] **Step 8:** Test endpoints via curl
- [ ] **Step 9:** Commit

---

## Phase 2: Statement Processing Enhancement

### Task 2.1: Enhance Reconciliation Business Rules

**Files:**
- Modify: `sql_rag/supplier_statement_reconcile.py`

- [ ] **Step 1:** Read existing `SupplierStatementReconciler` class
- [ ] **Step 2:** Add `_apply_business_rules()` method implementing the "only query when not in our favour" logic from spec section 5
- [ ] **Step 3:** Add `_classify_discrepancy()` — determines: auto_query, stay_quiet, always_notify, flag_for_review
- [ ] **Step 4:** Add `_calculate_payment_forecast()` — reads `pterm`/`pname` payment terms, calculates due dates
- [ ] **Step 5:** Add `generate_response()` — creates the structured response from spec section 7
- [ ] **Step 6:** Test reconciliation with a real supplier account via curl to existing `/api/supplier-statements/reconcile/{email_id}`
- [ ] **Step 7:** Commit

### Task 2.2: Enhance Statement Extraction

**Files:**
- Modify: `sql_rag/supplier_statement_extract.py`

- [ ] **Step 1:** Read existing extraction code
- [ ] **Step 2:** Add cross-reference verification: match extracted supplier name/reference against Opera `pname`
- [ ] **Step 3:** Add balance chain validation (same pattern as bank statement extraction)
- [ ] **Step 4:** Add extraction caching (avoid re-extracting same PDF)
- [ ] **Step 5:** Test with a real supplier statement PDF
- [ ] **Step 6:** Commit

### Task 2.3: Auto-Acknowledgment

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Add `POST /api/supplier-statements/{statement_id}/acknowledge` — sends acknowledgment email using template from settings
- [ ] **Step 2:** Add acknowledgment template to settings schema (default from spec section 2)
- [ ] **Step 3:** Add parameterised delay support (immediate or deferred)
- [ ] **Step 4:** Record acknowledgment in communications audit trail
- [ ] **Step 5:** Test via curl
- [ ] **Step 6:** Commit

---

## Phase 3: Communications & Workflow

### Task 3.1: Response Generation & Approval Queue

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Enhance existing `POST /api/supplier-statements/{statement_id}/process` to generate structured response using reconciler's `generate_response()`
- [ ] **Step 2:** Enhance existing `POST /api/supplier-statements/{statement_id}/approve` to send the response email and record in audit trail
- [ ] **Step 3:** Add `PUT /api/supplier-statements/{statement_id}/edit-response` — allow editing response before approval
- [ ] **Step 4:** Add `POST /api/supplier-statements/queue/bulk-approve` — approve and send multiple responses
- [ ] **Step 5:** Test full workflow: extract → reconcile → generate response → edit → approve → send
- [ ] **Step 6:** Commit

### Task 3.2: Remittance Advice

**Files:**
- Create: `apps/suppliers/api/routes_remittance.py`
- Modify: `api/main.py` (register router)

- [ ] **Step 1:** Create `routes_remittance.py` with APIRouter
- [ ] **Step 2:** Add `GET /api/supplier-remittance/recent-payments` — list recent payments from `ptran` with allocation detail from `palloc`
- [ ] **Step 3:** Add `POST /api/supplier-remittance/{account}/generate` — generate remittance advice for a payment
- [ ] **Step 4:** Add `POST /api/supplier-remittance/{account}/send` — send remittance via email
- [ ] **Step 5:** Add `GET /api/supplier-remittance/history` — sent remittance log
- [ ] **Step 6:** Add remittance settings to supplier settings (auto_send, format, cc)
- [ ] **Step 7:** Register router, test via curl
- [ ] **Step 8:** Commit

### Task 3.3: Query Management Enhancement

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Enhance existing query endpoints with follow-up reminder logic
- [ ] **Step 2:** Add `POST /api/supplier-queries/{query_id}/send-reminder` — sends follow-up email
- [ ] **Step 3:** Add `GET /api/supplier-queries/overdue` — queries past response deadline
- [ ] **Step 4:** Add auto-follow-up check (configurable days from settings)
- [ ] **Step 5:** Test via curl
- [ ] **Step 6:** Commit

---

## Phase 4: Security & Monitoring

### Task 4.1: Supplier Record Change Monitoring

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Add `POST /api/supplier-security/scan-changes` — compare current `pname` sensitive fields against last known values, log changes
- [ ] **Step 2:** Define monitored fields: bank account (`pn_bank`, `pn_acno`, `pn_sort`), payment method, remittance email
- [ ] **Step 3:** Add `GET /api/supplier-security/alerts` enhancement — include unverified bank detail changes
- [ ] **Step 4:** Add `POST /api/supplier-security/alerts/{alert_id}/verify` enhancement — phone verification workflow
- [ ] **Step 5:** Add email alerting for security-flagged changes (configurable recipients)
- [ ] **Step 6:** Test via curl
- [ ] **Step 7:** Commit

### Task 4.2: Email Security Rules

**Files:**
- Modify: `sql_rag/supplier_statement_reconcile.py`
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Add content scanning: detect bank detail mentions in incoming supplier emails
- [ ] **Step 2:** If bank details mentioned: block auto-processing, flag for manual review
- [ ] **Step 3:** Add `GET /api/supplier-security/email-flags` — emails flagged for bank detail content
- [ ] **Step 4:** Ensure auto-generated responses NEVER include or request bank details
- [ ] **Step 5:** Test via curl
- [ ] **Step 6:** Commit

---

## Phase 5: Aged Creditors & Reporting

### Task 5.1: Aged Creditors API

**Files:**
- Create: `apps/suppliers/api/routes_aged.py`
- Modify: `api/main.py` (register router)

- [ ] **Step 1:** Create `routes_aged.py` with APIRouter
- [ ] **Step 2:** Add `GET /api/creditors/aged` — aging analysis: current/30/60/90/120+ buckets from `ptran` where `pt_trbal != 0`
- [ ] **Step 3:** Add `GET /api/creditors/aged/{account}` — per-supplier aging drill-down with invoice detail
- [ ] **Step 4:** Add `GET /api/creditors/aged/trend` — month-on-month aging trend (last 6 months)
- [ ] **Step 5:** Add Opera 3 equivalents for all endpoints
- [ ] **Step 6:** Register router, test via curl
- [ ] **Step 7:** Commit

### Task 5.2: Enhanced Dashboard KPIs

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1:** Enhance existing `/api/supplier-statements/dashboard` with aged creditors summary
- [ ] **Step 2:** Add trend indicators (improving/worsening aged debt)
- [ ] **Step 3:** Add supplier health scoring (based on response times, query count, aging)
- [ ] **Step 4:** Add procurement integration status (if configured)
- [ ] **Step 5:** Test via curl
- [ ] **Step 6:** Commit

---

## Phase 6: Frontend

### Task 6.1: Supplier Dashboard

**Files:**
- Create: `frontend/src/pages/SupplierDashboard.tsx`
- Modify: `frontend/src/App.tsx` (add route)
- Modify: `frontend/src/components/Layout.tsx` (update menu)

- [ ] **Step 1:** Create dashboard page with KPI cards (statements received, pending approvals, open queries, overdue)
- [ ] **Step 2:** Add alerts section (security alerts, overdue queries, failed processing)
- [ ] **Step 3:** Add recent activity feed
- [ ] **Step 4:** Add aged creditors summary chart
- [ ] **Step 5:** Wire up to API endpoints
- [ ] **Step 6:** Add route and update Suppliers menu
- [ ] **Step 7:** Build and test in browser
- [ ] **Step 8:** Commit

### Task 6.2: Statement Queue & Reconciliation

**Files:**
- Create: `frontend/src/pages/SupplierStatementQueue.tsx`
- Create: `frontend/src/pages/SupplierReconciliation.tsx`

- [ ] **Step 1:** Create queue page — list of incoming statements with status, filter, sort
- [ ] **Step 2:** Create reconciliation review — side-by-side statement vs Opera, match highlighting
- [ ] **Step 3:** Add response preview and edit-before-send
- [ ] **Step 4:** Add approve/hold/reject actions
- [ ] **Step 5:** Add bulk approve
- [ ] **Step 6:** Wire up to API, add routes
- [ ] **Step 7:** Build and test
- [ ] **Step 8:** Commit

### Task 6.3: Query Management

**Files:**
- Create: `frontend/src/pages/SupplierQueries.tsx`

- [ ] **Step 1:** Create queries page — grouped by supplier, aging indicator, query type
- [ ] **Step 2:** Add tabs: Open, Overdue, Resolved
- [ ] **Step 3:** Add actions: resolve, send reminder
- [ ] **Step 4:** Wire up to API, add route
- [ ] **Step 5:** Build and test
- [ ] **Step 6:** Commit

### Task 6.4: Communications, Directory, Security

**Files:**
- Create: `frontend/src/pages/SupplierCommunications.tsx`
- Create: `frontend/src/pages/SupplierDirectory.tsx`
- Create: `frontend/src/pages/SupplierSecurity.tsx`

- [ ] **Step 1:** Create communications log — full history per supplier, filter, search
- [ ] **Step 2:** Create supplier directory — all suppliers with health indicators, approved senders
- [ ] **Step 3:** Create security page — change alerts, verification workflow, audit log
- [ ] **Step 4:** Wire up to APIs, add routes
- [ ] **Step 5:** Build and test
- [ ] **Step 6:** Commit

### Task 6.5: Settings, Aged Creditors, Contacts, Onboarding

**Files:**
- Create: `frontend/src/pages/SupplierSettings.tsx`
- Create: `frontend/src/pages/SupplierAgedCreditors.tsx`
- Create: `frontend/src/pages/SupplierContacts.tsx`
- Create: `frontend/src/pages/SupplierOnboarding.tsx`

- [ ] **Step 1:** Create settings page — timing params, thresholds, alert recipients, email templates, per-supplier overrides
- [ ] **Step 2:** Create aged creditors page — aging buckets, drill-down, trend chart
- [ ] **Step 3:** Create contacts page — read from Opera zcontacts + local extensions, role assignment
- [ ] **Step 4:** Create onboarding page — new supplier checklist, verification status
- [ ] **Step 5:** Wire up to APIs, add routes, update menu
- [ ] **Step 6:** Build and test all pages
- [ ] **Step 7:** Commit

---

## Phase 7: Documentation & Final Verification

### Task 7.1: Instruction Manual

**Files:**
- Create: `marketing/manuals/manual-suppliers.md`

- [ ] **Step 1:** Write user instruction manual covering full workflow
- [ ] **Step 2:** Include screenshots for key screens
- [ ] **Step 3:** Add to CLAUDE.md manual references
- [ ] **Step 4:** Commit

### Task 7.2: Knowledge Base Update

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1:** Add supplier module section covering ptran queries, zcontacts access, pterm payment terms
- [ ] **Step 2:** Document any new Opera fields/conventions discovered during implementation
- [ ] **Step 3:** Commit

### Task 7.3: End-to-End Verification

- [ ] **Step 1:** Test full statement processing workflow (email → extract → reconcile → respond → approve → send)
- [ ] **Step 2:** Test multi-company (switch company, verify isolation)
- [ ] **Step 3:** Test all frontend pages load and function
- [ ] **Step 4:** Test Opera 3 parity on all endpoints
- [ ] **Step 5:** Verify no regressions on existing bank reconcile / GoCardless functionality
- [ ] **Step 6:** Final commit and push
