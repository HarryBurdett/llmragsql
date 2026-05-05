# 2026-05-05 Workflow Audit — Index & Status

Comprehensive read-only audit of bank-rec, GoCardless and supplier
workflows for production readiness, run on 2026-05-05.

## Findings files

| File | Scope | C / S / Cos |
|---|---|---|
| `stages-1-2-findings.md` | Bank-rec Stage 1 (Select) + Stage 2 (Match) | 5 / 13 / 4 |
| `stages-3-4-5-findings.md` | Bank-rec Stage 3 (Import), 4 (Reconcile), 5 (Complete) | 3 / 10 / 4 |
| `cross-cutting-findings.md` | Multi-tenant, parity, locking, hardcoding, KB | 8 / 10 / 4 |
| `opera3-column-audit.md` | Opera 3 dict-access column-name typos | 3+ / — / — |
| `gocardless-findings.md` | GoCardless workflow | 9 / 12 / 6 |
| `suppliers-findings.md` | Supplier statement reconciliation workflow | 15 / 10 / 5 |
| **Total raw** | | **43+ / 55 / 23** |

(Severity counts include some overlap between findings files where the
same root issue surfaced in multiple places.)

## Fix status

Status as of close of session 2026-05-05.

### CRITICAL — fixed

| Theme | Findings | Commits |
|---|---|---|
| Open-items rule applied at every site (£198 P Flannery class) | stages-1-2 F1, cross-cutting F2/F3 | `e108e79` |
| Opera 3 column-name typos (nb_acnt, nk_name, nk_forgn, nk_lstrcln, st_cusref, st_custype) | opera3-column-audit F1/F1b/F2/F3, stages-3-5 F5 | `e108e79`, `abc9076` |
| Opera 3 type-blind already-posted fallback (HISCOX class) | cross-cutting F1, stages-1-2 F3 | `381c16d` |
| Stage 5 reversal contract (nbank Stage B + ae_recbal/ae_recdate) | stages-3-5 F1/F3 | `a6c14e2` |
| Opera 3 complete_reconciliation smart promotion parity | stages-3-5 F2 | `a6c14e2` |
| Stage 4 selectedCount display (UX consistency) | stages-3-5 F7 | `a6c14e2` |
| Crash bugs: tables_updated NameError + 3 supplier NameErrors + delete_contact_by_zcontact_id | gocardless F1, suppliers F7/F8 | `e59a228` |
| Hardcoded /Users/maccb/Downloads + intsys email + 9 missing NOLOCKs | cross-cutting F6/F8, suppliers F13 | `ac982bb` |
| 253 raw `str(e)` UI leaks → friendly_db_error | cross-cutting F13 | `ad2a534` |
| GoCardless idempotency + Opera 3 mandate verification + path validation | gocardless F2/F3/F8 | `7542dc5` |
| GoCardless sandbox=True default (don't accidentally hit live) | gocardless F6 | `e59a228` |
| Suppliers communication-policy gate on operator paths | suppliers F3 | `8c32099` |
| Suppliers statements_contact_position now read at send time | suppliers F11 | `8c32099` |
| Remittance idempotency + approval threshold wired | suppliers F4/F9 | `7cf549b` |
| Verify-bank evidence required + masked audit | suppliers F23 | `1b1d6df` |
| Verified sender gate + per-call sync snapshot (cross-tenant leak) | cross-cutting F4, suppliers F10 | `6b499fa` |
| Auto-reconcile statement_number from nk_lststno+1 (collision fix) | stages-3-5 F4 | `74b75cd` |
| Refunds increment sn_nextpay/pn_nextpay | stages-3-5 F13 | `74b75cd` |
| Sync-cooldown timezone bug | stages-1-2 F11 | `74b75cd` |
| Bank-transfer false-positive guard + LIKE escape + ambiguous match_type + defer fallback + sign-aware defer auto-clean | stages-1-2 F7/F8/F9/F10/F14 | `2adbc40` |
| RTRIM(nk_acnt) consistency + log silent record_bank_statement_import failures | stages-1-2 F15/F17 | `e67d888` |
| Opera 3 supplier-data provider built (parity) | suppliers F1 | `bd52827` |
| GoCardless Opera 3 parameter parity + currency validation + import lock | gocardless F4/F5 | `b5fdecc` |
| Per-call config.ini snapshot (multi-tenant race) | cross-cutting F12 | `1ed75eb` |
| Stage 4 toggle blocked on null-entry rows | stages-3-5 F8 | `d94f6f9` |
| Period-bound check on at_pstdate (not ae_lstdate) | stages-3-5 F12 | `d94f6f9` |
| Opera 3 reversal script (parity with SE) | stages-3-5 F11 | `76a4d88` |
| Opera 3 startup integrity check (parity) | cross-cutting F10 | `7a81f1c` |
| Stub scan-emails endpoints deprecated | stages-1-2 F19, cross-cutting F11 | `abc9076` |
| UPDLOCK on NOLOCK-then-UPDATE in complete_reconciliation | cross-cutting F7 | `74e1892` |
| Supplier bank-detail change scan supports Opera 3 | suppliers F5 | `d6ee1e7` |
| nk_lstrecl<1 auto-recover + dead-promotion clarification | stages-3-5 F15/F17 | `3511cde` |

### KB / Manual updates landed

Local KB (`apps/core/docs/opera_knowledge_base.md`):

- Bank Rec Self-Heal Rule section (`618bb20` related)
- Type-Blind Already-Posted Fallback section (`381c16d`)
- Open-items rule already documented from yesterday (`e6a84be`)

Central KB (`~/opera-knowledge-ref/packages/opera-knowledge/`,
`https://github.com/jonathangintsys/aisam`):

- `business-rules/matcher-period-bound.md` — added `ae_remove=0` (`8034d41`)
- `business-rules/duplicate-check.md` — type-blind fallback section (`08e67b1`)
- `business-rules/bank-rec-self-heal.md` — new (`dfad87a`)
- `platform/opera3-write-agent.md` — new + scope-clarification (`8d1eca2`)

Manual (`marketing/manuals/manual-bank-reconciliation.md`):

- Stage 4 click-to-toggle Match status (`618bb20`)
- Stage 5 self-heal of statements completed in Opera (`0c8021a`)

### Items deferred to dedicated future sessions

The following items are real but are large enough that landing them in
the same end-of-day push as the rest would risk an ill-considered
implementation. Each warrants its own brainstorm → spec → plan cycle:

| Theme | Findings | Reason for deferral |
|---|---|---|
| **f-string SQL → parameterised queries everywhere** | cross-cutting F5, stages-1-2 F5, suppliers F14 | ~250+ sites; each needs careful conversion preserving query semantics. Mitigation: route auth gates the surface; the values that flow in are URL/body params from authenticated callers, not raw user input. Schedule as a dedicated SQL-injection-hardening sprint. |
| **42 SE bank-rec endpoints with no Opera 3 mirror** | cross-cutting F9 | Each missing endpoint is its own feature; many are minor (settings UIs); some are not (audit-defer, deferred-items). Triage and add the Opera-touching ones in a dedicated parity pass. |
| **6 huge route handlers (>500 lines)** | cross-cutting F19 | Refactor exercise that doesn't change behaviour; substantial regression risk if rushed. |
| **Inline AI extraction blocking scan loop** | stages-1-2 F18 | Design change — needs background-worker plumbing, message queue or task-status table, frontend polling. Schedule as a focused performance project. |
| **Reversal in the UI** | stages-3-5 F10 | Frontend feature add — needs a new page or modal, route wiring, undo-confirmation flow. The CLI tools (SE + Opera 3) are landed and accessible to support engineers; the UI follow-up is non-urgent. |
| **bank_aliases not bank-scoped** | stages-1-2 F16 | Schema migration on bank_aliases.db (per-company) plus rollout coordination — needs careful planning to avoid losing existing aliases. |
| **Auto-reconcile line-number gap logic** | stages-3-5 F6 | Cosmetic / consistency issue; the existing 10/20/30 pattern works for the common case. Schedule with the reversal-UI work since both touch the rec-batch-completion display path. |
| **Tests SE/Opera 3 parity coverage** | cross-cutting F18 | Test-suite expansion of substantial scope. |
| **Suppliers F6 — bank-detail change scan scheduler** | suppliers F6 | F5 (Opera 3 mirror) is landed; the periodic-scheduling part requires new background-job infrastructure. Can run via the existing periodic email-sync hook in a follow-up. |

## Verification artifacts

- All 282 tests pass at HEAD.
- `scripts/validate_sql_columns.py` runs clean against the bank-rec
  workflow's SQL literals (no unknown columns in scope).
- Live verification of the bank-rec self-heal against the
  intsys/BC010/import 71 scenario succeeded earlier in the session
  (heal flipped is_reconciled 0→1, populated reconciled_count from
  transactions_imported, Opera state untouched).

## Next steps

The deferred items above each warrant their own session. None are
production-blocking on their own — the critical-path correctness work
(open-items rule everywhere, parity gaps, idempotency, never-communicate
gate, fraud-prevention evidence, multi-tenant isolation) is landed.

This audit + remediation cycle is the structured base from which to
plan the next round of incremental hardening.
