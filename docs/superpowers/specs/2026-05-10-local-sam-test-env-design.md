# Design: Local SAM test environment

**Date:** 2026-05-10
**Author:** Harry Burdett (with Claude)
**Status:** Spec — pending implementation

## Goal

Install a standalone local SAM on Harry's Mac, install the four plugins (`bank-reconcile`, `gocardless`, `suppliers`, `balance-check`) via `.sap` upload, validate them end-to-end against live Intsys Opera SE and the live mailbox, iterate until proven, then produce final `.sap` packages for Jonathan to install into Charlie's live SAM. Local SAM persists on Harry's Mac as the permanent dev/test environment for all future plugin work.

## Why

The four plugins currently have no test environment between development and live SAM. Every fix or enhancement would otherwise ship straight to Charlie's production SAM with rollback as the only safety net — operationally risky for a finance application where transactions are real money. Bug fixes typically take several iterations to be correct; doing those iterations in production is unsafe.

Local SAM becomes:
1. **Pre-deployment proof.** The four plugins cross the line to Charlie's Mac only after they've been shown to work end-to-end on Harry's Mac. The first time live SAM ever sees a plugin, it works.
2. **Ongoing staging.** Every future fix and enhancement is validated locally before being promoted to live.
3. **Independent dev platform.** Standalone (no Central), Harry controls what gets installed and when, no auto-sync from anywhere.

See the related project memory and feedback rule:
- `feedback_test_before_live_sam.md` — the principle this project operationalises
- `project_local_sam_test_env.md` — the project record

## Audience

- **Primary:** Harry — runs the local SAM, drives the validation work, owns the test environment going forward
- **Secondary:** Future Claude sessions picking up the work — the spec and resulting docs must be enough context to continue

## Architecture

```
   Harry's Mac (this Mac, /Users/maccb/...)
   ┌─────────────────────────────────────────────┐
   │                                             │
   │   Local SAM (Docker, STANDALONE)            │
   │   ├── nginx                                 │
   │   ├── ai-sam (backend + plugins)            │
   │   ├── db (MSSQL — SAM's own per-app state)  │
   │   └── redis                                 │
   │                                             │
   │   Plugins installed via .sap upload (no     │
   │   GitHub-pull, no Central):                 │
   │   • bank-reconcile                          │
   │   • gocardless                              │
   │   • suppliers                               │
   │   • balance-check                           │
   │                                             │
   │   Connects to LIVE production data:         │
   │   • Intsys Opera SE (read + write)          │ ──► live MSSQL
   │   • intsys@wimbledoncloud.net mailbox       │ ──► live IMAP/Graph
   │                                             │
   └─────────────────────────────────────────────┘
                       │
                       │ when validated, produce final .sap
                       │ files for Jonathan
                       ▼
              ┌────────────────────┐
              │ Charlie's live SAM │
              │ (existing path —   │
              │ DEPLOY-TO-SAM.md)  │
              └────────────────────┘
```

### Key architectural decisions

| Decision | Choice | Why |
|---|---|---|
| SAM mode | Standalone (no SAM Central) | Independence — Harry's local SAM never auto-syncs from anywhere; he controls when/what gets installed |
| Plugin install path | `.sap` upload (not GitHub-pull) | Fast iteration — change code, repackage as `.sap`, re-upload, retest. No GitHub round-trips during development. |
| License config | Empty / local — `LICENSE_SERVER_URL`, `LICENSE_HMAC_SECRET`, `LICENSE_KEY` left blank in `.env` | No phone-home, no Jonathan dependency, no Central account needed |
| Opera connection | Live Intsys Opera SE (read + write) | Real data, real edge cases. Tests duplicate-detection under real conditions. Risk acceptable: Harry controls Opera, can restore from backup; apps have been extensively tested as Python ports already. |
| Mailbox | Live `intsys@wimbledoncloud.net` as-is | Real emails. Legacy Python continues to scan the same inbox in parallel — duplicate-detection in SAM should skip already-posted transactions. If it doesn't, that's a bug worth finding pre-production. |
| Opera 3 | Deferred to a later phase | Needs Pegasus Agent on a Windows machine — separate setup. Not blocking Opera SE validation. |

## What's in scope

1. Standalone local SAM installation on Harry's Mac via `~/opera-knowledge-ref/DEPLOYMENT-GUIDE.md`
2. Wiring local SAM to live Intsys Opera SE
3. Wiring local SAM to live `intsys@wimbledoncloud.net` mailbox
4. Building `.sap` package for each of the four plugins
5. Uploading the four `.sap` files into local SAM via the admin UI
6. End-to-end smoke-testing each plugin against live data
7. Iterating on fixes — repackage + re-upload loop
8. Tagging final verified versions and producing the proven `.sap` files for handoff
9. Updating `apps-sam/MAINTAIN-SAM-PLUGINS.md` Section 1 release flow to include the local-SAM validation step
10. Drafting the email to Jonathan asking him to hold the existing deployment plan

## What's out of scope

- Opera 3 test environment (deferred — needs Pegasus Agent on Windows)
- Automated CI/build pipeline (manual `.sap` builds are fine to start)
- Monitoring / alerting on local SAM (it's a dev env, manual log inspection suffices)
- A staging mailbox or test Opera (we're using live data per Harry's decision)
- Re-doing the install on Charlie's Mac (that's Jonathan's job once we hand off proven artifacts)

## Plan phases (high level — implementation plan will expand)

| Phase | Title | Approx effort |
|---|---|---|
| 1 | Install SAM locally (Docker, standalone mode) | 30 min |
| 2 | Setup wizard — wire to live Opera SE + live mailbox | 30 min |
| 3 | Build `.sap` package for each of the four plugins | 30 min |
| 4 | Upload all four `.sap` files into local SAM via admin UI | 15 min |
| 5 | Smoke-test each plugin end-to-end against live data | 60-90 min |
| 6 | Iterate on any failures — fix in `apps-sam/<plugin>/`, rebuild `.sap`, re-upload, retest | TBD |
| 7 | Once all four pass: tag final versions, produce handoff `.sap` files | 15 min |
| 8 | Update `MAINTAIN-SAM-PLUGINS.md` to include the new local-SAM validation step in the release flow | 15 min |
| 9 | Draft (don't send) the email to Jonathan asking him to hold the existing deployment until we have proven artifacts | 5 min |

**Total active work for initial setup:** ~3-4 hours, comfortably spread over a couple of days.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Local SAM accidentally double-posts to live Opera (if duplicate-detection misses an edge case) | Run side-by-side with legacy Python initially; cross-check Opera transaction count after each test; treat any double-post as a P0 bug to fix before live deployment |
| Local SAM accidentally writes test data to live Opera that's hard to clean up | Harry controls Opera, can restore from backup; use small representative test cases rather than bulk imports during validation |
| Mailbox processing conflicts (local SAM marks email read, legacy Python misses it) | Local SAM should NOT alter mailbox state by default — only read. If `.sap` plugins do alter state, configure them not to during validation. |
| `.sap` install path behaves differently from GitHub-pull (Charlie's path) | Once validated locally, do a final dry-run via the GitHub-pull path before declaring "ready for Jonathan" — if both install paths produce a working plugin, we have high confidence |
| Standalone SAM mode might be poorly documented or have gotchas | Investigation Phase 1 task — confirm standalone mode works fully; if blocked, fall back to asking Jonathan for a test license |
| Opera 3 deferral leaves a gap | Opera 3 is intentionally Phase 2 of the broader project; the SE-only proven artifacts can ship to Charlie first; Opera 3 validation follows when the Pegasus Agent is available |

## Deliverables

1. **Local SAM running on Harry's Mac** with all four plugins installed and verified
2. **Four proven `.sap` files** at known versions (e.g. `v1.0.0`) ready to hand to Jonathan
3. **Updated [apps-sam/MAINTAIN-SAM-PLUGINS.md](apps-sam/MAINTAIN-SAM-PLUGINS.md)** — Section 1 release flow gains a "validate in local SAM" step between bump-version and push-to-release-repo
4. **Drafted email to Jonathan** — text ready for Harry to review and send asking him to hold the existing deployment plan
5. **Updated project memory** — `project_local_sam_test_env.md` marked complete with the actual outcome captured; `feedback_test_before_live_sam.md` updated to reference the now-operational local SAM
6. **Commit and push to `origin/main`** — doc updates and any script additions

## Acceptance criteria

The project is considered done when:

- [ ] Local SAM admin UI loads at the expected URL on Harry's Mac
- [ ] All four plugins appear in the local SAM Admin → Apps list as Installed
- [ ] Each plugin's main UI loads without errors
- [ ] A representative smoke-test passes for each plugin against live Opera SE (specifics in the implementation plan)
- [ ] No double-posts to Opera detected during validation
- [ ] Final `.sap` files exist and are ready for Jonathan
- [ ] Maintenance doc updated to reference local SAM in the release flow
- [ ] Draft email to Jonathan prepared (sending is Harry's call)

## What changes for the future maintenance loop

The current `apps-sam/MAINTAIN-SAM-PLUGINS.md` Section 1 says:

> fix → bump version → push to GitHub → SAM Central → live SAM pulls

Once this project is complete, it becomes:

> fix → bump version → build `.sap` → upload to **local SAM** → test → only if passes → push to GitHub OR hand `.sap` to Jonathan → live SAM installs

The local-SAM step is mandatory per `feedback_test_before_live_sam.md`. No release to live SAM without local validation first.

## Out-of-band: email to Jonathan

The implementation plan will include drafting an email to Jonathan asking him to:
1. **Pause** the deployment we sent him today (don't install the four plugins on Charlie's Mac yet)
2. **Wait** for us to produce proven `.sap` artifacts after local validation
3. **Expect** ~few days timeframe before we're ready

This email is **drafted but not sent** as part of the project deliverables — sending is Harry's call once he reviews.
