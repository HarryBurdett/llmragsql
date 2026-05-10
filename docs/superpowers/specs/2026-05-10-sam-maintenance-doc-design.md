# Design: SAM plugin maintenance documentation

**Date:** 2026-05-10
**Author:** Harry Burdett (with Claude)
**Status:** Spec — pending implementation

## Goal

Produce a single maintenance reference for the four SAM plugins (bank-reconcile, gocardless, suppliers, balance-check), structured by concern (release management / debugging / monitoring / extending), at a level that lets Harry diagnose and ship fixes confidently and lets Jonathan or Charlie pick up the work if Harry is unavailable.

This is the second of the two SAM documents. The first (`apps-sam/DEPLOY-TO-SAM.md`) covers one-time deployment. This one covers everything after.

## Audience

| Reader | When they reach for this doc | What they want |
|---|---|---|
| **Harry** (primary) | "A user reported a bug." / "I want to add a feature." / "Something broke after deploy." | Quick path from situation to fix |
| **Jonathan / Charlie** (secondary) | "Harry is unavailable and something is broken." | Enough understanding to triage, escalate, or apply documented rollbacks |

The tone matches `DEPLOY-TO-SAM.md` (plain English, labelled code blocks, expected-output markers) but assumes the reader knows the codebase — no need to re-explain `git`, `node`, or the project layout.

## What the doc is NOT

- Not first-line user support. ("User says X isn't working" diagnosis is excluded — Harry handles that himself.)
- Not a SAM-platform doc. (Maintaining SAM itself is SAM's own documentation.)
- Not a re-explanation of deployment. (Cross-references `DEPLOY-TO-SAM.md` instead.)

## The new document

### File

- **Path:** `apps-sam/MAINTAIN-SAM-PLUGINS.md`
- **Length target:** ~600 lines
- **Structure:** four big sections by concern + an appendix

### Tone and format conventions

Inherited from `DEPLOY-TO-SAM.md`:

- Plain English, no jargon assumed (except basic git/npm)
- Labelled code fences: `# Terminal — your Mac`, `# Browser — SAM Central`, etc.
- Every command followed by `✓ Looks good if you see:` block
- Common failure modes inline as `✗ If you see X…`
- `⚠` callouts for irreversible / dangerous actions

Differences from DEPLOY-TO-SAM:

- More dense — assumes the reader has read or has access to DEPLOY-TO-SAM for setup details
- Uses tables more aggressively for symptom → cause → fix lookups
- Sections are independent — read order doesn't matter

### Top-level structure

| Section | Title | Lines | What it covers |
|---|---|---|---|
| 0 | Mental model | ~80 | The two repos, the vendor pattern, where to look first when something breaks |
| 1 | Release management | ~200 | Ship a patch, ship a feature, update shared, roll back, hotfix path |
| 2 | Debugging | ~180 | Symptom → cause → fix tables for the most common production failures |
| 3 | Monitoring | ~60 | What to watch in normal operation |
| 4 | Extending | ~120 | Add an endpoint, add a migration, add a shared utility, add a new plugin |
| App | File structure reference | ~40 | Where things live in each plugin |

### Per-section content

#### Section 0 — Mental model
- **The two-repo picture:** SQLRAG monorepo (`apps-sam/<plugin>/`) is source of truth; `intsysuk/sam-*` are release artifacts produced by `extract-all.sh` and consumed by SAM Central.
- **Version semantics:** each plugin's `package.json` + `manifest.json` carries its version. Bump on every release. SAM Central pins versions per client license.
- **The shared library is vendored, not packaged:** `apps-sam/shared/` lives in the monorepo; `extract-all.sh` copies it into each plugin's `src/_shared/` at extraction time. Updating shared = re-extracting + bumping all four plugins.
- **Where to look first when something breaks** (4-step diagnostic flowchart):
  1. One plugin or all four? → narrows to one repo or the shared library
  2. Did it just deploy? → likely the new version; consider rollback
  3. Is it environmental (mailbox, Opera connection)? → SAM Admin, not code
  4. Is the legacy Python equivalent working? → if yes, the SAM port has drifted

#### Section 1 — Release management
- **1.1 The release flow** — 5-line summary + diagram: edit in monorepo → bump version → extract & push → SAM Central registers → SAM host pulls on next sync
- **1.2 Ship a patch (v1.0.x bump)** — single plugin, single bug fix. Full command sequence with labelled blocks.
- **1.3 Ship a minor (v1.x.0)** — same flow with version-bump rules (when to call it 1.1.0 vs 1.0.1)
- **1.4 Update the shared library** — the propagation gotcha: change in shared = touch all 4 plugins
- **1.5 Roll back a broken release** — two options: re-pin previous version in SAM Central (fast), or revert + republish (clean)
- **1.6 Hotfix flow** — "I'm tired, what's the minimum I must do" checklist

#### Section 2 — Debugging
Symptom-led tables. Example:

| Symptom | Most likely cause | Where to look | Fix |
|---|---|---|---|
| Plugin doesn't appear in SAM Admin after sync | Manifest validation failed | `docker logs ai-sam \| grep PluginLoader` | Check `manifest.json` against SAM's plugin contract |
| Plugin loads but endpoints return 500 | Knex query error (often a typo in column name) | Plugin-specific log filter | Cross-check against `scripts/opera_snapshot.json` |
| Plugin's UI loads but data is empty | `ctx.db.opera` not wired or wrong company | `req.operaCompany` middleware | Check Opera connections in SAM Admin |

Plus subsections for:
- ctx inspection (logging what SAM passed in)
- Migration failure recovery
- Mailbox-not-scanning checklist
- Slow queries (the few patterns we've seen and how to spot them)

#### Section 3 — Monitoring (kept brief)
- Logs to watch (`docker logs ai-sam` + plugin-specific filters)
- Health endpoint per plugin (`GET /api/<plugin>/health`)
- Sandbox vs live indicators (GoCardless settings panel shows which mode)
- What "all green" looks like on a normal day

#### Section 4 — Extending
- **4.1 Add an endpoint** — router.ts + service file + test, with file-structure example
- **4.2 Add a migration** — knex migrate workflow, naming convention (incremental number, descriptive suffix)
- **4.3 Add a shared utility** — vendor pattern: put in `apps-sam/shared/src/`; ensure all 4 plugins re-import on next extraction
- **4.4 Add a whole new plugin** — minimal walkthrough: scaffold from existing plugin, manifest.json contract, workspaces array, register in SAM Central

#### Appendix — File structure reference
One-glance map of each plugin: `src/`, `db/`, `tests/`, `manifest.json`, `package.json`, frontend bundle location.

## HTML rendering for the email attachment

For email delivery (see Deliverables below), the `.md` will be rendered to a styled `.html` using `markdown-it-py` (already installed in the venv per `pip list`).

A small build helper script will:
1. Read `apps-sam/MAINTAIN-SAM-PLUGINS.md`
2. Render to HTML via `markdown-it-py`
3. Wrap in a minimal `<style>` block giving it a readable document feel: sans-serif body, monospace for code, light grey code blocks, bordered tables, max-width 800px
4. Save as `apps-sam/MAINTAIN-SAM-PLUGINS.html` (gitignored — regenerated on each email send, never committed)

This keeps the `.md` as the single source of truth and the `.html` as a derived artifact.

## Constraints

- **Markdown is canonical**, HTML is derived. The doc is always edited as `.md`; the `.html` is rebuilt when needed for email delivery and not committed to the repo.
- **Cross-references DEPLOY-TO-SAM.md** rather than duplicating its content (machine labels table, format conventions, etc.).
- **No live GoCardless examples** — per project memory, all examples and screenshots use sandbox values only.
- **Legacy Python is canonical reference** — when debugging, the doc directs the reader to the corresponding legacy Python file as the assumed-correct baseline.

## Out of scope

- Plugin user-facing FAQ (Harry handles user support directly)
- Maintenance of the SAM platform itself
- Performance tuning beyond "the few patterns we've seen"
- Building a CI pipeline (covered separately if needed)
- A doc on maintenance of the legacy Python (the legacy is the canonical reference; if it has bugs we fix them but a doc is YAGNI for now)

## Deliverables

1. **`apps-sam/MAINTAIN-SAM-PLUGINS.md`** — new file, ~600 lines, structured per Section 0-4 + Appendix above.
2. **Update `apps-sam/README.md`** — mention the maintenance doc alongside the deployment doc.
3. **Add `apps-sam/MAINTAIN-SAM-PLUGINS.html` to `.gitignore`** — the derived rendered version is never committed.
4. **Render `.md` → `.html`** via a small helper script using `markdown-it-py`. The script lives at `apps-sam/scripts/render-md-to-html.py` and is committed (so future me can regenerate the HTML easily).
5. **Commit + push to `origin/main`**:
   - One commit for the doc itself
   - One commit for the README update + gitignore entry + render script
6. **Email to `charlieb@intsysuk.com`** via the project's `/api/email/send`:
   - **From:** `intsys@wimbledoncloud.net`
   - **Subject:** `SAM plugin maintenance reference`
   - **Body:** HTML, well-formatted (same conventions as the deployment email — `<h3>` headers, lists, bold, links, inline code)
   - **Cover framing:** "*FYI, this is my reference for maintaining the four SAM plugins — shipping fixes, debugging, rolling back, etc. Primarily my own reference but sharing so you have a copy if I'm unavailable.*"
   - **Attachments:**
     - `apps-sam/MAINTAIN-SAM-PLUGINS.md` (canonical source)
     - `apps-sam/MAINTAIN-SAM-PLUGINS.html` (rendered for pretty offline reading)
   - **Pre-send approval gate:** show Harry the rendered HTML body in chat and confirm both attachments are mentioned, before calling `/api/email/send`.

## Acceptance criteria

- [ ] Doc has 5 sections (0-4) plus appendix
- [ ] Every code block has a `# Terminal — …` or `# Browser — …` label (inherited convention)
- [ ] Section 2 debugging tables follow the symptom → cause → where → fix format
- [ ] Section 1 release management has concrete command sequences, not abstract descriptions
- [ ] Cross-references DEPLOY-TO-SAM.md where appropriate; no duplication of its content
- [ ] `.html` renders cleanly in Chrome/Safari (visual smoke test)
- [ ] Email body and both attachments visible to Charlie after send
- [ ] All committed and pushed to `origin/main`

## What this spec does NOT design

This spec is for the maintenance doc only. A separate ongoing-monitoring tool, alerting rules, or runbook automation are not in scope — those would be separate projects if ever needed.
