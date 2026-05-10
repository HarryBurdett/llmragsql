# Design: SAM deployment documentation

**Date:** 2026-05-10
**Author:** Harry Burdett (with Claude)
**Status:** Spec — pending implementation

## Goal

Replace the three existing SAM handoff documents with **one unified deployment guide** that walks the SAM team (Jonathan + Charlie) end-to-end through installing the four rewritten plugins (bank-reconcile, gocardless, suppliers, balance-check) into a running SAM instance — at a level of clarity that lets anyone follow it regardless of prior familiarity with the stack.

A second document covering **ongoing code maintenance** follows once the deployment guide is shipped. This spec covers the deployment guide only; the maintenance guide gets its own brainstorming pass.

## What's wrong with the current state

Three overlapping docs exist in `apps-sam/`:

| Doc | Role | Problem |
|---|---|---|
| `EMBEDDING.md` | Architecture rationale + extraction guide | Mixes "why we vendor shared" theory with "run this script" instructions; assumes operator familiarity. Contains pre-flight code-change instructions that are no longer needed (the rewires are already on main). |
| `OPERATOR-SETUP.md` | Phased checklist | Right shape, wrong depth — assumes you know what `gh repo create` does, doesn't tell you what success looks like at each step. |
| `MIGRATION.md` | Data migration runbook | Standalone runbook; the operator has to mentally splice it into the right point in OPERATOR-SETUP.md. |

These three will be **deleted** in the same commit as the new doc.

## The new document

### File
- **Path:** `apps-sam/DEPLOY-TO-SAM.md`
- **Length:** ~600 lines (one ~30-min read end-to-end; ~90 min of actual execution time)
- **Audience:** Jonathan + Charlie + any future SAM-team member who joins after this

### Tone and clarity bar
- Plain English. No jargon assumed.
- Same level of clarity throughout — no "as you know" moments.
- Every command tells the reader **which machine** it runs on.
- Every command shows **what success looks like** (expected output).
- Common failure modes are inline next to the command that produces them — no separate troubleshooting hunt.

### Format conventions

**Labelled code fences.** Every code block has a top-line label naming the machine and environment:

```
# Terminal — your Mac
```
```
# Terminal — SAM Mac
```
```
# Browser — GitHub
```
```
# Browser — SAM Central
```

**Expected-output markers.** Every command is followed by:

> **✓ Looks good if you see:**
> ```
> ...exact expected output...
> ```

**Inline failure modes.** Common errors appear next to the command that produces them, prefixed `✗`:

> **✗ If you see `remote: Repository not found`** — …explanation + fix…

**Bold + boxed warnings.** Irreversible or destructive steps get `⚠` callouts (e.g. *⚠ Do not use the live GoCardless token until sandbox smoke-test passes*).

**Repeat-for-each-plugin blocks.** Where the same step runs for all four plugins, the block ends with: *Repeat for the other three: `bank-reconcile`, `gocardless`, `suppliers`.* — so the reader doesn't have to mentally generalise.

**No screenshots in v1.** SAM Central UI steps describe the screen in words (e.g. *"In SAM Central, click **Apps** in the left nav, then **+ Add app**…"*). Screenshots can be added later by someone with SAM Central access.

### Structure

The document has 8 phases plus a final Done block and a Troubleshooting appendix.

| Phase | Title | Who | Est. time |
|---|---|---|---|
| 0 | Before you start | Read together | 5 min |
| 1 | Extract the four plugins from the monorepo | Jonathan | 10 min |
| 2 | Create GitHub repos and push the code | Jonathan | 15 min |
| 3 | Register the four apps in SAM Central | Jonathan | 15 min |
| 4 | Trigger install on the SAM host | Charlie | 5 min |
| 5 | Configure each plugin | Charlie | 20 min |
| 6 | Migrate existing data from Python apps | Charlie | 15 min |
| 7 | Verify everything works | Both | 15 min |
| ∎ | Done — hand back to Harry | — | — |

### Phase content map

#### Phase 0 — Before you start
- **The two machines you'll be using** — explicit table mapping each `# Terminal — …` label to a physical machine, who uses it, and what's installed there. The first time a reader encounters labelled code blocks, they understand exactly what they mean.
- **Accounts and access you need** — GitHub `intsysuk` write access; SAM Central admin login; SAM Mac SSH/physical access; sandbox GoCardless API token.
- **Software each machine needs** — with one-line check commands (`git --version`, `node --version`, `gh --version`).
- **What you're deploying** — one-paragraph plain-English summary of each plugin.
- **What "done" looks like** — the end-state we're driving to, so the team knows when they've succeeded.

#### Phase 1 — Extract the four plugins (Jonathan, ~10 min)
- Clone the SQLRAG monorepo (or pull latest if already cloned).
- Run `./apps-sam/scripts/extract-all.sh`.
- One paragraph explaining what the script does (copies each plugin's folder, vendors `shared/` into each, runs each plugin's tests, runs each plugin's build) so the reader knows it's not a black box.
- Verify four staged repo folders exist at `~/sam-plugins-staging/sam-*`.
- Expected: 4 ✓ lines printed, each with `tests passed`, `lint clean`, `build ok`.

#### Phase 2 — Create GitHub repos and push (Jonathan, ~15 min)
- Create 4 empty private repos under `intsysuk/sam-*` (don't initialise — no README, no .gitignore).
- For each plugin, `cd ~/sam-plugins-staging/sam-<plugin>` and `./push-to-github.sh <plugin>`.
- Confirm `v1.0.0` tag appears on each repo's GitHub page.
- Confirm all four repo URLs are reachable.

#### Phase 3 — Register in SAM Central (Jonathan, ~15 min)
- Open SAM Central → Apps catalogue → + Add app, for each plugin set `app_id`, `git_url`, `default_version: v1.0.0`.
- Assign `v1.0.0` to the IntSys client license.
- Confirm Central's GitHub PAT has read access to the four new repos (test pull from within Central).
- **Note in the doc:** this phase needs verification against SAM Central's actual UI. The doc is written from OPERATOR-SETUP.md / EMAIL-DRAFT.md; Jonathan may need to add screen-specific detail on his first run-through. Capture any UI-name differences as a follow-up doc update.

#### Phase 4 — Install on the SAM host (Charlie, ~5 min)
- In SAM Admin → Apps → Sync now.
- On the SAM Mac, watch `docker logs -f ai-sam | grep -E "GitInstall|PluginLoader"`.
- Expect `[GitInstall] Success` and `[PluginLoader] Loaded` lines for each of the four plugins.
- Inline failure modes: PAT permissions, network, repo visibility, plugin manifest validation errors.

#### Phase 5 — Configure each plugin (Charlie, ~20 min)
- Confirm Opera SE connections in SAM Admin (Intsys + CloudSiS should appear automatically, auto-discovered from `seqco`).
- Confirm Microsoft Graph email is active.
- Assign mailbox `owner_app_id`:
  - bank-statements mailbox → `bank-reconcile`
  - GoCardless payouts mailbox → `gocardless`
  - supplier-statements mailbox → `suppliers`
- Per-plugin settings:
  - **bank-reconcile** — base folder path (optional, only if you watch a local folder for PDFs)
  - **gocardless** — environment: `sandbox`; paste sandbox API token + company reference. ⚠ Do NOT use live token until sandbox smoke-test passes.
  - **suppliers** — defaults are fine to start
  - **balance-check** — nothing to configure

#### Phase 6 — Migrate existing data (Charlie, ~15 min)
- Mount or copy the Python `data/<company>/` folder so the SAM Mac can read it.
- For each company (`intsys`, `cloudsis`) and each plugin: dry-run first, then real run.
- Inline expected row counts (161 aliases, 95 patterns, 74 mandates, etc.) so the reader can sanity-check the migration without leaving the doc.
- Confirm migration log shows zero errors and inserted-row counts match expectations.

#### Phase 7 — Verify everything works (Both, ~15 min)
- Per-plugin smoke test:
  - **balance-check** — open the page, confirm a customer balance loads.
  - **bank-reconcile** — trigger a mailbox scan, confirm 1 statement appears.
  - **gocardless** — trigger a mailbox scan, confirm sandbox token authenticates (test API call).
  - **suppliers** — open the supplier dashboard, confirm a migrated statement appears.
- Sanity check: confirm legacy Python apps still work in parallel (nothing in the SAM merge should have broken them).

#### ∎ Done — hand back to Harry
- One paragraph: "Here's what to tell Harry" — list of what's now running, what's still on sandbox, what's still to do.
- Deferred follow-ups list:
  - Swap GoCardless sandbox → live token after smoke-test period
  - Retire the legacy Python frontend menu items once SAM apps are confirmed stable (legacy backend stays per `feedback_legacy_python_reference.md`)
  - Any UI-detail corrections to this doc captured during the first run-through

### Troubleshooting appendix

A short reference at the bottom of the doc — known failure modes paired with cause + fix. Initial entries:

1. `extract-all.sh` aborts on tests fail — likely npm cache; clear and rerun
2. `push-to-github.sh` permission denied — SSH key vs PAT
3. SAM Central can't pull the repo — PAT scope
4. `[PluginLoader]` errors — manifest validation; specific patterns to look for
5. Mailbox assignment doesn't trigger scan — the email-ingest adapter checks every 60 seconds
6. Sandbox GoCardless token rejected — token format or environment mismatch
7. Migration dry-run shows 0 rows — `--data-root` pointing at wrong folder
8. Migration real-run partial success — re-runnable: it's idempotent on `source_ref`

## Constraints

- **No screenshots in v1.** The author of this doc doesn't have access to SAM Central's UI to capture them.
- **Phase 3 needs SAM-side verification.** Field names and screen labels are taken from existing handoff docs and may not match the live UI exactly. Marked as such in the doc.
- **Legacy Python is retained.** Per `feedback_legacy_python_reference.md`, the legacy code under `apps/`, `sql_rag/`, `frontend/src/pages/` is the canonical behavioural reference and stays forever. The doc does NOT instruct anyone to remove or retire it.
- **No real GoCardless API calls.** Per project memory: `GoCardless: DO NOT make live API requests` — the doc enforces sandbox-first.

## Out of scope (deferred to the maintenance guide)

- How to ship a v1.0.1, v1.1.0 update of any plugin
- How to debug a production issue (logs, ctx inspection, common Knex query failures)
- How to roll a plugin back if a release breaks
- How to update the vendored `shared/` library across plugins
- How to add a new plugin under the same pattern

These belong in the second document.

## Deliverables

1. `apps-sam/DEPLOY-TO-SAM.md` — new unified deployment guide (this spec).
2. Deletion of `apps-sam/EMBEDDING.md`, `apps-sam/OPERATOR-SETUP.md`, `apps-sam/MIGRATION.md` in the same commit.
3. Update `apps-sam/EMAIL-DRAFT.md` to point at `DEPLOY-TO-SAM.md` instead of the three old docs.
4. Update `apps-sam/README.md` to point to `DEPLOY-TO-SAM.md` for deployment.
5. Commit message: `docs: replace 3 SAM handoff docs with single unified deployment guide`.

## Acceptance criteria

The doc is considered done when:

- [ ] Every code block has a `# Terminal — …` or `# Browser — …` label
- [ ] Every command is followed by a `✓ Looks good if you see:` block
- [ ] Each phase opens with a one-sentence "what you're doing here and why"
- [ ] The 8-phase table-of-contents at the top works as navigation
- [ ] All references to `EMBEDDING.md` / `OPERATOR-SETUP.md` / `MIGRATION.md` are removed from the codebase
- [ ] A non-technical reader can follow the doc top-to-bottom and produce a working SAM install without asking clarifying questions (the bar)

## Next deliverable (separate spec)

Once `DEPLOY-TO-SAM.md` is shipped, the same brainstorming flow produces a maintenance guide covering: shipping patches, rolling back, updating the vendored `shared/` library, debugging production issues, adding a new plugin. Filename TBD — likely `apps-sam/MAINTAIN-SAM-PLUGINS.md`.
