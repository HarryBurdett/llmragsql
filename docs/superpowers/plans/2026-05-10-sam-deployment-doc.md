# SAM Deployment Guide Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write a single unified deployment guide (`apps-sam/DEPLOY-TO-SAM.md`) that walks the SAM team end-to-end through installing the four rewritten plugins; delete the three superseded handoff docs; push to GitHub; email the new doc to Charlie and Jonathan.

**Architecture:** Pure documentation deliverable — no code changes. Implementation = drafting markdown in 4 sequential chunks (one per group of phases), updating cross-references in 2 sibling docs, deleting 3 superseded docs, pushing to `origin/main`, and emailing the result.

**Tech Stack:** Markdown, git, `gh` CLI, the project's `/api/email/send` endpoint.

**Working directory for all tasks:** `/Users/maccb/llmragsql/` (the parent repo on `main` — NOT a worktree). Verify before starting:
```bash
cd /Users/maccb/llmragsql && git branch --show-current && git status -sb
```
Expected: `main`, working tree clean (or with only `.claude/` untracked artifact).

---

## File Structure

**Files created (1):**
- `apps-sam/DEPLOY-TO-SAM.md` — the new unified guide (~700 lines).

**Files modified (2):**
- `apps-sam/README.md` — replace links/mentions of the three deleted docs with the single new one.
- `apps-sam/EMAIL-DRAFT.md` — replace its references to `EMBEDDING.md`/`OPERATOR-SETUP.md`/`MIGRATION.md` with `DEPLOY-TO-SAM.md`.

**Files deleted (3):**
- `apps-sam/EMBEDDING.md`
- `apps-sam/OPERATOR-SETUP.md`
- `apps-sam/MIGRATION.md`

**Files referenced (not modified):**
- `docs/superpowers/specs/2026-05-10-sam-deployment-doc-design.md` — the spec the plan implements.
- `apps-sam/scripts/extract-all.sh`, `push-to-github.sh` — referenced by the doc but not modified.
- `apps-sam/scripts/migrate-from-python/` — referenced by the doc but not modified.

**Out-of-repo deliverables:**
- One email to `charlieb@intsysuk.com`
- One email to `jonathang@intsysuk.com`
- (Sent as two separate calls since the endpoint takes a single `to: str` per [api/main.py:6811](api/main.py:6811).)

---

## Format conventions for every code-fence-style block written into `DEPLOY-TO-SAM.md`

Reference these in every task that writes doc content. Every code fence in the new doc MUST start with one of:

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

Every command must be followed by an "✓ Looks good if you see:" expected-output block AND at least one "✗ If you see …" inline failure-mode block where the failure is plausible. Each phase must open with a one-sentence "what you're doing here and why."

---

## Task 1: Write doc skeleton + complete Phase 0

**Files:**
- Create: `apps-sam/DEPLOY-TO-SAM.md`

**What this task produces:** The doc file with full top-matter (title, intro, table of contents linking to 8 phases) and all of Phase 0 (six subsections), establishing every format convention. Approximately 200 lines.

- [ ] **Step 1: Create the doc with top-matter and table of contents.**

Write the following to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
# Deploying the four plugins into SAM

This guide walks the SAM team end-to-end through installing the four rewritten plugins (bank-reconcile, gocardless, suppliers, balance-check) into a running SAM instance. Anyone can follow it regardless of prior familiarity with the stack — every command tells you which machine to run it on and what success looks like.

**Estimated time:** ~90 minutes of execution, split between Jonathan (~40 min) and Charlie (~50 min).

**Important:** This guide is the canonical deployment instructions. The previous handoff docs (`EMBEDDING.md`, `OPERATOR-SETUP.md`, `MIGRATION.md`) have been retired — everything you need is here.

---

## Table of contents

| Phase | Title | Who | Est. time |
|---|---|---|---|
| [0](#phase-0--before-you-start) | Before you start | Read together | 15 min |
| [1](#phase-1--extract-the-four-plugins) | Extract the four plugins | Jonathan | 10 min |
| [2](#phase-2--create-github-repos-and-push) | Create GitHub repos and push | Jonathan | 15 min |
| [3](#phase-3--register-in-sam-central) | Register in SAM Central | Jonathan | 15 min |
| [4](#phase-4--install-on-the-sam-host) | Install on the SAM host | Charlie | 5 min |
| [5](#phase-5--configure-each-plugin) | Configure each plugin | Charlie | 20 min |
| [6](#phase-6--migrate-existing-data) | Migrate existing data | Charlie | 15 min |
| [7](#phase-7--verify-everything-works) | Verify everything works | Both | 15 min |
| [∎](#-done--hand-back-to-harry) | Done — hand back to Harry | — | — |
| [Troubleshooting](#troubleshooting) | Common failure modes | reference | — |

---
````

- [ ] **Step 2: Write Phase 0.1 — "What kind of work is this?" (the idiot's guide).**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 0 — Before you start

Phase 0 explains what you're doing, on what machines, with what tools, before any command is run.

### 0.1 — What kind of work is this?

**Deploying the four plugins to SAM is entirely web-UI configuration. No code changes. No SQL scripts. No editing of config files.**

This section sets the expectation up front so nothing in later phases surprises you.

#### What "code" means here vs what "config" means here

- **Code** = TypeScript files inside each plugin's `src/` and `db/migrations/` folders. These are written by Harry and shipped on GitHub. You never edit these.
- **Config** = values typed into form fields in a web browser (SAM Admin or each plugin's own Settings page). This is what you do.

#### How the four plugins sit inside SAM

```
                    ┌──────────────────────────┐
                    │      SAM platform        │
                    │  (Docker on the SAM Mac) │
                    └──────────┬───────────────┘
                               │ provides at runtime:
                               │   • Opera DB connection
                               │   • Per-app database (one each)
                               │   • Mailbox access
                               │   • LLM service
                               ▼
   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   │ bank-        │  │ gocardless   │  │ suppliers    │  │ balance-     │
   │ reconcile    │  │              │  │              │  │ check        │
   └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
```

#### Pre-flight code rewires — already done

Earlier handoff drafts mentioned two small code changes needed before plugins could be installed (replacing `ctx.config.mailboxes` with `listMyMailboxes()`, and folder-storage reading from the `folder_settings` table). **Both are already done in the code Jonathan extracts.** You can ignore any older docs that mention pre-flight rewires.

#### The email side

| Layer | Code or config? | Where | Status |
|---|---|---|---|
| Plugin asks SAM for its mailbox via `ctx.emailIngest.listMyMailboxes()` | Code | Inside each plugin's default email-ingest adapter | ✅ Already done in the code on GitHub |
| SAM stores which mailbox belongs to which plugin (`owner_app_id` column on the `email_mailboxes` table) | Config | SAM Admin → Email Mailboxes UI | Charlie does this in Phase 5 |
| Microsoft Graph credentials (the underlying connection) | Config | SAM Admin → Email Settings | Should already exist if SAM serves email today |

So: the plugin code already knows how to ask SAM for its mailbox. The remaining email work is **Charlie clicking "this mailbox belongs to GoCardless, this one to bank-reconcile, this one to suppliers" in SAM Admin.** No coding, no terminal commands.

#### The SQL side — three separate connections

There are **three** different SQL connections in play. Keeping them straight prevents confusion later.

**1. The Opera database** (SQL Server for Opera SE / FoxPro share for Opera 3)
This is the main one — where customers, suppliers, and transactions live.

| Layer | Code or config? | Where | Status |
|---|---|---|---|
| Plugin reads Opera via `ctx.db.opera` (SAM provides the connection) | Code | Already in every plugin | ✅ Done |
| Connection strings — server, port, database name, credentials | Config | SAM Admin → Opera Connections | Should already be set if SAM is already talking to Opera. Both `Intsys` and `CloudSiS` companies auto-discovered from Opera's `seqco` table. |

**2. The per-app database** (one MSSQL database per plugin, SAM creates automatically)
Each plugin stores its own state (bank aliases, GoCardless mandates, supplier statements) in its own private database.

| Layer | Code or config? | Where | Status |
|---|---|---|---|
| Schema — tables, indexes | Code (`db/migrations/*.ts`) | Inside each plugin | ✅ Done |
| Database creation + running migrations | SAM internal | Automatic on plugin install | Happens automatically in Phase 4 |
| Connection string | SAM-managed | Plugin never sees it — SAM hands the plugin a ready-made `ctx.db.app` | Automatic |

**3. Per-plugin runtime settings** (API tokens, folder paths)
A few plugins need values that live inside their own per-app database, entered through their own Settings page after install.

| Plugin | What needs entering | How |
|---|---|---|
| bank-reconcile | Base folder path (only if monitoring a local folder for PDFs) | Plugin's own Settings page |
| gocardless | API token, environment (`sandbox`), company reference | Plugin's own Settings page. ⚠ Sandbox only until smoke-tested. |
| suppliers | Defaults are fine to start | No entry needed |
| balance-check | Nothing | No entry needed |

All of those are **form-field values in the plugin's UI** — not code, not SQL scripts, not config-file edits.

#### What the SAM team actually does, in 5 steps

1. **Confirm Opera connections exist in SAM Admin** (verify only — should already be there).
2. **Confirm Microsoft Graph email is active in SAM Admin** (verify only — should already be there).
3. **Assign three mailboxes to three plugins** in SAM Admin → Email Mailboxes (3 clicks, 3 saves).
4. **Enter the GoCardless sandbox token** in the GoCardless plugin's own Settings page (paste, save).
5. **Optionally set a bank-reconcile folder path** in bank-reconcile's Settings page (paste, save, only if monitoring a local folder).

That's the entirety of email + SQL work. If you ever find yourself wondering "wait, do I need to write code?" — the answer is no. Come back to this section.

---
````

- [ ] **Step 3: Write Phase 0.2 — "The two machines you'll be using".**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
### 0.2 — The two machines you'll be using

This guide uses commands that run on two different computers. Every code block in this guide is labelled at the top so you know which one to use. **If a command isn't working, first check you're on the right machine.**

| Label | Which machine | Who uses it | What's installed |
|---|---|---|---|
| `# Terminal — your Mac` | Jonathan's laptop | Jonathan | `git`, `node`, `npm`, `gh` (GitHub CLI). A clone of the SQLRAG monorepo. Does NOT run SAM. |
| `# Terminal — SAM Mac` | The Mac that hosts SAM (Charlie's) | Charlie | SAM running in Docker. Direct access to SAM's MSSQL database. |

Two other labels also appear:

- `# Browser — GitHub` — any computer, just web pages on github.com
- `# Browser — SAM Central` — any computer, just the SAM Central admin web UI

---
````

- [ ] **Step 4: Write Phase 0.3 — "Accounts and access you need".**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
### 0.3 — Accounts and access you need

Confirm each of these before starting. If something is missing, sort it out now — having it half-way through is more painful.

| Access | Who needs it | How to check |
|---|---|---|
| Write access to the `intsysuk` GitHub organisation | Jonathan | Browse to `https://github.com/intsysuk` — if you can see private repos and the "+ New" button, you're good |
| SAM Central admin login | Jonathan | Log into SAM Central; look for the "Apps" item in the left nav |
| SSH or physical access to the SAM Mac | Charlie | `ssh sam-mac.local` (or whatever the hostname is) should give you a shell |
| GoCardless sandbox API token + company reference | Charlie | You'll need these in Phase 5. Generate from the GoCardless sandbox dashboard. |
| `gh` CLI authenticated to GitHub | Jonathan | Run `gh auth status` in a terminal — should show "Logged in to github.com" |

---
````

- [ ] **Step 5: Write Phase 0.4 — "Software each machine needs".**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
### 0.4 — Software each machine needs

#### On Jonathan's Mac

```
# Terminal — your Mac
git --version    # Expect: git version 2.x or higher
node --version   # Expect: v18 or higher
npm --version    # Expect: 9 or higher
gh --version     # Expect: gh version 2.x
```

**✗ If `gh` is not found** — install with `brew install gh`, then `gh auth login`.

**✗ If `node` is too old** — install via `nvm` (`brew install nvm`) and `nvm install 20`.

#### On the SAM Mac

```
# Terminal — SAM Mac
docker --version    # Expect: Docker version 24+ (whatever SAM requires)
docker ps           # Expect: see the `ai-sam` container running
```

**✗ If `docker ps` is empty** — SAM isn't running. Start it before proceeding (`docker compose up -d` in SAM's deployment folder, or whatever your runbook says).

---
````

- [ ] **Step 6: Write Phase 0.5 — "What you're deploying".**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
### 0.5 — What you're deploying

Four plugins, each one a full-stack app (backend + frontend) that gets installed into SAM.

| Plugin | What it does | Where the data comes from |
|---|---|---|
| **bank-reconcile** | Imports bank statements (PDF or email), matches lines against Opera cashbook, posts unmatched items, reconciles | Bank statement PDFs (email or local folder) |
| **gocardless** | Imports GoCardless Direct Debit payout emails, matches payments to customers, posts as sales receipts | GoCardless payout notification emails |
| **suppliers** | Reconciles supplier statements against the Purchase Ledger, manages supplier communications | Supplier statement emails |
| **balance-check** | Internal Opera control-account reconciliation (Cashbook, Debtors, Creditors, VAT) | Direct queries on Opera |

All four run inside SAM and connect to Opera SE or Opera 3 via the connection SAM already has set up.

---
````

- [ ] **Step 7: Write Phase 0.6 — "What 'done' looks like".**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
### 0.6 — What "done" looks like

By the end of this guide, all of these should be true:

- [ ] All four plugins appear under **SAM Admin → Apps** with a green "Installed" status
- [ ] Each plugin's UI loads when you navigate to it in SAM
- [ ] A bank-statement test scan returns 1 result
- [ ] A GoCardless test API call returns 200 OK against the sandbox
- [ ] The supplier dashboard shows migrated rows (statements, contacts, aliases)
- [ ] The balance-check page loads and shows current control-account totals
- [ ] The legacy Python apps still work in parallel (sanity check — nothing should have broken them)

When all eight are ticked, you're done. Hand back to Harry.

---
````

- [ ] **Step 8: Verify the file's current state.**

Run:
```bash
cd /Users/maccb/llmragsql && wc -l apps-sam/DEPLOY-TO-SAM.md
```
Expected: ~200 lines.

```bash
grep -c "^### 0\." apps-sam/DEPLOY-TO-SAM.md
```
Expected: 6 (six Phase 0 subsections).

```bash
grep -c "# Terminal — your Mac\|# Terminal — SAM Mac\|# Browser —" apps-sam/DEPLOY-TO-SAM.md
```
Expected: at least 5 (one per machine-labelled code block written so far).

- [ ] **Step 9: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/DEPLOY-TO-SAM.md
git commit -m "$(cat <<'EOF'
docs: start DEPLOY-TO-SAM.md with Phase 0 (idiot's guide + setup)

Phase 0 covers: what kind of work this is (no coding), the two
machines you'll be using, accounts/access needed, software per
machine, what you're deploying, what 'done' looks like.

First chunk of the unified SAM deployment guide per spec
2026-05-10-sam-deployment-doc-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Write Phases 1–3 (Jonathan's section)

**Files:**
- Modify: `apps-sam/DEPLOY-TO-SAM.md` (append).

**What this task produces:** The complete Jonathan portion — extract, push, register. Approximately 200 lines.

- [ ] **Step 1: Write Phase 1 — Extract the four plugins.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 1 — Extract the four plugins

**What you're doing here:** The four plugins currently live as workspaces inside the SQLRAG monorepo. Before SAM can install them, each one has to be packaged as a standalone repo. The extraction script does this in one go — it copies each plugin's folder, vendors the shared library into each, runs the tests, and runs the build. The result is four ready-to-push folders in `~/sam-plugins-staging/`.

### Step 1.1 — Clone or update the SQLRAG monorepo on your Mac

If you've never cloned it before:

```
# Terminal — your Mac
cd ~
git clone https://github.com/HarryBurdett/llmragsql.git
cd llmragsql
```

If you already have it:

```
# Terminal — your Mac
cd ~/llmragsql
git checkout main
git pull
```

**✓ Looks good if you see:** the latest commit dated 2026-05-09 or later when you run `git log -1`.

### Step 1.2 — Run the extraction script

```
# Terminal — your Mac
cd ~/llmragsql
chmod +x apps-sam/scripts/*.sh
./apps-sam/scripts/extract-all.sh
```

**✓ Looks good if you see:**
```
✓ balance-check — tests passed, lint clean, build ok → ~/sam-plugins-staging/sam-balance-check
✓ bank-reconcile — tests passed, lint clean, build ok → ~/sam-plugins-staging/sam-bank-reconcile
✓ gocardless — tests passed, lint clean, build ok → ~/sam-plugins-staging/sam-gocardless
✓ suppliers — tests passed, lint clean, build ok → ~/sam-plugins-staging/sam-suppliers
```

**✗ If you see `Permission denied`** — `chmod +x` didn't work. Try `bash apps-sam/scripts/extract-all.sh` instead.

**✗ If you see a test failure** — stop. Don't proceed. Re-run with `npm install` first in `apps-sam/` (`cd apps-sam && npm install && cd ..`) then try again. If still failing, message Harry — the build is broken upstream and the deployment is not safe to proceed.

### Step 1.3 — Confirm the four staged folders exist

```
# Terminal — your Mac
ls -la ~/sam-plugins-staging/
```

**✓ Looks good if you see:** four directories: `sam-balance-check`, `sam-bank-reconcile`, `sam-gocardless`, `sam-suppliers`.

---
````

- [ ] **Step 2: Write Phase 2 — Create GitHub repos and push.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 2 — Create GitHub repos and push

**What you're doing here:** Each plugin needs its own GitHub repo so SAM Central can pull it. You'll create four empty private repos under the `intsysuk` organisation, then the push script pushes each plugin's code and tags it `v1.0.0`.

### Step 2.1 — Create four empty private repos on GitHub

For each of the four plugins, do this in the GitHub web UI:

```
# Browser — GitHub
```

1. Go to https://github.com/organizations/intsysuk/repositories/new
2. Repository name: `sam-balance-check` (then `sam-bank-reconcile`, `sam-gocardless`, `sam-suppliers`)
3. Visibility: **Private**
4. **Do not** initialise the repo (no README, no .gitignore, no licence — leave all those unchecked). The extraction script ships these.
5. Click **Create repository**.

**✓ Looks good if you see:** after creating all four, https://github.com/intsysuk shows the four `sam-*` repos in the list.

**Alternatively, with `gh` CLI** (faster if you prefer the terminal):

```
# Terminal — your Mac
gh repo create intsysuk/sam-balance-check --private --confirm
gh repo create intsysuk/sam-bank-reconcile --private --confirm
gh repo create intsysuk/sam-gocardless --private --confirm
gh repo create intsysuk/sam-suppliers --private --confirm
```

**✗ If you see `HTTP 422: Repository already exists`** — repo was created before. Either delete it from GitHub and retry, or skip Step 2.1 for that plugin and proceed to Step 2.2.

### Step 2.2 — Push each plugin to its repo

For each plugin, run:

```
# Terminal — your Mac
cd ~/sam-plugins-staging
./push-to-github.sh balance-check
```

**✓ Looks good if you see:**
```
Pushing to https://github.com/intsysuk/sam-balance-check.git…
✓ Success — v1.0.0 tagged and pushed
```

**✗ If you see `Permission denied (publickey)`** — your SSH key isn't set up for GitHub. Run `gh auth status` to check. If you'd rather use HTTPS, edit the `REMOTE_URL` line in the push script to use `https://` and `gh auth refresh -s repo`.

**✗ If you see `remote: Repository not found`** — the repo doesn't exist. Go back to Step 2.1.

Repeat for the other three plugins:

```
# Terminal — your Mac
./push-to-github.sh bank-reconcile
./push-to-github.sh gocardless
./push-to-github.sh suppliers
```

### Step 2.3 — Confirm v1.0.0 tag and code on GitHub

For each plugin, browse to the repo on GitHub. Check:

- [ ] There are files in the repo (not empty)
- [ ] The "tags" page (e.g. `https://github.com/intsysuk/sam-balance-check/tags`) shows `v1.0.0`

**✓ All four repos populated with v1.0.0 tagged** — Phase 2 is done.

---
````

- [ ] **Step 3: Write Phase 3 — Register in SAM Central.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 3 — Register in SAM Central

**What you're doing here:** SAM Central is the central registry that tells each SAM host instance which apps to install. You'll add four catalogue entries (one per plugin), pin each to `v1.0.0`, and assign them to the IntSys client's license. Charlie's SAM host will pick these up automatically when he triggers a sync in Phase 4.

> **⚠ UI verification note**: This phase is written from the existing handoff docs. The exact button labels and screen names in SAM Central may differ slightly from what's described here. If anything doesn't match, follow the spirit of each step — the data being entered is what matters. Send Harry a one-line update on any UI discrepancies so this doc can be corrected for next time.

### Step 3.1 — Open SAM Central

```
# Browser — SAM Central
```

Log in. Navigate to the **Apps** section (usually in the left nav).

### Step 3.2 — Add each plugin to the Apps catalogue

For each of the four plugins, do the following:

1. Click **+ Add app** (or **+ New app**).
2. Fill in the fields:

| Field | Value (for balance-check; substitute per plugin) |
|---|---|
| App ID | `balance-check` |
| Display name | `Balance Check` |
| Git URL | `https://github.com/intsysuk/sam-balance-check.git` |
| Default version | `v1.0.0` |
| Authentication | Use the org-wide GitHub PAT (configured at the org level — should already exist) |

3. Click **Save**.

**✓ Looks good if you see:** the app appears in the catalogue list with status "Ready" or similar.

**✗ If saving fails with "git URL not reachable"** — Central's GitHub PAT doesn't have access to the new private repo. Add the repos to the PAT's scope (in SAM Central → Settings → Integrations → GitHub), or extend the PAT's organisation read scope.

Repeat for `bank-reconcile`, `gocardless`, `suppliers` — substituting names accordingly.

### Step 3.3 — Assign `v1.0.0` to the IntSys client license

In SAM Central, navigate to **Clients** (or **Licenses**) → **IntSys**.

For each of the four plugins:

1. Click **+ Add app to license**.
2. Select the plugin from the dropdown.
3. Pin version: `v1.0.0`.
4. Click **Save**.

**✓ Looks good if you see:** all four plugins listed under IntSys's licensed apps, each at `v1.0.0`.

### Step 3.4 — Confirm Central can pull the repos

In SAM Central, on each app's catalogue entry, click **Test pull** (or equivalent — there's usually a "verify" or "preview" button).

**✓ Looks good if you see:** `Success — manifest read, version v1.0.0`.

**✗ If you see any pull error** — fix it now. The most common cause is the GitHub PAT lacking read access to the new private repos.

### Step 3.5 — Ping Charlie

Message Charlie: "All four apps registered in Central. Ready for you to trigger sync on the SAM Mac."

Phase 3 done. Hand-off complete.

---
````

- [ ] **Step 4: Verify file growth.**

```bash
cd /Users/maccb/llmragsql && wc -l apps-sam/DEPLOY-TO-SAM.md
```
Expected: ~400 lines (200 from Task 1 + ~200 new).

```bash
grep -c "^## Phase" apps-sam/DEPLOY-TO-SAM.md
```
Expected: 4 (Phase 0, 1, 2, 3).

- [ ] **Step 5: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/DEPLOY-TO-SAM.md
git commit -m "$(cat <<'EOF'
docs: DEPLOY-TO-SAM.md Phases 1-3 (Jonathan's section)

Extract the four plugins from the monorepo, create GitHub repos and
push, register the apps in SAM Central with v1.0.0 pinned to the
IntSys license.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Write Phases 4–6 (Charlie's section)

**Files:**
- Modify: `apps-sam/DEPLOY-TO-SAM.md` (append).

**What this task produces:** The complete Charlie portion — install, configure, migrate data. Approximately 200 lines.

- [ ] **Step 1: Write Phase 4 — Install on the SAM host.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 4 — Install on the SAM host

**What you're doing here:** Charlie's first job. SAM Central now knows about the four apps; the SAM host needs to pull them down and install them. One click triggers everything; the rest is watching logs.

### Step 4.1 — Trigger an app sync from SAM Admin

```
# Browser — SAM Central
```

In SAM Admin (the host's admin UI, not Central), go to **Apps** → click **Sync now**.

### Step 4.2 — Watch the install in the SAM container's logs

```
# Terminal — SAM Mac
docker logs -f ai-sam | grep -E "GitInstall|PluginLoader"
```

**✓ Looks good if you see** (one set of lines per plugin, four sets total):

```
[GitInstall] cloning github.com/intsysuk/sam-balance-check.git@v1.0.0
[GitInstall] Success
[PluginLoader] Loading balance-check
[PluginLoader] Running migrations for balance-check (0 to apply)
[PluginLoader] Loaded balance-check (7 routes registered)
```

**✗ If you see `[GitInstall] git: authentication required`** — the host's GitHub PAT isn't reaching the new repos. Check SAM host → Settings → Integrations → GitHub. The PAT needs `repo` scope on the `intsysuk` org.

**✗ If you see `[PluginLoader] migration failed`** — the per-app database creation failed. Check the SAM host's MSSQL instance is reachable and SAM has CREATE DATABASE rights. Send the full error to Harry.

**✗ If you see `[PluginLoader] manifest validation failed`** — the plugin's `manifest.json` doesn't match what this version of SAM expects. Likely a SAM-host version mismatch — Harry needs to know the host's version.

### Step 4.3 — Confirm all four plugins are installed in SAM Admin

```
# Browser — SAM Central
```

Navigate to SAM Admin → Apps. You should see four entries:

- [ ] balance-check — Installed
- [ ] bank-reconcile — Installed
- [ ] gocardless — Installed
- [ ] suppliers — Installed

Each should be green/healthy.

---
````

- [ ] **Step 2: Write Phase 5 — Configure each plugin.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 5 — Configure each plugin

**What you're doing here:** The plugins are installed but need to know which mailboxes to read, which GoCardless token to use, and which folders to scan. All configuration is done in the SAM Admin UI or each plugin's own Settings page — no terminal, no code.

### Step 5.1 — Verify Opera connections (should already exist)

```
# Browser — SAM Central
```

SAM Admin → **Opera Connections**. Confirm:

- [ ] `Intsys` is listed
- [ ] `CloudSiS` is listed

Both should be auto-discovered from Opera's `seqco` table. If neither appears, SAM has never been wired to Opera here — that's a wider setup issue, message Harry before proceeding.

### Step 5.2 — Verify Microsoft Graph email is active

SAM Admin → **Email Settings**. Status should read **Active** with a recent successful sync timestamp.

**✗ If status is `Disconnected`** — the Microsoft Graph integration needs re-auth. Click **Reconnect** and complete the OAuth flow.

### Step 5.3 — Assign mailboxes to plugins

SAM Admin → **Email Mailboxes**. For each mailbox in the table, set the `owner_app_id`:

| Mailbox purpose | owner_app_id |
|---|---|
| Bank statement notifications (Barclays, HSBC, etc.) | `bank-reconcile` |
| GoCardless payout notification emails | `gocardless` |
| Supplier statement emails | `suppliers` |

**✓ Looks good if you see:** three mailboxes assigned, each with the correct app shown next to it.

### Step 5.4 — Configure GoCardless plugin

```
# Browser — SAM Central
```

Navigate to the GoCardless plugin's UI → **Settings**. Enter:

| Field | Value |
|---|---|
| Environment | **sandbox** |
| API token | The sandbox token Charlie has from the GoCardless sandbox dashboard |
| Company reference | The IntSys company reference (matches Opera's company ID) |

Save.

⚠ **Do not use the live GoCardless API token until the sandbox smoke-test in Phase 7 passes.** Live calls move real money; we test in sandbox first.

### Step 5.5 — Configure bank-reconcile plugin (optional)

If you also watch a local folder for bank-statement PDFs (separate from email):

Navigate to bank-reconcile's UI → **Settings** → **Folder settings**. Enter the absolute path to the folder you watch. Save.

If you only ingest statements via email, skip this step.

### Step 5.6 — Confirm suppliers + balance-check need no setup

The other two plugins ship with sensible defaults:

- **suppliers** — defaults are fine. You can tune contact/automation settings later.
- **balance-check** — read-only, no configuration.

---
````

- [ ] **Step 3: Write Phase 6 — Migrate existing data.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 6 — Migrate existing data from Python apps

**What you're doing here:** The four Python apps under `apps/` in the SQLRAG monorepo have been collecting data for months — bank aliases, GoCardless mandates, supplier statements. That data lives in SQLite files on the existing Python host. This step copies it into the new SAM per-app MSSQL databases so the new plugins start with the same knowledge.

### Step 6.1 — Make the Python data folder readable from the SAM Mac

The Python data lives at `/Users/maccb/llmragsql/data/<company>/` (the path on Harry's machine — Charlie's path depends on where the legacy Python is hosted).

Either:
- Mount the existing data folder as a network share readable from the SAM Mac, or
- Copy the folder to the SAM Mac (`rsync` from the source).

**✓ Looks good if you see:** running `ls /path/to/data/intsys/` on the SAM Mac shows folders like `bank_reconcile/`, `gocardless/`, `suppliers/`.

### Step 6.2 — Install migration tool dependencies

```
# Terminal — SAM Mac
cd /path/to/cloned/sqlrag-monorepo/apps-sam/scripts/migrate-from-python
npm install
```

(If the monorepo isn't on the SAM Mac, clone it: `git clone https://github.com/HarryBurdett/llmragsql.git`)

### Step 6.3 — Dry-run for each company × each plugin

**Always dry-run first.** The dry-run reads the source files and shows the row counts it would migrate without writing anything.

```
# Terminal — SAM Mac
npm run migrate -- \
  --company intsys \
  --plugin bank-reconcile \
  --data-root /path/to/data \
  --dry-run
```

**✓ Looks good if you see** something like:

```
[DRY-RUN] company=intsys plugin=bank-reconcile
  bank_import_aliases:     161 rows ready
  bank_import_patterns:     95 rows ready
  deferred_transactions:    10 rows ready
  (skipped: extraction_cache, import_locks — runtime state)
```

Expected approximate row counts per the existing migration runbook:

| Plugin | Company | Aliases | Patterns | Mandates | Statements |
|---|---|---|---|---|---|
| bank-reconcile | intsys | 161 | 95 | — | — |
| bank-reconcile | cloudsis | 1 | 7 | — | — |
| gocardless | intsys | — | — | 39 | — |
| gocardless | cloudsis | — | — | 35 | — |
| suppliers | intsys | — | — | — | 9 |
| suppliers | cloudsis | — | — | — | 5 |

Run dry-run for each combination — 6 commands total.

**✗ If you see `0 rows ready`** — `--data-root` is pointing at the wrong folder. Check the path.

### Step 6.4 — Run the migration for real

Remove the `--dry-run` flag and rerun each command:

```
# Terminal — SAM Mac
npm run migrate -- \
  --company intsys \
  --plugin bank-reconcile \
  --data-root /path/to/data
```

**✓ Looks good if you see:** `inserted: X rows` where X matches the dry-run count.

**✗ If you see `duplicate key` errors** — the script is idempotent on `source_ref`. Errors mean either: (a) you already ran this and re-running double-counts (safe to ignore — it skipped existing rows); or (b) a unique constraint on the per-app DB doesn't match the source data. Send the full error to Harry.

Repeat for each combination. balance-check has no data to migrate (read-only plugin).

---
````

- [ ] **Step 4: Verify file growth.**

```bash
cd /Users/maccb/llmragsql && wc -l apps-sam/DEPLOY-TO-SAM.md
```
Expected: ~600 lines.

```bash
grep -c "^## Phase" apps-sam/DEPLOY-TO-SAM.md
```
Expected: 7 (Phases 0-6).

- [ ] **Step 5: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/DEPLOY-TO-SAM.md
git commit -m "$(cat <<'EOF'
docs: DEPLOY-TO-SAM.md Phases 4-6 (Charlie's section)

Install on the SAM host, configure each plugin (mailboxes,
GoCardless sandbox token, bank-reconcile folder), migrate
existing per-company data from Python SQLite databases.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Write Phase 7, Done, Troubleshooting appendix

**Files:**
- Modify: `apps-sam/DEPLOY-TO-SAM.md` (append).

**What this task produces:** The verification phase, the hand-back paragraph, and a troubleshooting appendix. Approximately 100 lines.

- [ ] **Step 1: Write Phase 7 — Verify everything works.**

Append to `apps-sam/DEPLOY-TO-SAM.md`:

````markdown
## Phase 7 — Verify everything works

**What you're doing here:** Smoke-test each plugin end-to-end before declaring done. Catch any "looks installed but doesn't actually work" issues now, while the install context is fresh.

### Step 7.1 — Smoke-test balance-check

```
# Browser — SAM Central
```

Open the balance-check plugin in SAM. Navigate to the **Cashbook reconcile** view.

**✓ Looks good if you see:** a list of cashbook balances loaded from Opera (e.g. each bank account's balance from `nbank` matches the corresponding `nacnt` row).

### Step 7.2 — Smoke-test bank-reconcile

Open bank-reconcile → click **Scan email**. Wait ~30 seconds.

**✓ Looks good if you see:** at least one bank statement appears in the list (from the bank-statements mailbox you assigned).

If no statements appear and you know there should be some in the mailbox: check Phase 5.3 — the mailbox `owner_app_id` may not be set correctly.

### Step 7.3 — Smoke-test gocardless

Open gocardless → **Settings** → **Test API connection**.

**✓ Looks good if you see:** `Success — connected to GoCardless sandbox`.

**✗ If you see `401 Unauthorized`** — the sandbox token is wrong or the environment is set to `live`. Check Phase 5.4.

### Step 7.4 — Smoke-test suppliers

Open suppliers → **Dashboard**.

**✓ Looks good if you see:** the migrated supplier statements from Phase 6 listed (9 for intsys, 5 for cloudsis).

### Step 7.5 — Sanity check: legacy Python apps still work

```
# Browser
```

In a different browser tab, open the legacy SQLRAG frontend (`http://localhost:5173` or wherever it runs). Navigate to:

- Cashbook → Cashbook Reconcile
- GoCardless → Import
- Suppliers → Dashboard

**✓ Looks good if you see:** all three still load and function as they did before. (The legacy code is preserved as the canonical behavioural reference — nothing in the SAM merge should have changed it.)

---

## ∎ Done — hand back to Harry

When all eight checkboxes in section 0.6 are ticked and Phase 7's smoke tests all pass, the SAM deployment is done. Email Harry:

```
Subject: SAM deployment complete

Harry,

All four plugins installed and verified in SAM:

- balance-check: installed, smoke-test passed
- bank-reconcile: installed, scan returned N statements
- gocardless: installed, sandbox API connection successful
- suppliers: installed, M statements migrated and visible

Open items handed back to you:
- GoCardless: still on sandbox token. Swap to live token after [N] days of clean sandbox runs.
- Any phase-3 UI discrepancies noticed: [list, or "none"].
- Anything else: [...].

Charlie + Jonathan
```

### Deferred items (Harry's responsibility, not SAM team's)

- Swap GoCardless sandbox → live token after smoke-test period
- Decide when to retire the legacy frontend menu items (the legacy *backend* code stays per repo policy)
- Schedule the maintenance-guide brainstorming session

---

## Troubleshooting

Quick reference for the most likely failure modes. Listed in the order you're likely to hit them.

### `extract-all.sh` fails partway through

Most common cause: stale `node_modules` in `apps-sam/`. Fix:

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam
rm -rf node_modules
npm install
cd ..
./apps-sam/scripts/extract-all.sh
```

### `push-to-github.sh` says `Permission denied (publickey)`

Your SSH key isn't loaded for GitHub. Run `gh auth status`. If not logged in, `gh auth login`. If you'd rather use HTTPS, edit `REMOTE_URL` in the push script.

### SAM Central can't reach the new private repos

Central's PAT scope is too narrow. SAM Central → Settings → Integrations → GitHub. Either re-issue the PAT with full `repo` scope on `intsysuk`, or add the four new repos individually if your auth setup uses fine-grained tokens.

### `[PluginLoader] Loaded` lines don't appear

Either the install failed (see `[GitInstall]` lines above) or `docker logs -f` isn't following — try `docker logs --tail 200 ai-sam` to see static history.

### Mailbox assignment doesn't trigger scans

The email-ingest adapter checks for new mailbox assignments every ~60 seconds. Wait a minute, then trigger the scan manually from the plugin's UI.

### GoCardless API call returns 401

Three possible causes (in order):

1. Token typo — re-paste from the dashboard.
2. Environment mismatch — token is for `live` but plugin is configured for `sandbox` (or vice versa).
3. Token was revoked in the GoCardless dashboard — generate a new one.

### Migration `0 rows ready` in dry-run

`--data-root` path is wrong. Verify: `ls $DATA_ROOT/intsys/bank_reconcile/` should show `.db` files.

### Migration says `duplicate key`

You already ran the migration for that combo. Safe to ignore — the script is idempotent on `source_ref`. Existing rows are skipped, not re-inserted.

---

**End of guide.** If you hit something not covered here, message Harry with: which phase + step number you're on, the exact command/click, and the full error output.
````

- [ ] **Step 2: Verify final file structure.**

```bash
cd /Users/maccb/llmragsql && wc -l apps-sam/DEPLOY-TO-SAM.md
```
Expected: ~700 lines.

```bash
grep -c "^## Phase\|^## ∎ Done\|^## Troubleshooting" apps-sam/DEPLOY-TO-SAM.md
```
Expected: 10 (Phases 0-7 + Done + Troubleshooting).

```bash
grep -c "^# Terminal\|^# Browser" apps-sam/DEPLOY-TO-SAM.md
```
Expected: at least 20 (every command labelled).

```bash
grep -c "✓ Looks good\|✗ If" apps-sam/DEPLOY-TO-SAM.md
```
Expected: at least 30 (output markers + failure modes).

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/DEPLOY-TO-SAM.md
git commit -m "$(cat <<'EOF'
docs: DEPLOY-TO-SAM.md Phase 7 + Done + Troubleshooting

Verify-everything-works smoke tests for all four plugins, hand-back
email template, troubleshooting appendix covering the most likely
failure modes from extraction through migration.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Update cross-references in `apps-sam/README.md` and `apps-sam/EMAIL-DRAFT.md`

**Files:**
- Modify: `apps-sam/README.md`
- Modify: `apps-sam/EMAIL-DRAFT.md`

**What this task produces:** Both sibling docs now point at `DEPLOY-TO-SAM.md` instead of the three docs about to be deleted.

- [ ] **Step 1: Inspect `apps-sam/README.md`'s current cross-references.**

Run:
```bash
cd /Users/maccb/llmragsql && grep -n "EMBEDDING\|OPERATOR-SETUP\|MIGRATION" apps-sam/README.md
```

Note every line number that references one of the three to-be-deleted docs.

- [ ] **Step 2: Update `apps-sam/README.md`.**

For each match found in Step 1, replace mentions of `EMBEDDING.md` / `OPERATOR-SETUP.md` / `MIGRATION.md` with a single reference to `DEPLOY-TO-SAM.md`. Where the README has separate paragraphs for "embedding" / "operator setup" / "migration", consolidate into one paragraph pointing at `DEPLOY-TO-SAM.md`:

```markdown
## Deploying to SAM

See [DEPLOY-TO-SAM.md](DEPLOY-TO-SAM.md) for the complete end-to-end deployment guide: extracting the plugins from this monorepo, pushing to GitHub, registering in SAM Central, installing on the SAM host, configuring each plugin, and migrating data from the legacy Python apps.
```

- [ ] **Step 3: Inspect `apps-sam/EMAIL-DRAFT.md`'s current cross-references.**

```bash
cd /Users/maccb/llmragsql && grep -n "EMBEDDING\|OPERATOR-SETUP\|MIGRATION" apps-sam/EMAIL-DRAFT.md
```

- [ ] **Step 4: Update `apps-sam/EMAIL-DRAFT.md`.**

For each line found in Step 3, replace the reference. Where the email draft says "Detailed sequence in `apps-sam/EMBEDDING.md` and `apps-sam/OPERATOR-SETUP.md` (Phases 1–2)", replace with: "Detailed sequence in [DEPLOY-TO-SAM.md](DEPLOY-TO-SAM.md) (Phases 1–3 cover your section)".

Where it says "Run the migration tool to bring our existing learned data across", point to "DEPLOY-TO-SAM.md Phase 6".

- [ ] **Step 5: Verify no stale references remain.**

```bash
cd /Users/maccb/llmragsql && grep -rn "EMBEDDING\.md\|OPERATOR-SETUP\.md\|MIGRATION\.md" apps-sam/ --include="*.md"
```

Expected: zero matches (other than possibly inside `DEPLOY-TO-SAM.md` itself referring to the deletion). If matches remain, fix them.

- [ ] **Step 6: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/README.md apps-sam/EMAIL-DRAFT.md
git commit -m "$(cat <<'EOF'
docs: update apps-sam cross-references to DEPLOY-TO-SAM.md

README.md and EMAIL-DRAFT.md now point at the unified deployment
guide instead of the three superseded docs being deleted next.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Delete the three superseded docs

**Files:**
- Delete: `apps-sam/EMBEDDING.md`
- Delete: `apps-sam/OPERATOR-SETUP.md`
- Delete: `apps-sam/MIGRATION.md`

**What this task produces:** A single commit removing all three superseded files.

- [ ] **Step 1: Confirm no remaining cross-references.**

```bash
cd /Users/maccb/llmragsql && grep -rn "EMBEDDING\.md\|OPERATOR-SETUP\.md\|MIGRATION\.md" apps-sam/ --include="*.md" --include="*.ts"
```

Expected: zero matches.

**✗ If matches appear:** stop. Go back to Task 5 and clean them up before deleting.

- [ ] **Step 2: Delete the three files.**

```bash
cd /Users/maccb/llmragsql
git rm apps-sam/EMBEDDING.md apps-sam/OPERATOR-SETUP.md apps-sam/MIGRATION.md
```

**✓ Looks good if you see:** `rm 'apps-sam/EMBEDDING.md'` etc., three lines.

- [ ] **Step 3: Commit the deletion.**

```bash
cd /Users/maccb/llmragsql
git commit -m "$(cat <<'EOF'
docs: delete superseded SAM handoff docs

EMBEDDING.md, OPERATOR-SETUP.md, and MIGRATION.md all replaced by
the unified DEPLOY-TO-SAM.md. Cross-references in README.md and
EMAIL-DRAFT.md updated in the previous commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Final acceptance check + push to origin/main

**Files:** (verification only — no edits)

**What this task produces:** Confirms the spec's acceptance criteria all pass, then pushes everything to `origin/main` so Charlie and Jonathan can access the doc via GitHub.

- [ ] **Step 1: Verify every code block has a machine label.**

```bash
cd /Users/maccb/llmragsql
# Count code fences vs. labelled code fences
total_fences=$(grep -c '^```' apps-sam/DEPLOY-TO-SAM.md)
labelled=$(grep -cE '^```$|^```[a-z]+$' apps-sam/DEPLOY-TO-SAM.md)
# Should approximately match — most fences are language-labelled or blank
# Critical check: every fence that opens a command block has a `# Terminal —` or `# Browser —` line nearby
labelled_blocks=$(grep -cE '^# (Terminal —|Browser —)' apps-sam/DEPLOY-TO-SAM.md)
echo "Total fences: $total_fences"
echo "Labelled blocks: $labelled_blocks"
```

Expected: at least 25 labelled blocks. If fewer, audit any command blocks missing labels.

- [ ] **Step 2: Verify every command has an "✓ Looks good if" block.**

```bash
cd /Users/maccb/llmragsql
grep -cE "✓ Looks good if" apps-sam/DEPLOY-TO-SAM.md
```
Expected: at least 20.

- [ ] **Step 3: Verify the 8-phase table-of-contents at the top works.**

```bash
cd /Users/maccb/llmragsql
grep -A 12 "^## Table of contents" apps-sam/DEPLOY-TO-SAM.md | head -15
```
Expected: see 8 phase rows (0 through 7) plus Done and Troubleshooting.

- [ ] **Step 4: Confirm three old docs are gone, three references replaced.**

```bash
cd /Users/maccb/llmragsql
ls apps-sam/EMBEDDING.md apps-sam/OPERATOR-SETUP.md apps-sam/MIGRATION.md 2>&1
```
Expected: `No such file or directory` for all three.

```bash
grep -rln "EMBEDDING\.md\|OPERATOR-SETUP\.md\|MIGRATION\.md" apps-sam/ --include="*.md"
```
Expected: empty output.

- [ ] **Step 5: Show the git log of new commits before pushing.**

```bash
cd /Users/maccb/llmragsql
git log origin/main..HEAD --oneline
```
Expected: 5 commits (Task 1 + Task 2 + Task 3 + Task 4 + Task 5 + Task 6 — wait, that's 6. Actually the doc-writing tasks each produce 1 commit, plus Task 5 + Task 6 = 6 commits total. Verify the count matches what you've actually done.)

- [ ] **Step 6: Push to origin/main.**

```bash
cd /Users/maccb/llmragsql
git push origin main
```

**✓ Looks good if you see:** `<old-sha>..<new-sha>  main -> main` and no errors.

**✗ If push is rejected** — someone else pushed to main since the merge. Run `git pull --rebase` and retry. If conflicts occur, stop and message Harry.

- [ ] **Step 7: Verify the doc is visible on GitHub.**

```bash
echo "Open in browser: https://github.com/HarryBurdett/llmragsql/blob/main/apps-sam/DEPLOY-TO-SAM.md"
```

Open that URL. The doc should render with all phases, code blocks, tables.

---

## Task 8: Draft email body, show to Harry for approval

**Files:** (no file changes — output to chat for review)

**What this task produces:** A complete HTML email body ready to send, displayed to Harry for approval before any send call.

- [ ] **Step 1: Draft the email body (HTML).**

Construct the following HTML body. Substitute `<DOC_URL>` with the actual GitHub URL: `https://github.com/HarryBurdett/llmragsql/blob/main/apps-sam/DEPLOY-TO-SAM.md`

```html
<p>Hi Jonathan, Charlie,</p>

<p>The TypeScript SAM-plugin rewrite of the four Python apps (bank-reconcile, gocardless, suppliers, balance-check) is ready to install. A unified deployment guide is now in the SQLRAG repo on the <code>main</code> branch.</p>

<h3>The guide</h3>
<p><strong>One document covers everything end-to-end:</strong> <a href="https://github.com/HarryBurdett/llmragsql/blob/main/apps-sam/DEPLOY-TO-SAM.md">DEPLOY-TO-SAM.md</a></p>

<p>~90 minutes of actual work, split roughly:</p>
<ul>
  <li><strong>Jonathan (~40 min):</strong> Phases 1–3 — extract plugins, push to GitHub, register in SAM Central</li>
  <li><strong>Charlie (~50 min):</strong> Phases 4–6 — install on SAM host, configure each plugin, migrate existing data</li>
  <li><strong>Both (~15 min):</strong> Phase 7 — verify everything works</li>
</ul>

<h3>Repo access</h3>
<p>You'll need access to <a href="https://github.com/HarryBurdett/llmragsql">github.com/HarryBurdett/llmragsql</a> — let me know your GitHub usernames and I'll add you as collaborators.</p>

<h3>Important notes</h3>
<ul>
  <li>The guide assumes <strong>no coding</strong>. Everything is web-UI configuration. Phase 0.1 explains this in detail.</li>
  <li>Every command is labelled with which machine to run it on (your Mac vs the SAM Mac).</li>
  <li>GoCardless: use the <strong>sandbox</strong> token only until the smoke-test in Phase 7 passes. Do not enter the live token yet.</li>
  <li>The legacy Python apps stay in place as a behavioural reference — don't remove them.</li>
</ul>

<h3>Phase 3 — UI verification</h3>
<p>Phase 3 (registering apps in SAM Central) is written from existing handoff docs. If SAM Central's UI labels differ in practice, follow the spirit of each step and send me a one-line note on the difference so I can update the doc.</p>

<h3>Questions?</h3>
<p>Reply to this thread for anything that isn't clear in the guide. There's no time pressure — go through it carefully and ask if you hit something.</p>

<p>Thanks,<br/>Harry</p>
```

- [ ] **Step 2: Show the user (Harry) the rendered body and the planned send call.**

In chat, paste the full HTML body shown above for Harry to review. Also show the planned API call:

```
Two API calls planned (one per recipient, since /api/email/send takes a single `to: str`):

  POST http://localhost:8000/api/email/send
  {
    "to": "charlieb@intsysuk.com",
    "subject": "SAM plugin deployment guide — ready to install",
    "body": "<html body from Step 1>",
    "from_email": "intsys@wimbledoncloud.net",
    "attachments": ["/Users/maccb/llmragsql/apps-sam/DEPLOY-TO-SAM.md"]
  }

Then the same payload with "to": "jonathang@intsysuk.com".
```

- [ ] **Step 3: Wait for Harry's explicit approval.**

Ask: **"Approve the body, or change anything?"**

If Harry requests changes, edit and re-show. Only proceed to Task 9 when Harry says "approved" or equivalent.

---

## Task 9: Send the email via /api/email/send

**Files:** (no file changes — external API call)

**What this task produces:** Two `POST /api/email/send` calls (one per recipient) using Harry's approved body. Two successful 200 responses.

**Prerequisite:** Harry's approval from Task 8 Step 3.

- [ ] **Step 1: Confirm the FastAPI server is running.**

```bash
curl -s http://localhost:8000/api/health 2>&1 | head -5 || echo "API not responding"
```

**✓ Looks good if you see:** a JSON response or any non-empty output.

**✗ If you see `API not responding`** — start the server:

```bash
cd /Users/maccb/llmragsql
source venv/bin/activate
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000 &
sleep 3
curl -s http://localhost:8000/api/health
```

- [ ] **Step 2: Send to Charlie.**

```bash
cd /Users/maccb/llmragsql
curl -X POST http://localhost:8000/api/email/send \
  -H "Content-Type: application/json" \
  -d @- <<'EOF'
{
  "to": "charlieb@intsysuk.com",
  "subject": "SAM plugin deployment guide — ready to install",
  "from_email": "intsys@wimbledoncloud.net",
  "body": "<HTML BODY APPROVED BY HARRY IN TASK 8>",
  "attachments": ["/Users/maccb/llmragsql/apps-sam/DEPLOY-TO-SAM.md"]
}
EOF
```

(Substitute the actual HTML body — escape any embedded double-quotes for JSON validity, or write the JSON to a file first and use `-d @file.json`.)

**✓ Looks good if you see:** `{"success": true, "message_id": "..."}` or similar.

**✗ If you see `503 Email storage not initialized`** — the email provider isn't configured on this instance. Tell Harry; the email can be sent manually from his mail client using the body from Task 8 instead.

**✗ If you see `400 No enabled email provider`** — same as above — manual fallback.

- [ ] **Step 3: Send to Jonathan.**

Repeat Step 2 with `"to": "jonathang@intsysuk.com"`.

- [ ] **Step 4: Report success to Harry.**

In chat, tell Harry:

> "Email sent to charlieb@intsysuk.com (200 OK, message_id: X) and jonathang@intsysuk.com (200 OK, message_id: Y). DEPLOY-TO-SAM.md is on origin/main. Ready for them to follow."

If either send failed, report the exact error and the fact that the email may need to go manually from Harry's mail client.

---

## Self-review

### Spec coverage
- ✅ Goal — covered by Tasks 1–4 (writing the doc).
- ✅ "Replace the three" — Task 5 (refs) + Task 6 (deletion).
- ✅ Phase 0.1 idiot's guide — Task 1 Step 2.
- ✅ 8 phases — Tasks 1–4.
- ✅ Format conventions — enforced via verification steps in each task.
- ✅ Deliverables 1–5 — Tasks 1–6.
- ✅ Deliverable 6 (push) — Task 7.
- ✅ Deliverable 7 (email) — Tasks 8–9.
- ✅ Acceptance criteria — Task 7 Steps 1–4.

### Placeholder scan
- No "TBD"/"TODO"/"implement later" in any task.
- Every command has explicit args and an expected-output assertion.
- The HTML email body in Task 8 is fully drafted, not "draft a body".

### Type consistency
- File paths used consistently throughout (`apps-sam/DEPLOY-TO-SAM.md`).
- Phase numbering 0–7 consistent.
- Working directory `/Users/maccb/llmragsql/` referenced consistently.
- Two recipients consistently named throughout.

No issues found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-sam-deployment-doc.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
