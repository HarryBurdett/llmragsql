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
| [6](#phase-6--migrate-existing-data-from-python-apps) | Migrate existing data | Charlie | 15 min |
| [7](#phase-7--verify-everything-works) | Verify everything works | Both | 15 min |
| [∎](#-done--hand-back-to-harry) | Done — hand back to Harry | — | — |
| [Troubleshooting](#troubleshooting) | Common failure modes | reference | — |

---

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

## Phase 3 — Register in SAM Central

**What you're doing here:** SAM Central is the central registry that tells each SAM host instance which apps to install. You'll add four catalogue entries (one per plugin), pin each to `v1.0.0`, and assign them to the IntSys client's license. Charlie's SAM host will pick these up automatically when he triggers a sync in Phase 4.

> **⚠ UI verification note:** This phase is written from the existing handoff docs. The exact button labels and screen names in SAM Central may differ slightly from what's described here. If anything doesn't match, follow the spirit of each step — the data being entered is what matters. Send Harry a one-line update on any UI discrepancies so this doc can be corrected for next time.

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
