# Embedding the SAM plugins into AI-SAM

This guide turns the four plugins under `apps-sam/` from a workspace
inside the SQLRAG monorepo into **four independent SAM plugins**, each
living in its own GitHub repo and installable via SAM Central.

## What you'll have when you're done

Four independent GitHub repositories under your org (e.g.
`intsysuk/sam-bank-reconcile`, `intsysuk/sam-gocardless`,
`intsysuk/sam-suppliers`, `intsysuk/sam-balance-check`). Each:

- Built and ready to install on any AI-SAM host that knows its git URL
  and tag.
- Independent of the others — no shared workspace, no cross-repo build
  step.
- Tagged with semver releases (`v1.0.0`, `v1.0.1`, …) that SAM Central
  pins to client licenses.

## Pre-flight (one-off, do before any extraction)

Two code changes are needed before plugins are installable. Both are
small.

### 1. Rewire the email-ingest defaults

The current defaults activate when `ctx.config.mailboxes` is set. SAM
always passes `config: {}` (see `~/opera-knowledge-ref/packages/backend/src/plugins/loader.ts:407`),
so this is the wrong signal. The correct source is
`ctx.emailIngest.listMyMailboxes()` — every row in SAM's
`email_mailboxes` table where `owner_app_id === <yourAppId>`.

Affects:

- `apps-sam/bank-reconcile/src/services/default-email-ingest.ts`
- `apps-sam/gocardless/src/services/default-email-ingest.ts`
- `apps-sam/suppliers/src/services/default-email-ingest.ts`

Change: at adapter construction, replace the `options.mailboxes` loop
with:

```ts
const mailboxes = await options.emailIngest.listMyMailboxes();
for (const mb of mailboxes) {
  // mb.email or mb.emailAddress depending on SAM's row shape
  const claim = await options.emailIngest.claimMailbox({ mailboxEmail: mb.email });
  // ... existing handler register
}
```

### 2. Rewire the bank-statement folder default

`bank-reconcile`'s default `fileStorage` keys off
`ctx.config.bankStatementRoot`. Change it to read from the existing
`folder_settings` table (already migrated, already managed by the
plugin's own UI). Build the adapter lazily on first request after the
operator has saved a folder via the UI.

Affects:

- `apps-sam/bank-reconcile/src/router.ts` (the `builtinFileStorage`
  construction)

Both changes are unit-testable and won't take more than an hour.

## Decision: shared package — vendor or publish

The four plugins all import a few helpers from `@sqlrag/sam-shared`
(in `apps-sam/shared/`). When you split into independent repos, those
helpers have to come along. Two options:

| Option | Effort | Cost |
| --- | --- | --- |
| **Vendor**: copy the ~12 files in `apps-sam/shared/src/` into each plugin's `src/_shared/` | Low (one-off copy) | Drift risk if you change shared code later — must apply to all 4 |
| **Publish to GitHub Packages** (`@intsysuk/sam-shared`) | Medium (need npm token in each plugin's CI) | Single source of truth, semver-pinned |

**Recommendation: vendor.** With only 4 plugins and a small shared
surface, the drift cost is low. You can switch to publishing later if
the surface grows. The rest of this guide assumes vendoring.

## The extraction recipe

Worked example: extracting `balance-check` first. It's the smallest
(~32 tests, no email, read-only against Opera) and the safest to
shake down the SAM install pipeline with.

### Step 1 — Create the GitHub repo

In your GitHub org, create a new private repo: `sam-balance-check`.
Don't initialise it (no README, no .gitignore) — you'll push from
local.

### Step 2 — Stage the plugin in a clean working directory

```sh
mkdir -p ~/sam-plugins-staging
cd ~/sam-plugins-staging
mkdir sam-balance-check
cd sam-balance-check

# Copy plugin contents (NOT the apps-sam/ wrapper)
cp -R /Users/maccb/llmragsql/.claude/worktrees/admiring-borg-888ae1/apps-sam/balance-check/* .
cp /Users/maccb/llmragsql/.claude/worktrees/admiring-borg-888ae1/apps-sam/balance-check/.gitignore . 2>/dev/null || true

# Vendor the shared package into src/_shared/
mkdir -p src/_shared
cp -R /Users/maccb/llmragsql/.claude/worktrees/admiring-borg-888ae1/apps-sam/shared/src/* src/_shared/

# Remove the workspace symlink and lockfile
rm -rf node_modules package-lock.json frontend/node_modules frontend/package-lock.json dist frontend/dist
```

### Step 3 — Rewrite imports to use the vendored shared

Find every `from '@sqlrag/sam-shared'` and rewrite to a relative path
under `src/_shared/`:

```sh
# Macro: each plugin source file imports shared via the package name;
# rewrite to a relative import.
find src -type f -name "*.ts" -exec sed -i '' \
  "s|from '@sqlrag/sam-shared'|from './_shared'|g" {} +

find src -type f -name "*.ts" -exec sed -i '' \
  "s|from '@sqlrag/sam-shared/opera'|from './_shared/opera'|g" {} +

find src -type f -name "*.ts" -exec sed -i '' \
  "s|from '@sqlrag/sam-shared/posting'|from './_shared/posting'|g" {} +
```

(For nested files under `src/services/`, the relative path becomes
`../_shared'` — adjust as needed. A safer approach is
`from 'src/_shared'` with a tsconfig path alias.)

### Step 4 — Rewrite `package.json`

Replace the workspace dep and make `npm run build` produce both
backend and frontend dist artefacts at the locations SAM expects.

```json
{
  "name": "sam-balance-check",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "main": "dist/index.js",
  "scripts": {
    "build": "tsc -p tsconfig.json && cd frontend && npm install --no-audit --no-fund && npm run build",
    "test": "vitest run",
    "lint": "tsc --noEmit",
    "clean": "rm -rf dist frontend/dist"
  },
  "dependencies": {
    "express": "^4.19.2",
    "knex": "^3.1.0"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^20.11.0",
    "sqlite3": "^6.0.1",
    "typescript": "^5.4.0",
    "vitest": "^1.4.0"
  }
}
```

Key changes:

- Drop `"@sqlrag/sam-shared": "*"`
- Drop `"workspaces"` if it was inherited
- The `build` script does **everything**: backend `tsc` + frontend
  `npm install && npm run build`. SAM's git-installer runs only
  `npm ci && npm run build` at the repo root, so it needs to produce
  both `dist/index.js` (backend) and `frontend/dist/` (frontend) in
  one shot.

### Step 5 — Verify the build runs in isolation

```sh
npm install
npm test                       # 32 tests pass
npm run build
```

Should produce `dist/index.js`, `dist/router.js`, etc., and
`frontend/dist/index.js`, `frontend/dist/style.css`. If any test or
build step fails, fix it before pushing.

### Step 6 — Verify the manifest

Open `manifest.json`. Confirm:

- `id` is `balance-check` (matches what SAM Central will register)
- `version` matches your `package.json` version (`1.0.0`)
- `backend.requiresDatabase` is correct (`false` for balance-check —
  no per-app DB)
- `consumes` only declares what the plugin actually needs (no
  `email-ingest` or `llm` for balance-check)

### Step 7 — Push to GitHub

```sh
git init
git add -A
git commit -m "Initial commit — sam-balance-check v1.0.0"
git branch -M main
git remote add origin git@github.com:intsysuk/sam-balance-check.git
git push -u origin main

git tag v1.0.0
git push --tags
```

### Step 8 — Register with SAM Central

In SAM Central admin:

1. **Apps catalogue** → Add new app (or edit existing `balance-check`):
   - `app_id`: `balance-check`
   - `git_url`: `https://github.com/intsysuk/sam-balance-check.git`
   - `default_version`: `v1.0.0`
2. **Client licence** → assign `balance-check@v1.0.0` to your client.
3. **Confirm SAM Central PAT has read access** to the new repo (the
   PAT was set up at SAM deploy time per
   `~/opera-knowledge-ref/DEPLOYMENT-GUIDE.md`).

### Step 9 — Trigger install on your SAM host

The next license check (every ~5 min, or trigger via SAM Admin →
Apps → Sync now) makes the host phone home, get the assignment,
clone the repo, run `npm ci && npm run build`, and mount the plugin.

Watch the SAM backend logs:

```sh
docker logs -f ai-sam | grep -E "GitInstall|PluginLoader"
```

You're looking for:

```
[GitInstall] Cloning https://github.com/intsysuk/sam-balance-check.git...
[GitInstall] Running npm ci...
[GitInstall] Running npm run build...
[GitInstall] Success: balance-check @ <sha>
[PluginLoader] Loaded: balance-check@1.0.0
```

If any line errors, the plugin doesn't load. Common causes:

| Error | Fix |
| --- | --- |
| `npm ci` fails: lockfile out of sync | Re-generate `package-lock.json` locally with `npm install`, commit, retag |
| `tsc: not found` | The git-installer uses `--include=dev` so devDeps install; if you see this, your `package.json` is missing `typescript` in `devDependencies` |
| `vite: not found` | `frontend/package.json` is missing the build deps. Make sure `npm install` ran inside `frontend/` (Step 4's build script does this) |
| `Manifest id "X" does not match expected appId "balance-check"` | The `manifest.json` `id` field doesn't match what's registered in Central |

### Step 10 — Smoke-test the plugin

In SAM portal, navigate to Balance Check. The 4 tabs should render
(Creditors, Debtors, Trial Balance, Cashbook). Each tab fires a
read-only query against Opera SE; you should see numbers within a
few seconds.

If the page loads but data is missing:

- Open browser devtools → Network tab → look at the response of
  `GET /api/apps/balance-check/api/reconcile/creditors`. Status code
  and JSON tell you which side is failing.
- 401 → SAM auth middleware rejected the request. Should not happen;
  check that `process.env.SAM_PLUGIN_MODE === 'true'` is being read
  (SAM sets it automatically).
- 503 → ctx adapter not configured. The default adapter activation
  is wrong; check the loader logs for `[GoCardless email-ingest]
  claimed ...` style messages.
- 500 → Opera query failed. The error JSON has the SQL — copy and
  run it directly against Opera SE to isolate.

## Replicate for the other 3 plugins

The recipe is identical for `bank-reconcile`, `gocardless`, and
`suppliers`. Differences:

### bank-reconcile

- **Has migrations** (`db/migrations/` with 12 files). SAM auto-runs
  these because `manifest.backend.separateDatabase = true`.
- **Uses `ctx.llm`**: the GoCardless API key isn't relevant; needs
  Microsoft Graph configured in SAM Admin → Email Settings for the
  PDF extractor to work. Bank-statement attachment PDFs flow through
  Graph, then through `ctx.llm` (Claude vision) for extraction.
- **Largest frontend** (~5,400 LOC port). Build time is longer
  (~10 s).
- **Needs `vendoring shared`** (uses `sequenceMatcherRatio` and a few
  other helpers).

### gocardless

- **Has migrations** (7 files).
- **Uses `ctx.llm`** for OCR endpoints (`/api/gocardless/parse`,
  `/api/gocardless/ocr`).
- **GoCardless API key** is configured per-tenant via the plugin's
  own Settings page. No SAM-level config needed.
- ⚠️ **Sandbox only**. As per the existing project memory, do NOT
  configure the live GoCardless key in the test environment. Use a
  GoCardless sandbox account.

### suppliers

- **Has migrations** (4 files).
- **Uses `ctx.llm`** for statement extraction.
- **Uses `ctx.email`** for sending response emails to suppliers.
- Simplest setup of the three with email — one mailbox claim, one
  set of credentials.

## SAM Central registration — common steps for all 4

For each plugin, in SAM Central:

1. Add an entry to the apps catalogue with:
   - `app_id` matching the plugin's `manifest.json:id`
   - `git_url` pointing at the GitHub repo
2. Pin a version (`default_version: v1.0.0`)
3. On the client license, assign all 4 apps with their default versions

When you cut a new release of a plugin (`v1.0.1`), update the version
in Central and trigger sync. SAM hot-reloads the plugin in-process —
no host restart needed.

## Per-plugin post-install configuration

After the install lights up, the operator needs to configure each
plugin from inside SAM (no further code changes).

### bank-reconcile

1. **SAM Admin → Email Mailboxes**: set `owner_app_id` of the
   bank-statement mailbox(es) to `bank-reconcile`. The plugin will
   pick them up via `listMyMailboxes()` on next request.
2. **bank-reconcile Settings page** (in the SAM portal, after you
   open the plugin): if you watch a local PDF folder in addition to
   email, set the path here. The plugin's `folder_settings` table
   stores it.
3. **bank-reconcile Match Config**: tweak fuzzy match thresholds
   if defaults don't suit. Optional.

### gocardless

1. **SAM Admin → Email Mailboxes**: assign the GoCardless payout
   mailbox to `gocardless`.
2. **gocardless Settings page**:
   - `environment`: `sandbox` (NEVER `live` until you're ready)
   - `access_token`: paste the GoCardless API token
   - `company_reference`: your GoCardless company ID
   - `default_batch_type`: e.g. `BC` for the default cashbook entry
     type when posting receipts
3. Click "Test connection" to verify.

### suppliers

1. **SAM Admin → Email Mailboxes**: assign the supplier-statements
   mailbox to `suppliers`.
2. **suppliers Settings page**: configure response email templates,
   automation rules, supplier-specific overrides.

### balance-check

Nothing to configure. Open the page and it queries Opera.

## What "done" looks like

For each of the four plugins:

- ✓ Lives in its own GitHub repo
- ✓ Has at least one tagged release (`v1.0.0`)
- ✓ Builds cleanly via `npm ci && npm run build` at the repo root
- ✓ Installs cleanly on your SAM host (no errors in `[GitInstall]` /
  `[PluginLoader]` logs)
- ✓ The plugin's nav tile appears in the SAM portal
- ✓ Opening the plugin renders the React UI
- ✓ At least one read-only endpoint returns sane data from your live
  Opera SE

When all four reach that bar, the rewrite is **embedded**. You're
free to retire the corresponding Python apps under `apps/` from the
client install.

## Rollback

If a plugin install fails partway through and SAM's automatic
rollback doesn't trigger:

```sh
# On the SAM host:
docker exec -it ai-sam node -e "require('./packages/backend/dist/licensing/git-installer.js').rollback('balance-check')"
```

Or remove the plugin assignment from Central and re-trigger sync —
SAM will mark it as "not installed" and remove the bind.

For your two test datasets specifically: since this is pre-production
and the only data is internal, you can also reset the per-app DBs
manually:

```sql
-- On SAM's MSSQL:
DROP DATABASE ai_sam_app_bank_reconcile;
DROP DATABASE ai_sam_app_gocardless;
DROP DATABASE ai_sam_app_suppliers;
-- balance-check has no per-app DB
```

The next install recreates them from scratch via the migrations.

---

## Quick reference: one-line summary per plugin

| Plugin | GitHub repo | Has migrations | Needs Graph | Needs LLM | Per-tenant config |
| --- | --- | --- | --- | --- | --- |
| balance-check | `intsysuk/sam-balance-check` | no | no | no | none |
| bank-reconcile | `intsysuk/sam-bank-reconcile` | yes | yes | yes | mailbox assignment + folder path |
| gocardless | `intsysuk/sam-gocardless` | yes | yes | optional | mailbox + GoCardless API key |
| suppliers | `intsysuk/sam-suppliers` | yes | yes | yes | mailbox assignment |

**Recommended order:** balance-check → bank-reconcile → gocardless →
suppliers. Smallest first, then the one with the deepest write path,
then the API-integration ones.
