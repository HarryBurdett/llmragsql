# Local SAM Test Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a standalone local SAM on Harry's Mac with the four plugins installed and validated against live Intsys Opera SE; produce proven `.sap` artifacts for Jonathan; document the maintenance loop; create a one-command SAM-platform-update mechanism.

**Architecture:** Local Docker stack (nginx + ai-sam + db + redis) on Harry's Mac, standalone mode (no SAM Central phone-home), connected to live production Opera SE and live mailbox. Four plugins built as `.sap` packages and uploaded directly via the admin UI. SAM source tree at `~/opera-knowledge-ref/` is the upstream; updates flow via `git pull && docker compose build`.

**Tech Stack:** Docker Desktop, SAM (from `~/opera-knowledge-ref/`), `markdown-it-py` (already in venv, for HTML doc rendering), the four plugins in `apps-sam/`, the project's `/api/email/send` endpoint.

**Working directory for all tasks:** `/Users/maccb/llmragsql/` (the SQLRAG monorepo on `main`).

**Per-task verification:** each task has its own `✓ Looks good if you see` and `✗ If you see X` markers, matching the convention used in `apps-sam/DEPLOY-TO-SAM.md` and `apps-sam/MAINTAIN-SAM-PLUGINS.md`.

---

## File Structure

**Files created (4):**

- `apps-sam/scripts/update-local-sam.sh` — wrapper script for `update test sam to latest version` trigger
- `apps-sam/scripts/build-sap.sh` — small helper that calls SAM's `POST /admin/apps/:appId/build/create` for one plugin (used by the upload tasks and the "update" script)
- `docs/sam-rewrite/jonathan-pause-message.md` — drafted (not sent) message asking Jonathan to hold the existing deployment
- `~/.local/sam-test/` — directory holding the standalone SAM `.env` and any other config (not inside the SQLRAG monorepo — keeps SAM config separate from app code)

**Files modified (3):**

- `apps-sam/MAINTAIN-SAM-PLUGINS.md` — Section 1 release flow gains a "validate in local SAM" step; new Section 1.7 covers updating the SAM platform
- `apps-sam/README.md` — gain a one-line reference to the local SAM test environment
- `/Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/MEMORY.md` and `feedback_local_sam_trigger_phrase.md` — save the "update test sam to latest version" trigger-phrase rule

**Files NOT modified:**

- `~/opera-knowledge-ref/` — read-only from our perspective; we never edit SAM's source, just run + pull
- Plugin source files in `apps-sam/<plugin>/` — only edited if the iteration loop finds bugs during validation

---

## Task 1: Pre-flight verification

**Files:** none (verification only)

**What this task produces:** Confidence that all prerequisites are met before we start the install.

- [ ] **Step 1: Verify Docker Desktop is installed and running.**

```bash
docker --version
docker compose version
docker ps
```

**✓ Looks good if you see:** Docker version 24+, Compose v2+, and `docker ps` returns successfully (even with empty list).

**✗ If `docker` not found** — install Docker Desktop for Mac from <https://www.docker.com/products/docker-desktop>. Restart terminal after install.

**✗ If `docker ps` errors with "Cannot connect"** — Docker Desktop is installed but not running. Launch the Docker Desktop app, wait ~30 seconds, retry.

- [ ] **Step 2: Verify SAM source repo is current.**

```bash
cd ~/opera-knowledge-ref
git pull
git log -1 --format="%H %ar %s"
```

**✓ Looks good if you see:** the latest commit pulled cleanly, dated recently (within Jonathan's normal release cadence).

**✗ If `git pull` errors with auth issue** — your GitHub credentials don't have access to the repo. Run `gh auth status` to check, or run `gh auth refresh -s repo`.

- [ ] **Step 3: Check disk space and RAM.**

```bash
df -h /
vm_stat | head -5
```

**✓ Looks good if you see:** ≥ 10 GB free on `/`. RAM availability depends on what else you have open — close any non-essential apps if you're tight (SAM containers will use ~2-3 GB).

- [ ] **Step 4: Confirm Opera SE credentials are available.**

You'll need the live Intsys Opera SE connection details for the SAM setup wizard later:

- Host or IP of the MSSQL Server
- Port (usually 1433)
- Username + password
- Database name (Opera SE company)

Confirm you have these — they're typically in `/Users/maccb/llmragsql/config.ini` (gitignored). If you don't have them ready, stop here and find them before starting Task 2.

- [ ] **Step 5: Confirm Microsoft Graph / IMAP credentials for `intsys@wimbledoncloud.net` are available.**

Same as Step 4 but for the mailbox — you'll need these for the SAM setup wizard.

- [ ] **Step 6: Verify working directory and main branch.**

```bash
cd /Users/maccb/llmragsql
git branch --show-current
git status -sb
```

**✓ Looks good if you see:** `main` and working tree clean (or only `.claude/` untracked).

---

## Task 2: Configure standalone SAM environment

**Files:**
- Create: `~/.local/sam-test/.env`

**What this task produces:** A complete `.env` file for SAM with all required values set and license vars left blank (standalone mode).

- [ ] **Step 1: Create the SAM test config directory.**

```bash
mkdir -p ~/.local/sam-test
```

- [ ] **Step 2: Copy SAM's `.env.example` as the starting point.**

```bash
cp ~/opera-knowledge-ref/.env.example ~/.local/sam-test/.env
```

- [ ] **Step 3: Edit `~/.local/sam-test/.env` and set the required values.**

Edit the file to look like this (substitute Harry's chosen passwords for `DB_PASSWORD` and the JWT secret):

```ini
# AI-SAM Environment Configuration (LOCAL TEST INSTANCE — STANDALONE)
NODE_ENV=development
PORT=3001

# JWT — use a long random string for the secret
JWT_SECRET=<generate a long random string, e.g. via `openssl rand -hex 32`>
JWT_EXPIRES_IN=8h

# Database (MSSQL — runs in a container, password Harry chooses)
DB_HOST=db
DB_PORT=1433
DB_USER=sa
DB_PASSWORD=<strong password Harry chooses, save it somewhere safe>
DB_NAME=ai_sam

# CORS
CORS_ORIGIN=http://localhost:5173

# App Storage
SAM_DATA_DIR=/data/sam

# License — INTENTIONALLY EMPTY — STANDALONE MODE
LICENSE_SERVER_URL=
LICENSE_HMAC_SECRET=
LICENSE_KEY=
```

**Why we leave the license fields empty:** SAM treats blank `LICENSE_SERVER_URL` as "no Central, run in dev/standalone mode". See `.env.example` comment: *"License (optional - leave empty for dev mode)"*.

- [ ] **Step 4: Verify the file is created and not accidentally committed to git.**

```bash
ls -la ~/.local/sam-test/.env
cd /Users/maccb/llmragsql
git status apps-sam/ docs/
```

**✓ Looks good if you see:** the `.env` exists at `~/.local/sam-test/.env` and `git status` doesn't mention it (it's outside the repo, so it's not in git's view).

The `.env` lives outside the SQLRAG monorepo intentionally — secrets stay out of git.

---

## Task 3: Build and start local SAM

**Files:**
- Create: `~/.local/sam-test/docker-compose.yml` (symlinked or copied from SAM's)

**What this task produces:** SAM running in Docker on this Mac, reachable at `http://localhost:3001`.

- [ ] **Step 1: Copy SAM's docker-compose.yml into the test config directory and point it at our `.env`.**

```bash
cp ~/opera-knowledge-ref/docker-compose.yml ~/.local/sam-test/docker-compose.yml
```

Then edit `~/.local/sam-test/docker-compose.yml` so the `build:` context points back at the SAM source tree:

```yaml
services:
  ai-sam:
    build:
      context: /Users/maccb/opera-knowledge-ref
      target: production
    ports:
      - '3001:3001'
    env_file:
      - .env
    # ... rest unchanged
```

The key change is `build.context` — without an absolute path, Docker would look for the SAM source relative to `~/.local/sam-test/`, which doesn't exist there.

- [ ] **Step 2: Build the SAM Docker image.**

```bash
cd ~/.local/sam-test
docker compose build
```

**✓ Looks good if you see:** Docker walks through the build steps (downloading layers, copying source, running `npm install`, running `npm run build`, ending with `Successfully tagged` lines). Takes 3-10 minutes the first time.

**✗ If build errors with `npm ERR!`** — likely a missing dependency or version mismatch. Inspect the SAM source for any recent changes that might need a fresh `npm install`. Run `git log -10` in `~/opera-knowledge-ref/` to see what's recent.

- [ ] **Step 3: Start SAM.**

```bash
cd ~/.local/sam-test
docker compose up -d
```

**✓ Looks good if you see:** `Container ... Started` for `db` (MSSQL) and `ai-sam` (and any other services per compose file).

- [ ] **Step 4: Wait for SAM to be healthy and reachable.**

```bash
sleep 30
docker compose ps
curl -s http://localhost:3001/api/health | head -5
```

**✓ Looks good if you see:** all containers `running` (or `healthy` if they declare healthchecks); the curl returns a JSON response or any HTTP-200 reply.

**✗ If `curl` times out** — SAM is still starting. Wait another 30 seconds and retry. Migrations can take a minute on first boot.

**✗ If `docker compose ps` shows `db` unhealthy** — MSSQL is having trouble. `docker logs $(docker compose ps -q db)` shows the cause (often password complexity rules — MSSQL requires uppercase + lowercase + digit + symbol, 8+ chars).

- [ ] **Step 5: Browse to the SAM admin UI.**

```bash
open http://localhost:3001
```

**✓ Looks good if you see:** a SAM Admin or setup wizard page loads in the browser. If it's the setup wizard, that's Task 4. If it's a login page, you'll need to set the initial admin password — likely prompted on first login.

---

## Task 4: Run SAM setup wizard

**Files:** none (interactive setup via browser)

**What this task produces:** SAM configured with admin user, Opera SE connection, and email provider — ready to receive plugin uploads.

- [ ] **Step 1: Open the SAM Admin UI and complete first-time setup.**

In the browser at `http://localhost:3001`:

1. **Admin password** — set a strong password for `admin@intsysuk.com` (the default admin email). Save this somewhere safe.
2. **Tenant info** — the wizard may prompt for a tenant name. Use something descriptive like `IntSys-Test`.
3. **Save and proceed.**

**✓ Looks good if you see:** you're logged in as `admin@intsysuk.com`, with the SAM Admin dashboard visible.

- [ ] **Step 2: Configure the Opera SE connection.**

In SAM Admin → **Opera Connections** → **+ Add Connection**:

| Field | Value |
|---|---|
| Type | Opera SE (SQL Server) |
| Host | (the MSSQL host from Task 1 Step 4) |
| Port | 1433 (or whatever Intsys uses) |
| Username | (Opera SE username) |
| Password | (Opera SE password) |

Save. SAM auto-discovers companies from Opera's `seqco` system table.

**✓ Looks good if you see:** `Intsys` and `CloudSiS` both appear in the company list after save.

**✗ If you see `Cannot connect`** — host/port/credentials wrong, or the network from Docker to Opera is blocked. Test with `docker compose exec ai-sam ping <opera-host>` to check reachability.

- [ ] **Step 3: Configure the Microsoft Graph / IMAP email provider.**

In SAM Admin → **Email Settings**:

Configure the provider for `intsys@wimbledoncloud.net`. The exact wizard fields depend on whether you're using Microsoft Graph (OAuth) or IMAP. Pick the same option that the legacy Python uses (whichever is in `/Users/maccb/llmragsql/config.ini`).

**✓ Looks good if you see:** status `Active` and a recent successful sync timestamp.

- [ ] **Step 4: Confirm SAM is in standalone mode (no Central).**

```bash
docker logs $(cd ~/.local/sam-test && docker compose ps -q ai-sam) | grep -i "central\|license"
```

**✓ Looks good if you see:** messages like `License server not configured — running in standalone mode` (or similar). No outbound calls to a license server.

**✗ If you see** `License validation failed: <something>` — your `.env` license vars may not actually be empty. Recheck `~/.local/sam-test/.env`.

---

## Task 5: Investigate the `.sap` build and upload mechanism

**Files:** none (investigation; conclusions baked into Tasks 6-7)

**What this task produces:** Concrete understanding of how to package a plugin as a `.sap` file and how to upload it. Output: a confirmed approach we'll use in Task 6 onward.

- [ ] **Step 1: Read the SAM `.sap` build function.**

```bash
cat ~/opera-knowledge-ref/packages/backend/src/services/scanner.ts | sed -n '200,280p'
```

Read the `buildSapFromProject` function signature and behaviour. Identify:

- Inputs: `projectPath`, `selectedPaths`, `manifest`
- Output: a buffer (the `.sap` binary)
- Format: likely a zip with `manifest.json` + bundled JS + frontend dist

Note the exact format so we know what to ship.

- [ ] **Step 2: Find the upload endpoint and confirm auth requirements.**

```bash
grep -B 2 -A 15 "router\.post.*\/upload" ~/opera-knowledge-ref/packages/backend/src/routes/admin/apps.ts
```

Identify the endpoint URL and what auth header it expects (likely a JWT from the admin login). Note both.

- [ ] **Step 3: Get an admin JWT token by logging in via API.**

```bash
curl -s -X POST http://localhost:3001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@intsysuk.com","password":"<the admin password you set in Task 4>"}'
```

**✓ Looks good if you see:** a JSON response with a `token` field (a JWT). Save the token for use in Tasks 6+.

```bash
TOKEN=$(curl -s -X POST http://localhost:3001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@intsysuk.com","password":"<admin-password>"}' | jq -r '.token')
echo "TOKEN length: ${#TOKEN}"
```

**✓ Looks good if you see:** TOKEN length around 200-300 characters.

- [ ] **Step 4: Choose the build approach based on Step 1's findings.**

Two options:

- **Approach A — use SAM's build endpoint** (`POST /admin/apps/:appId/build/create`): pass the `projectPath` (e.g. `/Users/maccb/llmragsql/apps-sam/balance-check`) and SAM builds the `.sap` server-side. Requires the SAM container to have read access to the host filesystem (volume mount).
- **Approach B — build `.sap` manually** as a zip with the format identified in Step 1. Uses standard `zip` command. No server-side build needed.

Pick whichever Step 1 makes simpler. Document the choice in the commit message.

- [ ] **Step 5: Commit findings.**

```bash
cd /Users/maccb/llmragsql
git status
# Nothing changes in this task — investigation only. No commit needed.
echo "Build approach chosen: <A or B per Step 4>"
```

---

## Task 6: Build and upload `balance-check` (first plugin)

**Files:**
- Create: `apps-sam/scripts/build-sap.sh` — wrapper script using the chosen approach from Task 5

**What this task produces:** A working `.sap` of `balance-check` installed in local SAM. Proves the build + upload pipeline end-to-end.

- [ ] **Step 1: Write the build-sap.sh helper.**

Based on Task 5's chosen approach, write a wrapper that takes a plugin name and produces a `.sap` ready to upload.

**If Approach A (SAM build endpoint):**

```bash
#!/usr/bin/env bash
# apps-sam/scripts/build-sap.sh — build a .sap package via SAM's build endpoint
# Usage: ./build-sap.sh <plugin-name> [token]
set -euo pipefail

PLUGIN="${1:?Usage: build-sap.sh <plugin-name> [token]}"
TOKEN="${2:-$SAM_TOKEN}"
SAM_URL="${SAM_URL:-http://localhost:3001}"
PROJECT_PATH="/Users/maccb/llmragsql/apps-sam/${PLUGIN}"
MANIFEST_PATH="${PROJECT_PATH}/manifest.json"

if [[ ! -f "$MANIFEST_PATH" ]]; then
  echo "No manifest at $MANIFEST_PATH" >&2; exit 1
fi

MANIFEST=$(cat "$MANIFEST_PATH")
VERSION=$(echo "$MANIFEST" | jq -r '.version')

# Call SAM's build endpoint
curl -s -X POST "$SAM_URL/api/admin/apps/$PLUGIN/build/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(jq -n --arg pp "$PROJECT_PATH" \
                --argjson m "$MANIFEST" \
                --arg v "$VERSION" \
                '{projectPath: $pp, selectedPaths: ["src","db","manifest.json","package.json","frontend/dist"], manifest: $m, version: $v}')" \
  -o "/tmp/${PLUGIN}-v${VERSION}.sap"

echo "Built /tmp/${PLUGIN}-v${VERSION}.sap"
```

**If Approach B (manual zip):** the script `zip`s the relevant folders and renames to `.sap`. Format details come from Task 5 Step 1.

Make it executable: `chmod +x apps-sam/scripts/build-sap.sh`.

- [ ] **Step 2: Run the build for `balance-check`.**

```bash
cd /Users/maccb/llmragsql
export SAM_TOKEN="<the JWT from Task 5 Step 3>"
./apps-sam/scripts/build-sap.sh balance-check
```

**✓ Looks good if you see:** `/tmp/balance-check-v1.0.0.sap` (or whatever the version is) appears, file size in the tens of KB to low MB.

**✗ If build fails with `Cannot read manifest`** — `manifest.json` path is wrong; check `apps-sam/balance-check/manifest.json` exists.

**✗ If build fails with auth error** — `SAM_TOKEN` is stale or invalid. Re-run Task 5 Step 3 to refresh.

- [ ] **Step 3: Upload the .sap to local SAM via the admin endpoint.**

```bash
curl -s -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/balance-check-v1.0.0.sap"
```

**✓ Looks good if you see:** a JSON response with `success: true` and the plugin marked as installed.

**✗ If `400 File must be a .sap package`** — the file extension is wrong, or the build didn't produce a valid `.sap`. Check Step 2's output.

**✗ If `401 Unauthorized`** — token expired. Refresh via Task 5 Step 3.

- [ ] **Step 4: Confirm the plugin appears in SAM Admin.**

```bash
open http://localhost:3001
```

In the browser: SAM Admin → Apps. Confirm `balance-check` appears as Installed.

**✓ Looks good if you see:** `balance-check` listed with status `Installed` or `Loaded`.

- [ ] **Step 5: Quick smoke test — open the plugin's UI.**

In the browser, click into `balance-check`. The plugin's frontend should load.

**✓ Looks good if you see:** the balance-check UI loads (Cashbook reconcile, Debtors, Creditors, VAT pages). Don't worry about whether data populates yet — that depends on the Opera connection (Task 4 step 2). Just confirm the JS loads and the page doesn't 500.

**✗ If you see `503 — plugin not loaded`** — `docker logs ai-sam | grep balance-check` to find the loader error.

- [ ] **Step 6: Commit the script.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/scripts/build-sap.sh
git commit -m "$(cat <<'EOF'
feat(scripts): build-sap.sh — package a plugin as .sap for local SAM

Used during the local-SAM iteration loop: edit code, rebuild .sap,
re-upload, retest. Wraps SAM's build endpoint (or manual zip per
Task 5's chosen approach).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

(Use `git add -f` if `apps-sam/scripts/` is gitignored — same workaround as for `render-md-to-html.py`.)

---

## Task 7: Build and upload the other three plugins

**Files:** none (uses Task 6's `build-sap.sh`)

**What this task produces:** `bank-reconcile`, `gocardless`, `suppliers` all installed in local SAM.

- [ ] **Step 1: Build and upload `bank-reconcile`.**

```bash
cd /Users/maccb/llmragsql
./apps-sam/scripts/build-sap.sh bank-reconcile
curl -s -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/bank-reconcile-v1.0.0.sap"
```

**✓ Looks good if you see:** `success: true` for both build and upload, and the plugin appears in SAM Admin → Apps.

**✗ If upload fails with `manifest validation`** — the plugin's `manifest.json` may need updating. Read the exact error to understand what SAM expects.

- [ ] **Step 2: Build and upload `gocardless`.**

```bash
cd /Users/maccb/llmragsql
./apps-sam/scripts/build-sap.sh gocardless
curl -s -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/gocardless-v1.0.0.sap"
```

**✓ Looks good if you see:** same as Step 1.

- [ ] **Step 3: Build and upload `suppliers`.**

```bash
cd /Users/maccb/llmragsql
./apps-sam/scripts/build-sap.sh suppliers
curl -s -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/suppliers-v1.0.0.sap"
```

**✓ Looks good if you see:** same as Step 1.

- [ ] **Step 4: Confirm all four plugins are installed.**

```bash
curl -s -H "Authorization: Bearer $SAM_TOKEN" http://localhost:3001/api/admin/apps | jq '.[] | .id + " " + .version + " " + .status'
```

**✓ Looks good if you see:** four lines, one per plugin, all with status `Installed` (or `Loaded`).

- [ ] **Step 5: Assign mailbox `owner_app_id` for the three plugins that consume email.**

In SAM Admin → Email Mailboxes — for the `intsys@wimbledoncloud.net` mailbox (or each relevant inbox if there are multiple), assign:

- bank-reconcile (for bank statement emails)
- gocardless (for GoCardless payout emails)
- suppliers (for supplier statement emails)

If only one mailbox exists and all three plugins need it, three separate assignments are needed (or the docs/UI might support multi-owner — read the wizard prompts).

**✓ Looks good if you see:** the mailbox shows the three apps as owners.

---

## Task 8: Migrate legacy Python app data into local SAM

**Files:** none new (uses existing `apps-sam/scripts/migrate-from-python/migrate.ts`)

**What this task produces:** The accumulated learned data from months of legacy Python app use (bank aliases, GoCardless mandates, supplier statements, etc.) migrated from SQLite files at `/Users/maccb/llmragsql/data/<company>/` into the per-app MSSQL databases that local SAM created when the plugins installed.

**Why this matters:** the plugins were ported faithfully but they start with empty per-app databases. Without migrating the legacy state, smoke tests in Task 9 will see a "clean room" environment that doesn't reflect real usage. Migrating gives us realistic testing conditions and validates the migration tool itself before we use it for the eventual live SAM cutover.

**What's migrated, per the migration tool's header docstring:**

| Plugin | Source files | Target tables |
|---|---|---|
| bank-reconcile | `data/<company>/bank_reconcile/bank_aliases.db`, `bank_patterns.db`, `deferred_transactions.db` | `bank_import_aliases`, `repeat_entry_aliases`, `match_config`, `duplicate_overrides`, `bank_import_patterns`, `deferred_transactions` |
| gocardless | `data/<company>/gocardless/gocardless_payments.db`, `gocardless_settings.json` | `gocardless_mandates`, `gocardless_payment_requests`, `gocardless_subscriptions`, `gocardless_subscription_documents`, `gocardless_partner_signups`, `mandate_setup_requests`, `settings` |
| suppliers | `data/<company>/suppliers/supplier_statements.db` | `supplier_statements` + supporting tables |
| balance-check | (none — read-only plugin, nothing to migrate) | — |

Each combination of company × plugin is a separate migration run.

- [ ] **Step 1: Install migration tool dependencies.**

```bash
cd /Users/maccb/llmragsql/apps-sam/scripts/migrate-from-python
npm install
```

**✓ Looks good if you see:** `node_modules` populated, `tsx` available in `node_modules/.bin/`.

- [ ] **Step 2: Identify the per-app database names SAM created.**

When each plugin installed in Tasks 6-7, SAM created a per-app MSSQL database for it. The migration tool needs the exact database name as `--target-db`.

```bash
# Terminal — your Mac
# Query the SAM MSSQL container directly to list databases
docker exec -it $(cd ~/.local/sam-test && docker compose ps -q db) \
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$DB_PASSWORD" -C \
  -Q "SELECT name FROM sys.databases WHERE name LIKE 'ai_sam_app%' OR name LIKE '%bank%' OR name LIKE '%gocardless%' OR name LIKE '%suppliers%';"
```

**✓ Looks good if you see:** three database names returned, one per plugin that has data to migrate. Likely candidates:
- `ai_sam_app_bank_reconcile` (or similar)
- `ai_sam_app_gocardless`
- `ai_sam_app_suppliers`

**Note the exact names.** They'll be used in Step 4 onward as `--target-db` values.

**✗ If no matching databases appear** — the plugins may not have completed their migrations on install. Check `docker logs ai-sam | grep migration` for any errors. Don't proceed until all three per-app DBs exist.

- [ ] **Step 3: Dry-run migration for each company × plugin combination.**

There are six combinations: `intsys` and `cloudsis` × `bank-reconcile`, `gocardless`, `suppliers`. Run dry-run for all six first so we see expected row counts before writing anything:

```bash
cd /Users/maccb/llmragsql/apps-sam/scripts/migrate-from-python
DB_PASSWORD=<the SAM admin DB_PASSWORD from .env>

for company in intsys cloudsis; do
  for plugin in bank-reconcile gocardless suppliers; do
    echo "=== $company × $plugin (DRY-RUN) ==="
    npx tsx migrate.ts \
      --company $company \
      --plugin $plugin \
      --data-root /Users/maccb/llmragsql/data \
      --target-host localhost --target-port 1433 \
      --target-user sa --target-password "$DB_PASSWORD" \
      --target-db ai_sam_app_$(echo $plugin | tr '-' '_') \
      --dry-run
  done
done
```

**✓ Looks good if you see** (approximate row counts per the existing MIGRATION.md notes — actual counts may differ on current data):

```
intsys × bank-reconcile:  ~161 aliases, ~95 patterns, ~10 deferred
intsys × gocardless:      ~39 mandates, ~27 payment_requests, ~78 subscriptions
intsys × suppliers:       ~9 supplier_statements
cloudsis × bank-reconcile: ~1 alias, ~7 patterns
cloudsis × gocardless:    ~35 mandates, ~3 payment_requests, ~66 subscriptions
cloudsis × suppliers:     ~5 supplier_statements
```

**✗ If any combination shows `0 rows ready` and you know there's data** — the source path is wrong, or the legacy file naming doesn't match what the tool expects. Check `ls /Users/maccb/llmragsql/data/$company/$plugin/` to see what's actually there.

**✗ If you see `Cannot find target database`** — the `--target-db` name is wrong. Re-check Step 2's database list and adjust the `target-db` template above.

- [ ] **Step 4: Run real migration for each combination.**

Once the dry-run counts look right, remove `--dry-run` and rerun:

```bash
cd /Users/maccb/llmragsql/apps-sam/scripts/migrate-from-python

for company in intsys cloudsis; do
  for plugin in bank-reconcile gocardless suppliers; do
    echo "=== $company × $plugin (REAL RUN) ==="
    npx tsx migrate.ts \
      --company $company \
      --plugin $plugin \
      --data-root /Users/maccb/llmragsql/data \
      --target-host localhost --target-port 1433 \
      --target-user sa --target-password "$DB_PASSWORD" \
      --target-db ai_sam_app_$(echo $plugin | tr '-' '_')
  done
done
```

**✓ Looks good if you see:** for each combination, `inserted: X rows` matching the dry-run count (or close to it — re-runs may show fewer if idempotent inserts skip duplicates).

**✗ If you see `duplicate key` errors** — the script uses MERGE / idempotent inserts per its docstring, so duplicates should be skipped silently. If they're surfacing as errors, something about the schema doesn't match expectation; send the full error to investigate.

- [ ] **Step 5: Verify migrated data is visible in local SAM.**

Open the local SAM UI for each plugin and confirm the migrated data appears:

```
# Browser — Local SAM admin
```

- **bank-reconcile** → check the alias list / pattern list — should show the migrated entries
- **gocardless** → check the mandate list — should show the migrated mandates
- **suppliers** → check the supplier list / statement history — should show the migrated statements

**✓ Looks good if you see:** in each plugin's UI, you can see records that originated in the legacy Python data (recognise customer names, dates, etc.).

**✗ If a plugin's UI is empty despite the migration reporting success** — the data went into the wrong table or the plugin queries from a different table name than the migration target. Check `docker exec ... sqlcmd ...` to list tables and row counts in the per-app DB.

- [ ] **Step 6: Commit nothing — this task changed no files in the repo.**

The migration writes to SAM's data volume (not the SQLRAG repo). No commit needed. Move on to Task 9.

---

## Task 9: Smoke-test each plugin end-to-end against live data

**Files:** none (interactive validation, Harry's involvement needed)

**What this task produces:** Confidence that all four plugins behave correctly against live Intsys data. Any bugs surface here, get fixed via Task 10's iteration loop.

- [ ] **Step 1: Smoke-test `balance-check`.**

In the browser, open `balance-check` → **Cashbook reconcile**. Then **Debtors reconcile**. Then **Creditors reconcile**. Then **VAT reconcile**.

**✓ Looks good if:**
- Each page loads without 5xx errors
- Numbers appear (real Opera balances from Intsys)
- The numbers match what the legacy Python app shows for the same date range (cross-check by running the legacy app at `http://localhost:8000` simultaneously)

**Note any discrepancies.** Don't fix them yet — collect first, fix in Task 10.

- [ ] **Step 2: Smoke-test `bank-reconcile`.**

Open `bank-reconcile`. Click **Scan email** (or whatever the equivalent action is). Wait 30-60 seconds.

**✓ Looks good if:**
- A list of bank statements appears (from the live mailbox)
- For each statement, transaction lines are extracted
- Already-posted transactions are flagged as such (duplicate detection working)

**Important:** **don't actually import anything to Opera in this test.** We're checking that the plugin can read + display, not that the posting works. Posting tests come later if/when needed.

If the UI offers an "Import" or "Post" button, **don't click it**. Note this in the smoke-test notes file.

- [ ] **Step 3: Smoke-test `gocardless`.**

Open `gocardless`. Click **Test API connection** in the Settings page (with sandbox token configured).

**✓ Looks good if you see:** `Success — connected to GoCardless sandbox` (or similar 200 response).

Then click **Scan email**. The UI should list any GoCardless payout notification emails.

**✗ If `401 Unauthorized` on API test** — the sandbox token wasn't configured. Configure in Settings → save → retry.

- [ ] **Step 4: Smoke-test `suppliers`.**

Open `suppliers` → **Dashboard**. Then **Supplier list**.

**✓ Looks good if:**
- Supplier list loads from Opera (real Intsys suppliers)
- The dashboard shows aged debt / reconciliation status without errors

- [ ] **Step 5: Repeat all four plugin smoke-tests for the OTHER company.**

⚠ **Important** — every smoke test in Steps 1-4 above must be run against BOTH `intsys` AND `cloudsis`. The data migration in Task 8 brought both companies' data in; both must be verified.

In SAM Admin, switch the active company (typically via a company selector dropdown in the UI, or by sending `X-Opera-Company: <company>` on API calls). Then re-run Steps 1-4:

- balance-check pages — load against the other company
- bank-reconcile email scan — load against the other company
- gocardless test API + scan — should work the same regardless of company
- suppliers dashboard — load against the other company

**✓ Looks good if:** both companies produce sensible data, with the values matching the respective company's legacy Python view. They should differ (different customers, different transactions, different aliases) — that's correct.

**✗ If only one company shows data and the other is empty** — the company switching mechanism isn't working, OR the migration only succeeded for one company. Check Task 8's migration output for both companies, and check SAM's company list.

- [ ] **Step 6: Document any issues found.**

Create a file `/tmp/local-sam-smoke-test-results.md`:

```markdown
# Local SAM smoke-test results — 2026-05-XX

## balance-check
- intsys / Cashbook reconcile: ✓
- intsys / Debtors: ✗ (numbers off by £X.XX from legacy — investigate)
- intsys / Creditors: ✓
- intsys / VAT: ✓
- cloudsis / Cashbook reconcile: ✓
- cloudsis / Debtors: ✓
- cloudsis / Creditors: ✓
- cloudsis / VAT: ✓

## bank-reconcile
- intsys / Email scan: ✓
- intsys / Statement display: ✓
- intsys / Duplicate detection: <observed/needs check>
- cloudsis / Email scan: ✓
- cloudsis / Statement display: ✓

## gocardless
- intsys / API test: ✓
- intsys / Email scan: ✓
- intsys / Mandate list (from migrated data): ✓
- cloudsis / API test: ✓
- cloudsis / Email scan: ✓
- cloudsis / Mandate list (from migrated data): ✓

## suppliers
- intsys / Dashboard: ✓
- intsys / Supplier list: ✓
- intsys / Migrated statements visible: ✓
- cloudsis / Dashboard: ✓
- cloudsis / Supplier list: ✓
- cloudsis / Migrated statements visible: ✓
- <issue: ...>
```

**If everything passed cleanly for BOTH companies** — skip Task 10 and go straight to Task 11.

**If anything failed for either company** — proceed to Task 10 for the iteration loop.

---

## Task 10: Iterate — fix any issues, rebuild, re-upload, retest

**Files:** depends on what's broken; will edit `apps-sam/<plugin>/src/...` files

**What this task produces:** All four plugins passing smoke tests cleanly.

> **This task is open-ended by nature.** Each issue takes its own time to diagnose and fix. Repeat the cycle (fix → rebuild → re-upload → retest) until all four plugins pass.

- [ ] **Step 1: Pick one issue from the smoke-test results.**

Order by severity — anything that affects financial figures first, UI/cosmetic issues last.

- [ ] **Step 2: Diagnose.**

For each issue, check:
- **SAM logs** — `docker logs ai-sam | grep <plugin>` for runtime errors
- **Plugin source** — `apps-sam/<plugin>/src/...` — read the relevant service
- **Legacy Python** — compare against the equivalent file in `apps/` or `sql_rag/` (the SAM service file's comments cite the Python source by line number)

- [ ] **Step 3: Fix.**

Edit the relevant plugin source file. Make the minimum change needed.

- [ ] **Step 4: Run unit tests for the affected plugin.**

```bash
cd /Users/maccb/llmragsql/apps-sam/<plugin>
npm test
```

**✓ Looks good if:** all tests still pass. If the fix breaks an existing test, that's important — the test may be wrong, or the fix may be wrong. Both need attention.

- [ ] **Step 5: Rebuild the `.sap` and re-upload.**

```bash
cd /Users/maccb/llmragsql
./apps-sam/scripts/build-sap.sh <plugin>
curl -s -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/<plugin>-v1.0.0.sap"
```

**Note:** since you're re-uploading the same version (`v1.0.0`), SAM may either (a) refuse the upload (version conflict) or (b) overwrite silently. If (a), bump the version in `package.json` + `manifest.json` to `v1.0.1` first, then rebuild + upload.

- [ ] **Step 6: Retest the specific issue.**

Repeat the smoke test for the affected workflow.

**✓ Looks good if:** the issue is resolved.

**✗ If still broken:** loop back to Step 2 with the new evidence.

- [ ] **Step 7: Commit the fix.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/<plugin>/
git commit -m "fix(<plugin>): <one-line summary of what was wrong>"
```

- [ ] **Step 8: Loop back to Step 1 if more issues remain.**

Continue until smoke-test results show all green for all four plugins.

- [ ] **Step 9: Update the smoke-test notes file with final status.**

Mark every plugin's items as resolved. This file becomes part of the proof-of-readiness for the Jonathan handoff.

---

## Task 11: Tag final verified versions and produce handoff `.sap` files

**Files:** none new; potentially version bumps in `package.json` / `manifest.json` if needed

**What this task produces:** Four `.sap` files at known versions ready to hand to Jonathan.

- [ ] **Step 1: Confirm all four plugin versions are sensible.**

```bash
cd /Users/maccb/llmragsql
for p in balance-check bank-reconcile gocardless suppliers; do
  echo "=== $p ==="
  grep '"version"' apps-sam/$p/package.json apps-sam/$p/manifest.json
done
```

**✓ Looks good if you see:** the version in `package.json` and `manifest.json` matches for each plugin, and the versions reflect any bumps from Task 10.

- [ ] **Step 2: Rebuild all four `.sap` files one final time.**

```bash
cd /Users/maccb/llmragsql
for p in balance-check bank-reconcile gocardless suppliers; do
  ./apps-sam/scripts/build-sap.sh "$p"
done
ls -la /tmp/*.sap
```

**✓ Looks good if you see:** four `.sap` files at the latest versions.

- [ ] **Step 3: Copy the final `.sap` files to a stable handoff location.**

```bash
mkdir -p ~/sam-handoff-2026-05/
for p in balance-check bank-reconcile gocardless suppliers; do
  v=$(jq -r '.version' "/Users/maccb/llmragsql/apps-sam/$p/manifest.json")
  cp "/tmp/$p-v$v.sap" ~/sam-handoff-2026-05/
done
ls -la ~/sam-handoff-2026-05/
```

**✓ Looks good if you see:** four `.sap` files in `~/sam-handoff-2026-05/`, named with their versions.

- [ ] **Step 4: Commit any version bumps that happened during iteration.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/
git commit -m "chore: bump plugin versions to v1.0.x after local SAM validation" --allow-empty
```

(`--allow-empty` is fine if no version bumps were needed — keeps the commit log narrative clear.)

---

## Task 12: Create the "update test sam" automation

**Files:**
- Create: `apps-sam/scripts/update-local-sam.sh`
- Create: `/Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/feedback_local_sam_trigger_phrase.md`

**What this task produces:** A one-command SAM platform update mechanism, plus a memory rule so future Claude sessions recognise the trigger phrase.

- [ ] **Step 1: Write the update script.**

Create `apps-sam/scripts/update-local-sam.sh`:

```bash
#!/usr/bin/env bash
# apps-sam/scripts/update-local-sam.sh — pull latest SAM, rebuild, restart, re-smoke-test
# Triggered by user phrase: "update test sam to latest version"
set -euo pipefail

echo "=== Step 1: Show current SAM version ==="
cd ~/opera-knowledge-ref
OLD_SHA=$(git rev-parse HEAD)
echo "Current SAM platform: $OLD_SHA"
git log -1 --format="%h %ar %s"

echo ""
echo "=== Step 2: Pull latest ==="
git pull
NEW_SHA=$(git rev-parse HEAD)
echo "New SAM platform: $NEW_SHA"

if [[ "$OLD_SHA" == "$NEW_SHA" ]]; then
  echo "Already up to date — nothing to do."
  exit 0
fi

echo ""
echo "=== Step 3: Rebuild Docker image ==="
cd ~/.local/sam-test
docker compose build

echo ""
echo "=== Step 4: Restart containers ==="
docker compose up -d

echo ""
echo "=== Step 5: Wait for SAM to be healthy ==="
sleep 30
docker compose ps
curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:3001/api/health

echo ""
echo "=== Step 6: Confirm all four plugins still load ==="
echo "(Use a fresh JWT — log in if needed: ./apps-sam/scripts/get-sam-token.sh)"
if [[ -n "${SAM_TOKEN:-}" ]]; then
  curl -s -H "Authorization: Bearer $SAM_TOKEN" http://localhost:3001/api/admin/apps \
    | jq -r '.[] | .id + " " + .version + " " + .status'
else
  echo "Set SAM_TOKEN env var to skip manual verification step."
fi

echo ""
echo "=== Update complete: $OLD_SHA → $NEW_SHA ==="
echo "Run plugin smoke tests manually to confirm nothing broke."
```

Make it executable: `chmod +x apps-sam/scripts/update-local-sam.sh`.

- [ ] **Step 2: Save the trigger-phrase memory rule.**

Write to `/Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/feedback_local_sam_trigger_phrase.md`:

```markdown
---
name: trigger-phrase-update-local-sam
description: When Harry says "update test sam to latest version" (or close variants), run the documented update procedure and report results. Don't ask for confirmation — this is a routine maintenance operation Harry has pre-authorised.
type: feedback
---

When Harry says any of:

- "update test sam to latest version"
- "update local sam"
- "update test sam"
- "pull latest sam"
- close paraphrases

The exact response is: run `/Users/maccb/llmragsql/apps-sam/scripts/update-local-sam.sh`, capture the output, then report back to Harry with:

1. Old SAM commit SHA → new SAM commit SHA
2. Whether the rebuild succeeded
3. Whether SAM came back healthy
4. Whether all four plugins still load with the new SAM platform

**Why:** Harry articulated this as the desired UX on 2026-05-10: he says the phrase, Claude does the work. No confirmation needed because the script is idempotent and reversible (a bad update can be rolled back via `git checkout <old-sha> && docker compose build && docker compose up -d`).

**How to apply:**

1. Recognise the trigger phrase (or close paraphrase) — invoke the script immediately.
2. Don't prompt for confirmation. The script is pre-authorised.
3. If the update produces an error or a plugin fails to load after the update, report the specifics and propose next steps — don't auto-rollback unless Harry asks.
4. If the script doesn't exist (memory drifted), tell Harry — likely means the project memory is referencing a script that was removed; investigate before improvising a replacement.
5. Cross-check `apps-sam/MAINTAIN-SAM-PLUGINS.md` Section 1.7 — the same procedure should be documented there for human readers.
```

- [ ] **Step 3: Add a pointer in MEMORY.md.**

Edit `/Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/MEMORY.md` and append under "User Rules (IMPERATIVE)":

```markdown
- **"Update test sam" trigger phrase**: See [feedback_local_sam_trigger_phrase.md](feedback_local_sam_trigger_phrase.md) — when Harry says "update test sam to latest version", run `apps-sam/scripts/update-local-sam.sh` and report results. Pre-authorised; no confirmation needed.
```

- [ ] **Step 4: Commit the script.**

```bash
cd /Users/maccb/llmragsql
git add -f apps-sam/scripts/update-local-sam.sh
git commit -m "$(cat <<'EOF'
feat(scripts): update-local-sam.sh — one-command SAM platform update

Pull latest SAM source, rebuild image, restart containers, confirm
the four plugins still load. Triggered by Harry saying "update test
sam to latest version" (memory rule: feedback_local_sam_trigger_phrase).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Update `MAINTAIN-SAM-PLUGINS.md` to include local SAM steps

**Files:**
- Modify: `apps-sam/MAINTAIN-SAM-PLUGINS.md`

**What this task produces:** The maintenance doc now reflects the local-SAM-first workflow.

- [ ] **Step 1: Add Section 1.7 — "Updating the SAM platform itself".**

Edit `apps-sam/MAINTAIN-SAM-PLUGINS.md`. After Section 1.6 (Hotfix flow), add:

````markdown
### 1.7 — Updating the SAM platform itself

When Jonathan ships a new version of the SAM platform, you can update your local test SAM with one command:

```
# Terminal — your Mac
cd /Users/maccb/llmragsql
./apps-sam/scripts/update-local-sam.sh
```

This runs the full update sequence:

1. Show current SAM commit
2. `git pull` in `~/opera-knowledge-ref/`
3. `docker compose build` (rebuild the SAM image)
4. `docker compose up -d` (restart containers)
5. Wait for SAM to be healthy
6. List plugin status — confirm all four still load

**✓ Looks good if you see:** `Update complete: <old-sha> → <new-sha>` at the end, and all four plugins still listed as `Installed`.

**✗ If a plugin fails to load after the update** — the new SAM platform may have changed the plugin contract (e.g. a new required `manifest.json` field). Inspect the loader error in `docker logs ai-sam | grep PluginLoader`, update the plugin's manifest or code in `apps-sam/<plugin>/`, rebuild the `.sap`, and re-upload (per Section 1.2 patch flow).

**Shortcut:** you can also just say "update test sam to latest version" in a Claude Code session — Claude runs this script for you per the trigger-phrase memory rule.
````

- [ ] **Step 2: Update Section 1 release flow to include the local-SAM step.**

Find Section 1.2 (Ship a patch) and modify Step 1.2.4 to insert a local-SAM validation step between the version bump and the GitHub push.

Specifically: replace the existing Section 1.2 with a version that adds Step 1.2.3.5:

```markdown
#### Step 1.2.3.5 — Validate the fix in local SAM first

Before pushing to GitHub (where it could be pulled by the live SAM):

```
# Terminal — your Mac
cd /Users/maccb/llmragsql
./apps-sam/scripts/build-sap.sh <plugin>
curl -X POST http://localhost:3001/api/admin/apps/upload \
  -H "Authorization: Bearer $SAM_TOKEN" \
  -F "package=@/tmp/<plugin>-v<version>.sap"
```

Then open the local SAM UI and exercise the workflow the fix affects. Confirm the fix actually works against live Intsys data.

**✓ Looks good if:** the bug no longer reproduces; smoke tests still pass; nothing else broke.

**✗ If still broken:** loop back to Step 1.2.1. **DO NOT push to GitHub** until local SAM passes — that's the whole point of having a test environment. Per feedback rule `feedback_test_before_live_sam.md`, no release crosses to live SAM without local validation first.
```

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/MAINTAIN-SAM-PLUGINS.md
git commit -m "$(cat <<'EOF'
docs: MAINTAIN-SAM-PLUGINS.md — add Section 1.7 + local-SAM validation step

Section 1.7 covers updating the SAM platform itself (git pull +
rebuild + restart). Section 1.2 release flow now requires local
SAM validation before any push to the GitHub release repo.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: Draft the message to Jonathan

**Files:**
- Create: `docs/sam-rewrite/jonathan-pause-message.md`

**What this task produces:** A drafted message Harry can review and send to Jonathan, asking him to pause the existing deployment plan.

- [ ] **Step 1: Create the draft message.**

Write to `docs/sam-rewrite/jonathan-pause-message.md`:

```markdown
# Drafted message to Jonathan — pause the existing deployment

**Status:** DRAFT — Harry reviews before sending. Send via whatever channel makes sense (email, Slack, etc.).

---

Hi Jonathan,

Quick change of plan on the SAM plugin deployment.

I sent over `DEPLOY-TO-SAM.md` on Saturday — please **hold off** installing the four plugins on the live SAM for now.

I'm going to stand up a local SAM here on my Mac as a test environment first (standalone, no Central). I'll get the four plugins (`bank-reconcile`, `gocardless`, `suppliers`, `balance-check`) working end-to-end against live Intsys Opera SE here, iterate on anything that's broken, and only then hand you the proven `.sap` files for installation on the live SAM.

Operationally this gives us:

- Fewer surprises in live — the first time the live SAM sees a plugin, we already know it works
- A permanent dev/test environment for me to use for ongoing fixes and new plugins
- No more "ship to live and hope" — every release gets validated locally before it crosses over

**Timeframe:** ~few days for the initial setup + validation pass. I'll ping you when the proven `.sap` files are ready.

**Nothing needed from you in the meantime** — no licence, no Central work, nothing. I'm running local SAM standalone. When I'm ready to hand off, it'll be the four proven `.sap` files for you to install on the live SAM (or push to the `intsysuk/sam-*` repos with verified tags — whichever path you prefer).

When the apps are ready for live, we can also start thinking about the eventual merge with your apps + SAM platform updates.

Cheers,
Harry
```

- [ ] **Step 2: Commit the draft (it's not sent yet).**

```bash
cd /Users/maccb/llmragsql
git add docs/sam-rewrite/jonathan-pause-message.md
git commit -m "$(cat <<'EOF'
docs: draft message to Jonathan — pause existing SAM deployment

Internal team message (Jonathan is on the team, not external).
Asks him to hold the install while Harry validates the four
plugins in a local SAM test environment first.

Drafted, not sent — Harry reviews and sends via whatever channel
makes sense.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Show Harry the draft.**

In chat, paste the body of `docs/sam-rewrite/jonathan-pause-message.md` and ask if he wants changes before sending.

**Important:** **Do not auto-send.** Harry decides when and how to send. The deliverable is the draft, not the send.

---

## Task 15: Final acceptance check, README update, push to origin/main

**Files:**
- Modify: `apps-sam/README.md`

**What this task produces:** Everything committed, pushed, and discoverable. End state.

- [ ] **Step 1: Add a one-liner about local SAM to `apps-sam/README.md`.**

Edit `apps-sam/README.md`. After the "Deploying and maintaining SAM plugins" section (added in the previous deployment doc work), add:

```markdown
## Local test environment

Harry runs a local standalone SAM on his Mac for development and testing.
All fixes and enhancements go through there before any release crosses
to the live SAM. See
[apps-sam/MAINTAIN-SAM-PLUGINS.md](MAINTAIN-SAM-PLUGINS.md) Section 1.7
for how to update local SAM to the latest platform version, and
[docs/superpowers/specs/2026-05-10-local-sam-test-env-design.md](../docs/superpowers/specs/2026-05-10-local-sam-test-env-design.md)
for the architectural design.
```

- [ ] **Step 2: Run acceptance checks.**

```bash
cd /Users/maccb/llmragsql
echo "=== 1. Local SAM running? ==="
docker compose -f ~/.local/sam-test/docker-compose.yml ps
echo ""
echo "=== 2. All four plugins installed? ==="
curl -s -H "Authorization: Bearer $SAM_TOKEN" http://localhost:3001/api/admin/apps | jq -r '.[] | .id + " " + .version + " " + .status'
echo ""
echo "=== 3. Scripts present and executable? ==="
ls -la apps-sam/scripts/update-local-sam.sh apps-sam/scripts/build-sap.sh
echo ""
echo "=== 4. Maintenance doc has new sections? ==="
grep -c "^### 1\.7\|Step 1\.2\.3\.5" apps-sam/MAINTAIN-SAM-PLUGINS.md
echo ""
echo "=== 5. Jonathan message drafted? ==="
ls -la docs/sam-rewrite/jonathan-pause-message.md
echo ""
echo "=== 6. Handoff .sap files staged? ==="
ls -la ~/sam-handoff-2026-05/
echo ""
echo "=== 7. Memory file present? ==="
ls -la /Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/feedback_local_sam_trigger_phrase.md
echo ""
echo "=== 8. Commits ready to push ==="
git log origin/main..HEAD --oneline
```

Expected: SAM running with 4 plugins, both scripts present, doc has new sections, message drafted, `.sap` files staged, memory file exists, several commits ready to push.

- [ ] **Step 3: Push to origin/main.**

```bash
cd /Users/maccb/llmragsql
git push origin main
```

**✓ Looks good if you see:** `<old-sha>..<new-sha>  main -> main`.

- [ ] **Step 4: Confirm GitHub visibility.**

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" "https://raw.githubusercontent.com/HarryBurdett/llmragsql/main/apps-sam/scripts/update-local-sam.sh"
```

**✓ Looks good if you see:** `HTTP 200`.

- [ ] **Step 5: Update the project memory to mark this project complete.**

Edit `/Users/maccb/.claude/projects/-Users-maccb-llmragsql/memory/project_local_sam_test_env.md` and change `**Status as of 2026-05-10:** Concept agreed. Not yet started.` to a "completed" status with the actual outcome:

```markdown
**Status as of 2026-05-XX:** Complete. Local SAM running on Harry's Mac with all four plugins installed and validated against live Intsys Opera SE. The "update test sam to latest version" trigger phrase is wired. Maintenance doc updated. Draft message to Jonathan exists at `docs/sam-rewrite/jonathan-pause-message.md`.

**Outcome:** the local-SAM-first maintenance loop is operational. No fix reaches live SAM until validated locally.

**Followups (when convenient):**
- Harry sends the drafted Jonathan message
- Harry signals "ready" → Jonathan installs the four `.sap` files on the live SAM
- Add Opera 3 test connection (deferred phase — needs Pegasus Agent on Windows)
```

Commit the memory update separately (memory lives outside the repo, no git tracking needed — but make sure the file is saved).

---

## Self-review

### Spec coverage

- ✅ Standalone SAM install — Tasks 1-4
- ✅ Live Opera SE + live mailbox wired — Task 4
- ✅ Build .sap for each plugin — Tasks 5-7
- ✅ Upload all four to local SAM — Tasks 6-7
- ✅ Smoke-test end-to-end — Task 9
- ✅ Iterate on bugs — Task 10
- ✅ Final tagged .sap files for handoff — Task 11
- ✅ "Update test sam" script + memory — Task 12
- ✅ MAINTAIN doc updated — Task 13
- ✅ Jonathan message drafted (not sent) — Task 14
- ✅ Final commit + push + memory update — Task 15
- ✅ Isolation guarantee respected — standalone mode throughout
- ✅ Team context — Jonathan referred to as teammate, message is internal-tone

### Placeholder scan

- No "TBD"/"TODO"/"implement later" — every step has actual commands
- Task 10 is open-ended by design (iteration depends on what's found) but the structure shows how to handle each issue concretely
- Auth tokens are obtained explicitly (Task 5 Step 3) and reused via `SAM_TOKEN` env var
- Build approach (A vs B) is decided in Task 5, then Tasks 6-7 reference the chosen approach

### Type consistency

- `SAM_TOKEN` used consistently throughout
- File paths consistent: `apps-sam/scripts/build-sap.sh`, `apps-sam/scripts/update-local-sam.sh`, `~/.local/sam-test/`, `~/sam-handoff-2026-05/`
- Plugin order consistent: `balance-check`, `bank-reconcile`, `gocardless`, `suppliers`
- Version numbering consistent: `v1.0.x` patches per the maintenance doc convention

### Gaps (acknowledged)

- **Task 10 has variable scope** — by design. Iteration time depends on what's found. Acceptable for a validation pass.
- **Task 5 has investigation rather than fixed commands** — by design. We confirm the build mechanism on the day rather than guessing now.
- **Task 9 needs Harry's involvement** — interactive validation. Marked explicitly.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-local-sam-test-env.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks. Useful here because some tasks (4, 8) require interactive input from Harry — discrete subagents naturally pause between them.

**2. Inline Execution** — execute tasks in this session with checkpoints. Lower overhead but the session gets long given the number of tasks.

Given this project has several genuine pause points (Harry entering Opera credentials in the wizard, Harry running smoke tests interactively, Harry reviewing the drafted Jonathan message), **subagent-driven** is the better fit. But inline works too if you'd rather not switch modes.

Which approach?
