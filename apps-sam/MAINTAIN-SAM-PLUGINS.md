# Maintaining the four SAM plugins

This is the reference for everything *after* the plugins are installed in SAM: shipping bug fixes, adding features, rolling back when a release breaks, and debugging issues in production. For first-time deployment see [DEPLOY-TO-SAM.md](DEPLOY-TO-SAM.md).

**Primary audience:** Harry — the person who diagnoses issues and ships fixes. **Secondary audience:** Jonathan and Charlie, if Harry is unavailable.

**How this doc is structured:** four big sections by concern. Read whichever applies to the situation you're in — sections are independent.

| Section | When to read it |
|---|---|
| [0 — Mental model](#section-0--mental-model) | First time you maintain a plugin. Sets up the vocabulary and the diagnostic flowchart. |
| [1 — Release management](#section-1--release-management) | "I have a fix or a feature and need to ship it to SAM." |
| [2 — Debugging](#section-2--debugging) | "Something is broken and I need to find out why." |
| [3 — Monitoring](#section-3--monitoring) | "What should I watch in normal operation?" |
| [4 — Extending](#section-4--extending) | "I'm adding a new endpoint, migration, or plugin." |
| [Appendix — File structure](#appendix--file-structure-reference) | "Where does X live in the codebase?" |

---

## Section 0 — Mental model

Five things to know before you touch anything.

### 0.1 — The two repos

Code lives in two places:

| Repo | What's there | Who edits it |
|---|---|---|
| **SQLRAG monorepo** (`github.com/HarryBurdett/llmragsql`) | The source of truth for all four plugins, under `apps-sam/<plugin>/`. The shared library lives at `apps-sam/shared/`. | You. Direct commits to `main`. |
| **Four release repos** (`intsysuk/sam-<plugin>`) | Release artifacts. One repo per plugin. Tagged `v1.0.0`, `v1.0.1`, etc. SAM Central pulls from here. | Nobody edits these directly — they're produced by `apps-sam/scripts/extract-all.sh` and pushed by `push-to-github.sh`. |

**Rule:** never edit the release repos directly. They will get overwritten on the next extraction.

### 0.2 — The vendor pattern for shared

`apps-sam/shared/` contains code used by all four plugins (Opera helpers, period validation, VAT-rate lookup, etc.). It is **not** an npm package. The extraction script copies its contents into each plugin's `src/_shared/` folder at extraction time.

**Practical consequence:** changing `apps-sam/shared/` does not propagate until you re-extract. Updating shared is always a four-plugin release (see Section 1.4).

### 0.3 — Version semantics

Each plugin has its own version, carried in two files:

- `apps-sam/<plugin>/package.json` → `"version": "1.0.0"`
- `apps-sam/<plugin>/manifest.json` → `"version": "1.0.0"`

Both must match on every release. SAM Central pins each client license to a specific version.

| Bump | When | Example |
|---|---|---|
| Patch (1.0.0 → 1.0.1) | Bug fix, no new behaviour | "GoCardless import was double-posting on overlapping payouts" |
| Minor (1.0.0 → 1.1.0) | New endpoint or new feature, backwards-compatible | "Added supplier-overrides endpoint" |
| Major (1.0.0 → 2.0.0) | Breaking change to existing endpoint shape or DB schema | Not expected in normal maintenance |

### 0.4 — Where to look first when something breaks

Four-step diagnostic flowchart:

1. **One plugin or all four?**
   - One plugin → likely a bug in that plugin's code
   - All four → likely the shared library, or a SAM-platform issue
2. **Did it just deploy?**
   - Yes → suspect the new version; consider rollback (Section 1.5) before deep-diving
   - No → probably an environmental change (mailbox, Opera connection, network)
3. **Is it environmental (data flowing in)?**
   - Mailbox not picking up → see Section 2 "Mailbox not scanning"
   - Opera connection lost → see SAM Admin → Opera Connections (not in this doc — handled by SAM)
4. **Is the legacy Python equivalent working?**
   - Legacy works, SAM doesn't → SAM port has drifted. Compare the SAM service file (which cites Python line numbers in its comments) against the Python source.
   - Both broken → the bug is in the *behaviour*, fix the legacy first (it's the canonical reference), then port the fix to SAM.

### 0.5 — The legacy Python is the canonical behavioural reference

Every SAM service file has comments citing the legacy Python file and line numbers it was ported from (e.g. `// see sql_rag/bank_import.py:432`). When debugging:

- The legacy Python is the assumed-correct baseline.
- If SAM disagrees with Python, Python is right unless you have a specific reason to override.
- Fix legacy bugs in both places (legacy stays a working reference, not a fossil).

The legacy code under `apps/`, `sql_rag/`, and `frontend/src/pages/` is retained indefinitely as the canonical behavioural reference. Don't propose retiring it.

---

## Section 1 — Release management

### 1.1 — The release flow (overview)

```
┌─────────────────┐   1. Edit       ┌────────────────┐
│ SQLRAG monorepo │ ──────────────> │ Bump version   │
│ (your Mac)      │                 │ in pkg + man   │
└─────────────────┘                 └────────┬───────┘
                                             │ 2. Extract & push
                                             ▼
                                    ┌────────────────────┐
                                    │ intsysuk/sam-*     │
                                    │ tagged v1.0.x      │
                                    └────────┬───────────┘
                                             │ 3. Sync
                                             ▼
                                    ┌────────────────────┐
                                    │ SAM Central        │
                                    │ (auto-pulls)       │
                                    └────────┬───────────┘
                                             │ 4. Host sync
                                             ▼
                                    ┌────────────────────┐
                                    │ SAM host installs  │
                                    │ new version        │
                                    └────────────────────┘
```

Five-line summary of the loop:

1. Edit code in the monorepo (`apps-sam/<plugin>/src/...`).
2. Bump the plugin's version in `package.json` and `manifest.json` (both must match).
3. Re-run `extract-all.sh` then `push-to-github.sh <plugin>`. This pushes the new code and tags it.
4. SAM Central picks up the new tag (on schedule or via "Sync now").
5. The SAM host installs the new version on its next sync.

### 1.2 — Ship a patch (v1.0.0 → v1.0.1)

Use this for: a bug fix in one plugin. No new endpoints. No shared-library changes.

#### Step 1.2.1 — Make the fix

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>
# Edit the relevant file(s), e.g. src/services/import-route.ts
```

Run the plugin's tests before doing anything else:

```
# Terminal — your Mac
npm test
```

**✓ Looks good if you see:** all green, zero failures.

**✗ If tests fail:** stop. Don't release a broken plugin. Fix until green.

#### Step 1.2.2 — Bump the version in two places

Edit `apps-sam/<plugin>/package.json` and `apps-sam/<plugin>/manifest.json`. Both should show the new version, e.g. `"version": "1.0.1"`.

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>
grep '"version"' package.json manifest.json
```

**✓ Looks good if you see:** both files show the same new version.

#### Step 1.2.3 — Commit and push the monorepo change

```
# Terminal — your Mac
cd ~/llmragsql
git add apps-sam/<plugin>/
git commit -m "fix(<plugin>): <one-line bug summary>"
git push origin main
```

#### Step 1.2.4 — Re-extract and push the release repo

```
# Terminal — your Mac
cd ~/llmragsql
./apps-sam/scripts/extract-all.sh
cd ~/sam-plugins-staging
./push-to-github.sh <plugin>
```

**✓ Looks good if you see:** `✓ Success — v1.0.1 tagged and pushed`.

**✗ If you see `tag v1.0.1 already exists`** — the push script tried to push a tag that's already on GitHub. Two possibilities:
1. You forgot to bump the version (it pushed v1.0.0 again). Check `package.json` and `manifest.json`.
2. You already published v1.0.1 from a previous attempt. Skip — it's already there.

#### Step 1.2.5 — Trigger SAM to pick up the new version

In SAM Central, navigate to the plugin's app catalogue entry. Either wait for the next scheduled sync (usually within minutes), or click **Sync now** to force it.

Then in SAM Admin (on the SAM host), Apps → **Sync now**. Watch:

```
# Terminal — SAM Mac
docker logs -f ai-sam | grep -E "GitInstall|PluginLoader"
```

**✓ Looks good if you see:**
```
[GitInstall] cloning github.com/intsysuk/sam-<plugin>.git@v1.0.1
[GitInstall] Success
[PluginLoader] Running migrations for <plugin> (0 to apply)
[PluginLoader] Loaded <plugin> (N routes registered)
```

The previous version is now superseded. The new code is running.

### 1.3 — Ship a minor (v1.0.0 → v1.1.0)

Identical flow to a patch, except:

- The version bump goes to the next minor (e.g. `1.0.5` → `1.1.0`)
- The commit message and tag annotation usually describe the new feature(s)
- If you added a database migration, the `[PluginLoader]` log line will show `Running migrations for <plugin> (1 to apply)` — confirm that line and that the migration didn't error

### 1.4 — Update the shared library

⚠ **Updating `apps-sam/shared/` touches all four plugins.** You must:

1. Re-extract (which vendors the new shared into each plugin)
2. Bump the version in **all four** plugin manifests
3. Push all four release repos

#### Step 1.4.1 — Make the shared change

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/shared
# Edit src/...
npm test
```

**✓ Looks good if you see:** shared tests pass.

#### Step 1.4.2 — Bump all four plugin versions

```
# Terminal — your Mac
cd ~/llmragsql
for plugin in balance-check bank-reconcile gocardless suppliers; do
  echo "=== $plugin ==="
  grep '"version"' apps-sam/$plugin/package.json apps-sam/$plugin/manifest.json
done
```

Edit each one. Use patch bumps (e.g. all four go from `1.0.x` to `1.0.x+1`) unless the shared change is a feature, in which case minor bumps.

#### Step 1.4.3 — Commit, push, extract, push all four

```
# Terminal — your Mac
cd ~/llmragsql
git add apps-sam/
git commit -m "feat(shared): <one-line summary> — all four plugins bumped"
git push origin main

./apps-sam/scripts/extract-all.sh
cd ~/sam-plugins-staging
for plugin in balance-check bank-reconcile gocardless suppliers; do
  ./push-to-github.sh $plugin
done
```

**✓ Looks good if you see:** four `✓ Success` lines, one per plugin.

### 1.5 — Roll back a broken release

If a release is causing problems in production, you have two options.

#### Option A — Re-pin to the previous version in SAM Central (fast, ~30 seconds)

In SAM Central → Apps catalogue → the plugin → **Change pinned version** → select the previous version (e.g. v1.0.0 instead of v1.0.1) → **Save**.

In SAM Admin (on the SAM host) → Apps → **Sync now**. The host downgrades to the pinned version.

**✓ Looks good if you see:** `[GitInstall] cloning github.com/intsysuk/sam-<plugin>.git@v1.0.0` (note the older version).

**This is reversible** — pin back to the new version when fixed.

#### Option B — Revert and republish (clean)

If you want the rollback to be the new canonical version (not a pin override):

```
# Terminal — your Mac
cd ~/llmragsql
git revert <commit-sha-of-bad-release>
# Bump version (e.g. v1.0.1 was broken → publish v1.0.2 with the revert)
# Edit package.json + manifest.json
git add apps-sam/<plugin>/
git commit -m "revert: rollback <plugin> v1.0.1 — <reason>"
git push origin main

./apps-sam/scripts/extract-all.sh
cd ~/sam-plugins-staging
./push-to-github.sh <plugin>
```

Then trigger SAM Central + SAM host sync as in Step 1.2.5.

### 1.6 — Hotfix flow (when something is on fire)

The minimum sequence when production is degraded and you need to ship fast:

1. **Reproduce the bug locally.** If you can't reproduce, you can't trust the fix. (5-15 min)
2. **Write the fix.** Skip the temptation to refactor on the way through. One change. (5-30 min)
3. **Run tests for the affected plugin.** Don't skip this even when tired — broken fix = bigger fire. (1-2 min)
4. **Bump patch version + commit + push + extract + push release repo.** (Steps 1.2.2–1.2.4 above, ~3 min.)
5. **Sync SAM** (Step 1.2.5, ~1 min).
6. **Verify in production** by reproducing the original symptom and confirming it's fixed.

If the fix turns out to be wrong, roll back via Section 1.5 Option A, then go back to Step 1 with the new evidence.

---

## Section 2 — Debugging

When something breaks in SAM, find the symptom in the table below, follow the diagnostic, apply the fix.

### 2.1 — Symptom lookup table

| Symptom | Most likely cause | Where to look | First fix to try |
|---|---|---|---|
| Plugin doesn't appear in SAM Admin after sync | Manifest validation failed | `docker logs ai-sam \| grep PluginLoader` on SAM Mac | Compare `manifest.json` to a working plugin's manifest |
| Plugin appears in Admin but endpoints return 500 | Knex query error (typo in column or table name) | `docker logs ai-sam \| grep <plugin>` | Cross-check column names against `scripts/opera_snapshot.json` |
| Plugin appears but data is empty | `ctx.db.opera` not wired or wrong company | `req.operaCompany` middleware logs | SAM Admin → Opera Connections — confirm company is selected |
| `[PluginLoader] migration failed` | A migration is broken or per-app DB unreachable | `docker logs ai-sam \| grep migration` | Re-run the migration locally against a clean SQLite to find the error |
| `[GitInstall] git: authentication required` | SAM host's GitHub PAT lost access | SAM host → Settings → Integrations → GitHub | Re-issue PAT with `repo` scope on `intsysuk` |
| Plugin logs `503 — ctx.llm not wired` | SAM hasn't injected the LLM service | SAM host config | SAM Admin → AI Settings — confirm Claude Vision or Gemini key is set |
| Plugin logs `503 — bankPdfExtractor missing` | Same as above for the PDF extractor | SAM host config | Same — `ctx.llm` enables this; if absent, the PDF flow can't run |
| Bank statement mailbox not picking up new emails | `owner_app_id` not set, or adapter is checking too slowly | SAM Admin → Email Mailboxes | Confirm `owner_app_id: bank-reconcile` on the right mailbox; wait 60 seconds |
| GoCardless API returns 401 | Token typo, environment mismatch, or revoked token | Plugin Settings page | Re-paste sandbox token from GoCardless dashboard |
| GoCardless import double-posted | Idempotency check failed (rare) | Plugin's `gocardless_import_idempotency` table | Compare matching logic to legacy `sql_rag/opera_sql_import.py` |
| Endpoint suddenly slow (> 2s) | Knex query missing an index, or Opera-side lock | Plugin logs + Opera SQL Server activity | See 2.5 below |
| Supplier statement reconcile gives wrong matches | Drift between SAM and legacy matcher | Compare service file's cited Python line numbers | Re-port the diff from `sql_rag/...` |

### 2.2 — How to inspect what SAM passes to a plugin (ctx inspection)

When a plugin behaves unexpectedly, you often need to know what SAM is actually passing in at runtime. Add a temporary log statement to the relevant service file:

```ts
// Inside a service or router handler
console.log("[ctx-inspect]", {
  user: ctx.user,
  operaCompany: ctx.operaCompany,
  hasOpera: !!ctx.db.opera,
  hasApp: !!ctx.db.app,
  hasLlm: !!ctx.llm,
  hasEmailIngest: !!ctx.emailIngest,
});
```

Then redeploy (Section 1.6 hotfix flow), trigger the failing endpoint, watch the logs on the SAM Mac. Remove the log line and ship again once you have your answer.

### 2.3 — Migration failure recovery

A failed migration can leave the per-app database half-applied. Recovery:

1. **Look at the actual error in the SAM logs** — Knex prints the failing SQL.

```
# Terminal — SAM Mac
docker logs ai-sam | grep -A 5 "migration failed"
```

2. **Identify which migration file failed** (the log shows e.g. `005_align_imports_history.ts`).

3. **Inspect the per-app DB to see how far it got**:

```
# Terminal — SAM Mac
# (Use whatever client SAM uses for its MSSQL — sqlcmd or a GUI)
SELECT * FROM <plugin>_app.knex_migrations ORDER BY id DESC;
```

This shows which migrations did succeed. The failed one is missing from the list but its DDL may be partly applied.

4. **Two recovery paths:**
   - **Roll back the migration manually** (write a corrective SQL by hand to undo whatever the failed migration partially applied), then fix the migration code locally, ship the fix per Section 1.6.
   - **Drop and recreate the per-app DB** (only if no production data — likely safe early in deployment). Then re-trigger SAM install to rebuild from scratch.

### 2.4 — Mailbox-not-scanning checklist

When you assigned a mailbox to a plugin but emails aren't being processed:

1. **Mailbox `owner_app_id` matches the plugin's app id** — SAM Admin → Email Mailboxes.
2. **Microsoft Graph connection is `Active`** — SAM Admin → Email Settings.
3. **Adapter scan interval has elapsed** — default is 60 seconds. Wait, then trigger from the plugin's UI.
4. **Plugin can actually call `listMyMailboxes()`** — add a temporary `console.log` on the adapter init (see 2.2) and confirm it returns the mailbox row.
5. **The email is in the mailbox's inbox**, not a sub-folder — by default the adapter scans the inbox.

### 2.5 — Slow / hanging queries

A few patterns to recognise:

| Pattern | Cause | Fix |
|---|---|---|
| Endpoint suddenly slow after a release | Missing or wrong index on a column you started filtering on | Add an index migration. Test locally to confirm. |
| Endpoint slow only on one company | Opera DB has different volume on that company | Profile the Knex query (`db.raw('...')` with `EXPLAIN`) — usually a join order issue |
| Endpoint hangs indefinitely | Opera-side lock from a long-running posting job | Wait, retry. If reproducible, add `NOLOCK` hints on reads (per locking rules). |
| Migration takes minutes | Backfill on a large table | Move backfill out of the migration to a one-off script; the migration just creates the structure |

---

## Section 3 — Monitoring

What to watch in normal operation. Not exhaustive — just the dials worth a daily glance.

### 3.1 — Logs to follow

```
# Terminal — SAM Mac
docker logs -f ai-sam --tail 100
```

Filter for one plugin:

```
# Terminal — SAM Mac
docker logs -f ai-sam | grep "<plugin>"
```

Watch for:
- `[PluginLoader]` lines on every sync — should always say `Loaded` for all four
- `[GitInstall]` lines on every release — should always say `Success`
- Stack traces — never expected in steady state

### 3.2 — Per-plugin health endpoints

Each plugin exposes a health check. From inside SAM (or via the SAM Admin shell):

```
# Terminal — SAM Mac
curl http://localhost:<sam-port>/api/balance-check/health
curl http://localhost:<sam-port>/api/bank-reconcile/health
curl http://localhost:<sam-port>/api/gocardless/health
curl http://localhost:<sam-port>/api/suppliers/health
```

**✓ Looks good if you see:** `{"status": "ok", "version": "1.0.x"}` for each.

**✗ If any returns non-200** — plugin is broken or not loaded. See Section 2.

### 3.3 — Sandbox vs live indicators

Only relevant for `gocardless`. Open the GoCardless plugin → **Settings** → confirm the environment field shows `sandbox` or `live`. ⚠ If it shows `live` and you weren't expecting that, do NOT proceed with any imports — verify with Harry first.

### 3.4 — What "all green" looks like on a normal day

- All four `/api/<plugin>/health` endpoints return 200
- `docker logs ai-sam --tail 50 | grep ERROR` is empty
- Each plugin's "last sync" timestamp (visible in its UI) is recent
- No `[PluginLoader] reload` events without an accompanying `[GitInstall]` (a reload without an install means SAM thinks the plugin restarted unexpectedly)

---

## Section 4 — Extending

### 4.1 — Add a new endpoint to an existing plugin

A SAM plugin endpoint is three things: a route definition, a service function, and a test. Follow the existing pattern.

#### Step 4.1.1 — Add the service function

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>/src/services
# Create new-endpoint.ts
```

Example:

```ts
// apps-sam/<plugin>/src/services/new-endpoint.ts
import type { Knex } from "knex";

export interface NewEndpointInput {
  // ...
}

export async function newEndpoint(db: Knex, input: NewEndpointInput) {
  // implementation
  return { ok: true };
}
```

#### Step 4.1.2 — Wire it in the router

```ts
// apps-sam/<plugin>/src/router.ts
import { newEndpoint } from "./services/new-endpoint";

// inside the router setup function:
router.post("/api/<plugin>/new-endpoint", async (req, res) => {
  const result = await newEndpoint(ctx.db.opera, req.body);
  res.json(result);
});
```

#### Step 4.1.3 — Write the test

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>/tests
# Create new-endpoint.test.ts
```

```ts
import { describe, it, expect } from "vitest";
import { newEndpoint } from "../src/services/new-endpoint";
import { createKnexMock } from "./helpers/knex-mock";

describe("newEndpoint", () => {
  it("returns ok for valid input", async () => {
    const db = createKnexMock([{ /* fake row */ }]);
    const result = await newEndpoint(db, { /* input */ });
    expect(result.ok).toBe(true);
  });
});
```

#### Step 4.1.4 — Run tests

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>
npm test
```

**✓ Looks good if you see:** new test included in the count, all green.

#### Step 4.1.5 — Ship via Section 1.3 (minor version bump)

### 4.2 — Add a new database migration

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/<plugin>/db/migrations
# Create NNN_descriptive_name.ts where NNN is the next number
```

Use the existing migration naming convention: zero-padded sequence + descriptive snake_case suffix (e.g. `010_scanned_statements.ts`).

```ts
// apps-sam/<plugin>/db/migrations/NNN_descriptive_name.ts
import type { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("new_table", (t) => {
    t.increments("id").primary();
    t.string("name").notNullable();
    t.timestamps(true, true);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTable("new_table");
}
```

The plugin's `migrations.test.ts` will run the new migration automatically on next `npm test` to confirm it applies and reverts cleanly. Always confirm tests pass before pushing.

### 4.3 — Add a new shared utility

Per Section 1.4, **any change to shared touches all four plugins**.

```
# Terminal — your Mac
cd ~/llmragsql/apps-sam/shared/src
# Create new-helper.ts
# Add an export in src/index.ts (or src/opera/index.ts if Opera-related)
npm test
```

Then follow Section 1.4 to ship: bump all four plugin versions, commit, push, extract, push all four release repos.

### 4.4 — Add a whole new plugin

Less common. Five steps:

1. **Scaffold from an existing plugin.** `cp -r apps-sam/balance-check apps-sam/<new-plugin>` then rename in `package.json`, `manifest.json`, and the workspaces array in `apps-sam/package.json`.
2. **Write the `manifest.json`** — `id`, `name`, `description`, `category`, `frontend`, `backend`, `permissions`, `consumes`. Match the format of an existing manifest.
3. **Add to the workspaces array** in `apps-sam/package.json` so the monorepo recognises it.
4. **Write the first endpoint + test** (Section 4.1).
5. **Run extract-all.sh** — should produce a fifth staged repo. Push to a new `intsysuk/sam-<new-plugin>` repo and register in SAM Central per `DEPLOY-TO-SAM.md` Phase 3.

---

## Appendix — File structure reference

Each plugin has roughly the same layout. Here's where to find things in any plugin's folder under `apps-sam/<plugin>/`:

| Path | Contains |
|---|---|
| `package.json`, `manifest.json` | Version, name, declared routes/permissions |
| `src/index.ts` | Default-export factory function consumed by SAM's PluginLoader |
| `src/router.ts` | Express route registrations |
| `src/services/*.ts` | One file per concern (one endpoint or one cohesive group of endpoints) |
| `src/services/default-*.ts` | Default adapters provided by the plugin when SAM doesn't inject one (filesystem, LLM, email-ingest, etc.) |
| `src/_shared/` | Vendored copy of `apps-sam/shared/`. Don't edit here — edit `apps-sam/shared/` and re-extract. |
| `db/migrations/NNN_*.ts` | Knex migrations, applied automatically on plugin install |
| `tests/*.test.ts` | Vitest unit tests (run with `npm test`) |
| `tests/migrations.test.ts` | Smoke test that runs all migrations on a clean SQLite |
| `frontend/src/` | React components — the page rendered when the user clicks into this plugin in SAM |
| `frontend/dist/index.js` | UMD bundle produced by `vite build`. Registers the entry component on `window.__SAM_APPS__`. |

Outside the plugins:

| Path | Contains |
|---|---|
| `apps-sam/shared/src/` | The shared library — vendored into each plugin on extraction |
| `apps-sam/scripts/extract-all.sh` | Packages all four plugins as standalone repos in `~/sam-plugins-staging/` |
| `apps-sam/scripts/push-to-github.sh` | Pushes a staged repo to its `intsysuk/sam-*` GitHub repo and tags it |
| `apps-sam/scripts/migrate-from-python/` | Data migration tool — only relevant during first-time deployment |
| `apps-sam/scripts/render-md-to-html.py` | Helper that produces styled HTML versions of these docs for email delivery |
| `apps-sam/DEPLOY-TO-SAM.md` | First-time deployment guide |
| `apps-sam/MAINTAIN-SAM-PLUGINS.md` | This file |
| `apps-sam/README.md` | Workspace overview and conventions |

The legacy Python that the SAM ports faithfully replicate:

| Path | Contains |
|---|---|
| `apps/<plugin>/` | The legacy Python app's routes and business logic |
| `sql_rag/*.py` | Core Opera-interfacing modules (bank_import.py, opera_sql_import.py, statement_reconcile.py, etc.) |
| `frontend/src/pages/*.tsx` | The legacy React pages (still in use as the canonical reference) |

---

**End of document.** Edits welcome — open a PR or commit directly to `main`.
