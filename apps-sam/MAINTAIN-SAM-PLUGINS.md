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
