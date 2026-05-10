# SAM Maintenance Guide Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write the SAM-plugin maintenance reference (`apps-sam/MAINTAIN-SAM-PLUGINS.md`) and a companion render script (`apps-sam/scripts/render-md-to-html.py`); push to GitHub; email both `.md` and the rendered `.html` to `charlieb@intsysuk.com` with an HTML body and a pre-send approval gate.

**Architecture:** Markdown doc as canonical source. A small Python helper renders the doc to a styled standalone HTML so it looks like a document when opened in a browser. Markdown commits to git; rendered HTML is gitignored (regenerated whenever needed via the helper).

**Tech Stack:** Markdown, `markdown-it-py` (already in venv), git, the project's `/api/email/send` endpoint.

**Working directory:** `/Users/maccb/llmragsql/` (on `main`).

---

## File Structure

**Files created (3):**
- `apps-sam/MAINTAIN-SAM-PLUGINS.md` — the maintenance reference (~600 lines, 5 sections + appendix)
- `apps-sam/scripts/render-md-to-html.py` — renders any `.md` to a styled standalone `.html`
- `apps-sam/MAINTAIN-SAM-PLUGINS.html` — derived artifact, NOT committed (added to `.gitignore`)

**Files modified (2):**
- `apps-sam/README.md` — add a one-line mention of the maintenance doc next to the deployment doc
- `.gitignore` — add an entry so `apps-sam/*.html` (the rendered artifacts) stay out of git

**Files referenced (not modified):**
- `docs/superpowers/specs/2026-05-10-sam-maintenance-doc-design.md` — the spec this plan implements
- `apps-sam/DEPLOY-TO-SAM.md` — cross-referenced from the maintenance doc

---

## Format conventions (inherited from DEPLOY-TO-SAM)

Every code block in `MAINTAIN-SAM-PLUGINS.md` MUST start with one of:

```
# Terminal — your Mac
# Terminal — SAM Mac
# Browser — GitHub
# Browser — SAM Central
```

Every command must be followed by an `✓ Looks good if you see:` expected-output block. Failure modes appear inline as `✗ If you see X…`. The maintenance doc may also use `⚠` for warnings (irreversible actions).

---

## Task 1: Write the markdown-to-HTML render helper

**Files:**
- Create: `apps-sam/scripts/render-md-to-html.py`

**What this task produces:** A small Python script that takes a path to a `.md` file and produces a styled `.html` next to it. Used by Task 8 to render the maintenance doc for email attachment.

- [ ] **Step 1: Create the render script.**

Write the following to `apps-sam/scripts/render-md-to-html.py`:

```python
#!/usr/bin/env python3
"""Render a markdown file to a styled standalone HTML next to it.

Usage:
    python3 apps-sam/scripts/render-md-to-html.py apps-sam/MAINTAIN-SAM-PLUGINS.md

Produces:
    apps-sam/MAINTAIN-SAM-PLUGINS.html

The rendered HTML is a single self-contained file (CSS inlined). Safe to
attach to an email — the recipient can open it in any browser.
"""
from __future__ import annotations

import sys
from pathlib import Path

from markdown_it import MarkdownIt

CSS = """
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               Helvetica, Arial, sans-serif;
  font-size: 16px;
  line-height: 1.6;
  color: #24292f;
  max-width: 880px;
  margin: 2rem auto;
  padding: 0 1.5rem;
  background: #fff;
}
h1, h2, h3, h4 { line-height: 1.25; margin-top: 2rem; }
h1 { font-size: 2rem; border-bottom: 1px solid #d0d7de; padding-bottom: 0.4rem; }
h2 { font-size: 1.5rem; border-bottom: 1px solid #d0d7de; padding-bottom: 0.3rem; }
h3 { font-size: 1.2rem; }
h4 { font-size: 1rem; }
p { margin: 0.75rem 0; }
a { color: #0969da; text-decoration: none; }
a:hover { text-decoration: underline; }
code {
  font-family: "SF Mono", Menlo, Monaco, Consolas, "Liberation Mono",
               "Courier New", monospace;
  font-size: 0.9em;
  background: #f6f8fa;
  padding: 0.15em 0.35em;
  border-radius: 4px;
}
pre {
  background: #f6f8fa;
  padding: 0.9rem 1rem;
  border-radius: 6px;
  overflow-x: auto;
  font-size: 0.9em;
}
pre code { background: transparent; padding: 0; font-size: 1em; }
table {
  border-collapse: collapse;
  margin: 1rem 0;
  width: 100%;
}
th, td {
  border: 1px solid #d0d7de;
  padding: 0.4rem 0.7rem;
  text-align: left;
  vertical-align: top;
}
th { background: #f6f8fa; font-weight: 600; }
tr:nth-child(even) td { background: #fafbfc; }
blockquote {
  border-left: 4px solid #d0d7de;
  margin: 1rem 0;
  padding: 0.1rem 1rem;
  color: #57606a;
}
ul, ol { margin: 0.75rem 0; padding-left: 1.5rem; }
li { margin: 0.2rem 0; }
hr { border: 0; border-top: 1px solid #d0d7de; margin: 2rem 0; }
"""

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
{body}
</body>
</html>
"""


def render(md_path: Path) -> Path:
    """Render md_path to a styled HTML at the same basename + .html. Returns the HTML path."""
    text = md_path.read_text(encoding="utf-8")
    md = MarkdownIt("commonmark", {"html": True, "linkify": True}).enable("table")
    body = md.render(text)
    # Use the first H1 as the title if present, else the filename
    title = md_path.stem
    for line in text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break
    html = HTML_TEMPLATE.format(title=title, css=CSS, body=body)
    out_path = md_path.with_suffix(".html")
    out_path.write_text(html, encoding="utf-8")
    return out_path


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: render-md-to-html.py <path/to/file.md>", file=sys.stderr)
        return 2
    md_path = Path(sys.argv[1])
    if not md_path.exists():
        print(f"File not found: {md_path}", file=sys.stderr)
        return 1
    out = render(md_path)
    print(f"Rendered {md_path} → {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Make it executable.**

```bash
cd /Users/maccb/llmragsql
chmod +x apps-sam/scripts/render-md-to-html.py
```

- [ ] **Step 3: Smoke-test the script with the existing DEPLOY-TO-SAM.md.**

```bash
cd /Users/maccb/llmragsql
source venv/bin/activate
python3 apps-sam/scripts/render-md-to-html.py apps-sam/DEPLOY-TO-SAM.md
```

**✓ Looks good if you see:** `Rendered apps-sam/DEPLOY-TO-SAM.md → apps-sam/DEPLOY-TO-SAM.html` AND the file `apps-sam/DEPLOY-TO-SAM.html` exists.

**✓ Inspect the output:**
```bash
head -20 apps-sam/DEPLOY-TO-SAM.html
```
Expected: valid HTML5 with `<!DOCTYPE html>`, `<title>Deploying the four plugins into SAM</title>`, embedded `<style>` block.

- [ ] **Step 4: Clean up the smoke-test artifact.**

```bash
cd /Users/maccb/llmragsql
rm apps-sam/DEPLOY-TO-SAM.html
```

- [ ] **Step 5: Add `apps-sam/*.html` to `.gitignore`.**

Read the current `.gitignore`, then append the new rule. Run:

```bash
cd /Users/maccb/llmragsql
grep -q 'apps-sam/\*\.html' .gitignore || printf '\n# Rendered docs (regenerated by render-md-to-html.py)\napps-sam/*.html\n' >> .gitignore
```

**✓ Verify:**
```bash
tail -3 .gitignore
```
Expected: the new rule appears at the end.

- [ ] **Step 6: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/scripts/render-md-to-html.py .gitignore
git commit -m "$(cat <<'EOF'
docs: render-md-to-html.py helper for SAM docs

Renders any markdown file to a styled standalone HTML next to it,
using markdown-it-py (already in venv). Used to produce pretty
HTML versions of the SAM docs for email attachments.

The rendered .html files are gitignored — the .md is canonical;
the .html is regenerated on demand.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Write doc skeleton + Section 0 (mental model)

**Files:**
- Create: `apps-sam/MAINTAIN-SAM-PLUGINS.md`

**What this task produces:** Doc top-matter (title, intro, link back to DEPLOY-TO-SAM, table of contents) and all of Section 0 (mental model). Approximately 100 lines.

- [ ] **Step 1: Write top-matter + table of contents.**

Write to `apps-sam/MAINTAIN-SAM-PLUGINS.md`:

````markdown
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
````

- [ ] **Step 2: Write Section 0 — Mental model.**

Append:

````markdown
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

Per [feedback memory](../memory/feedback_legacy_python_reference.md), the legacy code under `apps/`, `sql_rag/`, and `frontend/src/pages/` is retained indefinitely. Don't propose retiring it.

---
````

- [ ] **Step 3: Verify file structure.**

```bash
cd /Users/maccb/llmragsql
wc -l apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "^### 0\." apps-sam/MAINTAIN-SAM-PLUGINS.md
```

Expected: ~100 lines, 5 subsections (0.1 through 0.5).

- [ ] **Step 4: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/MAINTAIN-SAM-PLUGINS.md
git commit -m "$(cat <<'EOF'
docs: start MAINTAIN-SAM-PLUGINS.md with Section 0 (mental model)

The two repos, the vendor pattern for shared, version semantics,
the 4-step diagnostic flowchart, and the legacy Python as canonical
behavioural reference. Per spec
2026-05-10-sam-maintenance-doc-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Write Section 1 — Release management

**Files:**
- Modify: `apps-sam/MAINTAIN-SAM-PLUGINS.md` (append).

**What this task produces:** Section 1 with six subsections (1.1 release flow, 1.2 patch, 1.3 minor, 1.4 shared, 1.5 rollback, 1.6 hotfix). Approximately 200 lines.

- [ ] **Step 1: Append Section 1 to the doc.**

Append the following to `apps-sam/MAINTAIN-SAM-PLUGINS.md`:

````markdown
## Section 1 — Release management

### 1.1 — The release flow (overview)

```
┌─────────────────┐   1. Edit       ┌────────────────┐
│ SQLRAG monorepo │ ──────────────> │ Bump version   │
│ (your Mac)      │                 │ in pkg + man    │
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

In SAM Central, navigate to the plugin's app catalogue entry. Either:
- Wait for the next scheduled sync (usually within minutes), or
- Click **Sync now** to force it.

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
````

- [ ] **Step 2: Verify file growth.**

```bash
cd /Users/maccb/llmragsql
wc -l apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "^### 1\." apps-sam/MAINTAIN-SAM-PLUGINS.md
```

Expected: ~300 lines total, 6 subsections of Section 1 (1.1–1.6).

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/MAINTAIN-SAM-PLUGINS.md
git commit -m "$(cat <<'EOF'
docs: MAINTAIN-SAM-PLUGINS.md Section 1 (release management)

The release flow overview, ship a patch (v1.0.x), ship a minor
(v1.x.0), the shared-library propagation gotcha, two rollback
options, and the hotfix flow.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Write Section 2 — Debugging

**Files:**
- Modify: `apps-sam/MAINTAIN-SAM-PLUGINS.md` (append).

**What this task produces:** Symptom → cause → fix tables plus subsections for ctx inspection, migration recovery, mailbox issues, slow queries. Approximately 180 lines.

- [ ] **Step 1: Append Section 2 to the doc.**

Append:

````markdown
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
| Endpoint slow only on one company | Opera DB has different volume on that company | Profile the Knex query (`db.raw(`...`)` with `EXPLAIN`) — usually a join order issue |
| Endpoint hangs indefinitely | Opera-side lock from a long-running posting job | Wait, retry. If reproducible, add `NOLOCK` hints on reads (per `feedback_locking_mandatory.md`). |
| Migration takes minutes | Backfill on a large table | Move backfill out of the migration to a one-off script; the migration just creates the structure |

---
````

- [ ] **Step 2: Verify file growth.**

```bash
cd /Users/maccb/llmragsql
wc -l apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "^## Section" apps-sam/MAINTAIN-SAM-PLUGINS.md
```

Expected: ~500 lines total, 3 top-level sections (0, 1, 2).

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/MAINTAIN-SAM-PLUGINS.md
git commit -m "$(cat <<'EOF'
docs: MAINTAIN-SAM-PLUGINS.md Section 2 (debugging)

Symptom-led lookup table covering the most common failure modes:
plugin won't load, runtime 500s, empty data, migration failures,
auth issues, mailbox not scanning, slow queries. Plus subsections
for ctx inspection, migration recovery, mailbox checklist, slow
query patterns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Write Section 3 (monitoring) + Section 4 (extending) + Appendix

**Files:**
- Modify: `apps-sam/MAINTAIN-SAM-PLUGINS.md` (append).

**What this task produces:** Section 3 (monitoring, ~60 lines), Section 4 (extending, ~120 lines), Appendix (~40 lines). Total ~220 lines.

- [ ] **Step 1: Append Section 3 to the doc.**

Append:

````markdown
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
````

- [ ] **Step 2: Verify final file structure.**

```bash
cd /Users/maccb/llmragsql
wc -l apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "^## Section\|^## Appendix" apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "^# Terminal\|^# Browser" apps-sam/MAINTAIN-SAM-PLUGINS.md
grep -c "✓ Looks good\|✗ If" apps-sam/MAINTAIN-SAM-PLUGINS.md
```

Expected: ~700 lines (target was ~600, will run a bit longer with detail), 6 sections (0-4 + Appendix), at least 10 machine labels, at least 15 ✓/✗ markers.

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/MAINTAIN-SAM-PLUGINS.md
git commit -m "$(cat <<'EOF'
docs: MAINTAIN-SAM-PLUGINS.md Sections 3-4 + Appendix

Monitoring (logs, health endpoints, sandbox indicators), extending
(add endpoint/migration/shared/plugin), file-structure reference.

Doc now complete: ~700 lines, 5 sections + appendix.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Update `apps-sam/README.md` cross-reference

**Files:**
- Modify: `apps-sam/README.md`

**What this task produces:** The README now points at both docs (`DEPLOY-TO-SAM.md` for deployment, `MAINTAIN-SAM-PLUGINS.md` for ongoing maintenance).

- [ ] **Step 1: Find the existing "Deploying to SAM" section.**

```bash
cd /Users/maccb/llmragsql
grep -n "Deploying to SAM" apps-sam/README.md
```

- [ ] **Step 2: Edit the README to add the maintenance doc reference.**

Replace the existing block (the line referencing `DEPLOY-TO-SAM.md`) with:

```markdown
## Deploying and maintaining SAM plugins

Two reference documents cover everything you need:

- **[DEPLOY-TO-SAM.md](DEPLOY-TO-SAM.md)** — first-time deployment.
  Extract plugins from this monorepo, push to GitHub, register in SAM
  Central, install on the SAM host, configure each plugin, migrate
  data from the legacy Python apps.
- **[MAINTAIN-SAM-PLUGINS.md](MAINTAIN-SAM-PLUGINS.md)** — everything
  after deployment. Ship fixes, ship features, roll back releases,
  debug production issues, monitor health, add new endpoints or new
  plugins.

The earlier handoff docs (`EMBEDDING.md`, `OPERATOR-SETUP.md`,
`MIGRATION.md`) have been retired and replaced by these two.
```

- [ ] **Step 3: Commit.**

```bash
cd /Users/maccb/llmragsql
git add apps-sam/README.md
git commit -m "$(cat <<'EOF'
docs: README points at both DEPLOY and MAINTAIN docs

apps-sam/README.md now cross-references both the deployment guide
(DEPLOY-TO-SAM.md) and the new maintenance reference
(MAINTAIN-SAM-PLUGINS.md).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Final acceptance check + push to origin/main

- [ ] **Step 1: Run all acceptance checks.**

```bash
cd /Users/maccb/llmragsql
echo "=== 1. Doc stats ==="
wc -l apps-sam/MAINTAIN-SAM-PLUGINS.md
echo ""
echo "=== 2. Section count (expect 6: Sections 0-4 + Appendix) ==="
grep -c "^## Section\|^## Appendix" apps-sam/MAINTAIN-SAM-PLUGINS.md
echo ""
echo "=== 3. Machine labels ==="
grep -c "^# Terminal\|^# Browser" apps-sam/MAINTAIN-SAM-PLUGINS.md
echo ""
echo "=== 4. Render script exists ==="
ls -la apps-sam/scripts/render-md-to-html.py
echo ""
echo "=== 5. Gitignore entry ==="
grep "apps-sam/\*\.html" .gitignore
echo ""
echo "=== 6. Commits ready to push ==="
git log origin/main..HEAD --oneline
```

Expected: ~700 lines, 6 sections, ≥10 machine labels, render script present, gitignore entry present, ~7 commits to push (Task 1 + Tasks 2-5 + Task 6 + the spec).

- [ ] **Step 2: Push to origin/main.**

```bash
cd /Users/maccb/llmragsql
git push origin main
```

**✓ Looks good if you see:** `<old-sha>..<new-sha>  main -> main` and no errors.

- [ ] **Step 3: Verify doc is visible on GitHub.**

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" "https://raw.githubusercontent.com/HarryBurdett/llmragsql/main/apps-sam/MAINTAIN-SAM-PLUGINS.md"
```

Expected: `HTTP 200`.

---

## Task 8: Render the markdown to HTML

- [ ] **Step 1: Run the render script.**

```bash
cd /Users/maccb/llmragsql
source venv/bin/activate
python3 apps-sam/scripts/render-md-to-html.py apps-sam/MAINTAIN-SAM-PLUGINS.md
```

**✓ Looks good if you see:** `Rendered apps-sam/MAINTAIN-SAM-PLUGINS.md → apps-sam/MAINTAIN-SAM-PLUGINS.html`.

- [ ] **Step 2: Verify the HTML file exists, has the right size, and isn't tracked by git.**

```bash
cd /Users/maccb/llmragsql
ls -la apps-sam/MAINTAIN-SAM-PLUGINS.html
git status apps-sam/MAINTAIN-SAM-PLUGINS.html
```

Expected: file exists (~40-80 KB), `git status` shows it as ignored (or doesn't mention it at all — clean state, just like the .md).

- [ ] **Step 3: Open in a browser for a visual smoke test (optional but recommended).**

```bash
open apps-sam/MAINTAIN-SAM-PLUGINS.html
```

Visual check: headers styled, tables bordered, code blocks have grey backgrounds, links are blue, body is readable width.

---

## Task 9: Draft email body and show to Harry for approval

- [ ] **Step 1: Draft the HTML email body.**

The body is similar to the deployment email but framed as Harry's reference being shared:

```html
<p>Hi Charlie,</p>

<p>FYI — this is my reference for maintaining the four SAM plugins:
shipping bug fixes, debugging production issues, rolling back when
something breaks, and extending with new features. Primarily my own
reference, sharing so you have a copy if I'm unavailable.</p>

<h3>The doc</h3>
<p><strong>One reference covers everything post-deployment:</strong>
<a href="https://github.com/HarryBurdett/llmragsql/blob/main/apps-sam/MAINTAIN-SAM-PLUGINS.md">MAINTAIN-SAM-PLUGINS.md</a></p>

<p>Five sections you can jump between:</p>
<ul>
  <li><strong>Section 0 — Mental model:</strong> the two repos, the
      vendor pattern for shared, the 4-step diagnostic flowchart</li>
  <li><strong>Section 1 — Release management:</strong> ship a patch,
      ship a feature, update shared, roll back, hotfix flow</li>
  <li><strong>Section 2 — Debugging:</strong> symptom → cause → fix
      lookup table; ctx inspection; migration recovery; mailbox checklist</li>
  <li><strong>Section 3 — Monitoring:</strong> what to watch in normal
      operation (logs, health endpoints, sandbox indicators)</li>
  <li><strong>Section 4 — Extending:</strong> add an endpoint, add a
      migration, add a shared utility, add a whole new plugin</li>
</ul>

<h3>Attachments</h3>
<ul>
  <li><code>MAINTAIN-SAM-PLUGINS.md</code> — the canonical markdown,
      always current on GitHub</li>
  <li><code>MAINTAIN-SAM-PLUGINS.html</code> — same content rendered
      as a standalone HTML page; opens in any browser with styled
      headers, tables, code blocks</li>
</ul>

<h3>How to use it</h3>
<p>It's organised by concern, not by sequence — read whichever section
applies to the situation. Section 0 first time you're in the doc, then
jump straight to whichever concern you've got.</p>

<p>Together with <a href="https://github.com/HarryBurdett/llmragsql/blob/main/apps-sam/DEPLOY-TO-SAM.md">DEPLOY-TO-SAM.md</a>
(the deployment guide I sent earlier), these two docs cover everything
operational about the SAM plugins.</p>

<p>Reply if anything is unclear.</p>

<p>Thanks,<br/>Harry</p>
```

- [ ] **Step 2: Show the user the full HTML body and the planned send call in chat.**

Paste the HTML body and the call plan:

```
POST http://localhost:8000/api/email/send
{
  "to": "charlieb@intsysuk.com",
  "subject": "SAM plugin maintenance reference",
  "from_email": "intsys@wimbledoncloud.net",
  "body": "<HTML body from Step 1>",
  "attachments": [
    "/Users/maccb/llmragsql/apps-sam/MAINTAIN-SAM-PLUGINS.md",
    "/Users/maccb/llmragsql/apps-sam/MAINTAIN-SAM-PLUGINS.html"
  ]
}
```

- [ ] **Step 3: Wait for Harry's explicit approval.**

Ask: **"Approve the body, or change anything?"**

If Harry requests changes, edit and re-show. Only proceed to Task 10 when Harry confirms.

---

## Task 10: Send the email

**Prerequisite:** Harry's approval from Task 9 Step 3.

- [ ] **Step 1: Confirm the API server is up.**

```bash
curl -s -o /dev/null -w "HTTP %{http_code}\n" "http://localhost:8000/api/health"
```

**✓ Looks good if you see:** `HTTP 200`.

**✗ If not** — start the server (see [CLAUDE.md](../CLAUDE.md) — `source venv/bin/activate && uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`).

- [ ] **Step 2: Write the JSON payload to a file (safer than inlining HTML).**

```bash
cat > /tmp/email_payload_charlie_maintain.json <<'EOF'
{
  "to": "charlieb@intsysuk.com",
  "subject": "SAM plugin maintenance reference",
  "from_email": "intsys@wimbledoncloud.net",
  "body": "<HTML BODY APPROVED IN TASK 9>",
  "attachments": [
    "/Users/maccb/llmragsql/apps-sam/MAINTAIN-SAM-PLUGINS.md",
    "/Users/maccb/llmragsql/apps-sam/MAINTAIN-SAM-PLUGINS.html"
  ]
}
EOF
```

(Substitute the approved HTML body — JSON-escape any embedded double quotes.)

- [ ] **Step 3: Send the email.**

```bash
curl -X POST http://localhost:8000/api/email/send \
  -H "Content-Type: application/json" \
  -d @/tmp/email_payload_charlie_maintain.json
```

**✓ Looks good if you see:** `{"success":true,"message":"Email sent to charlieb@intsysuk.com",...}`.

- [ ] **Step 4: Clean up temp file.**

```bash
rm -f /tmp/email_payload_charlie_maintain.json
```

- [ ] **Step 5: Report success to Harry.**

In chat:

> "Email sent to charlieb@intsysuk.com (200 OK, message_id: X). MAINTAIN-SAM-PLUGINS.md is on origin/main and HTML version attached for offline reading."

---

## Self-review

### Spec coverage
- ✅ Section 0 mental model — Task 2
- ✅ Section 1 release management — Task 3
- ✅ Section 2 debugging — Task 4
- ✅ Section 3 monitoring — Task 5
- ✅ Section 4 extending — Task 5
- ✅ Appendix file structure — Task 5
- ✅ Render script — Task 1
- ✅ Gitignore entry — Task 1 Step 5
- ✅ README update — Task 6
- ✅ Push to origin/main — Task 7
- ✅ HTML rendering — Task 8
- ✅ Email body draft + approval gate — Task 9
- ✅ Email send to Charlie only — Task 10

### Placeholder scan
- No "TBD"/"TODO"/"implement later" patterns.
- Every script step has the actual command and expected output.
- The HTML email body is fully drafted in Task 9 Step 1, not "draft a body".

### Type consistency
- File paths used consistently throughout (`apps-sam/MAINTAIN-SAM-PLUGINS.md`, `apps-sam/scripts/render-md-to-html.py`).
- Recipient consistently `charlieb@intsysuk.com` (one recipient this time, not two).
- Section numbering 0-4 + Appendix used everywhere.

No issues found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-10-sam-maintenance-doc.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks.

**2. Inline Execution** — execute tasks in this session with checkpoints.

Same recommendation as last time: **inline is fine for a docs-only deliverable.** Task 9 has the email-approval gate, which is the only point where I'd pause regardless.

Which approach?
