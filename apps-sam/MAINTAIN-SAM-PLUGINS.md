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
