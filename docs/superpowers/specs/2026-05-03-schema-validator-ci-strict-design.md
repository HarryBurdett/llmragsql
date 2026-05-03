# Schema Validator CI Strict — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

Make `scripts/validate_sql_columns.py` a hard CI gate. **No SQL column reference may ship to `main` that does not exist in the canonical Opera schema snapshot.** Eliminate the entire class of "silent typo" bugs (`pt_ref`, `st_ref`, `at_date`, `nk_lstdate`, etc.) that pyodbc swallows as warnings.

## Why

Today's session uncovered three live typos (`pt_ref`, `st_ref`, plus 172 candidates from a one-shot scan) that had been silently broken in production for months. pyodbc throws on missing columns, but the calling code wraps it in a `logger.warning` and returns empty results. That looks "working" until someone runs an audit. We've taken eight commits this session to swat them. The class doesn't end without a CI gate.

## Constraints (must hold)

- **Production-grade:** the validator must be deterministic, fast (<10s on the full repo), and stable against false positives.
- **Existing 172 candidates:** must be triaged into "real bug — fix" vs "false positive — suppress with reason" before strict mode flips on. No "fix later" lists.
- **Opera 3 + Opera SE coverage:** validates against the SE snapshot `scripts/opera_snapshot.json`; a parallel mechanism handles Opera 3 (DBF) field names. Out of scope for this spec — flagged for a follow-up.
- **No regressions:** existing tests must pass; new tests cover the validator itself.

## Architecture

```
                         ┌─────────────────────────────┐
                         │ scripts/opera_snapshot.json │  ← canonical schema
                         └──────────────┬──────────────┘
                                        │ load
                                        ▼
┌──────────────┐   walk    ┌────────────────────────────┐  finding(s)
│ apps/, api/, │ ────────▶ │ validate_sql_columns.py    │ ─────────────▶ stdout report
│ sql_rag/,    │           │  • extract SQL strings     │                + exit code
│ scripts/     │           │  • parse identifiers       │
└──────────────┘           │  • narrow by FROM/UPDATE   │
                           │  • check vs schema         │
                           │  • apply suppression list  │
                           └────────────────────────────┘
                                        │
                                        ▼
                           ┌────────────────────────────┐
                           │ tests/test_validate_sql_   │
                           │ columns.py                 │
                           │  • fixture-based unit tests│
                           │  • known-bug regression    │
                           └────────────────────────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │ .github/workflows│  ← runs on every PR
                              │ + pre-commit hook│  ← runs locally
                              └──────────────────┘
```

## Components

### 1. `scripts/validate_sql_columns.py` (existing — needs hardening)

Already written this session. Needs:
- **Suppression mechanism:** `scripts/sql_validator_suppressions.yaml` listing every false-positive `(file_path, line_range, identifier, reason)`. Forces a human to write a justification per suppression. No bulk-suppress.
- **Strict mode (`--strict`):** exit 1 on any unsuppressed unknown column. Already partially exists — must verify behaviour and harden.
- **Fast mode:** caches the parsed snapshot; <10s end-to-end on the full repo.
- **Deterministic output:** sorted findings, stable line numbers.

### 2. `scripts/sql_validator_suppressions.yaml` (new)

Format:
```yaml
suppressions:
  - file: api/main.py
    line: 6756
    column: it_value
    reason: |
      Local Python variable used as SQL parameter — not a column reference.
      Confirmed via grep: no SELECT it_value FROM ... in this file.
    added: 2026-05-03
    added_by: charlie
```

The validator loads this and skips matching findings. **Adding a suppression is a code-review step** — the YAML is checked into git, change history is auditable.

### 3. Triage pass (one-time)

Before strict mode lands, every one of the 172 current candidates is classified:

| Class | Action |
|---|---|
| **Real Opera typo** (e.g. `pt_ref`, `st_ref`, `at_date`) | Fix the code; remove from candidates |
| **Non-Opera prefix** (e.g. `db_*`, `pg_*`, `dm_*`, `as_*`) | Add to validator's `SKIP_PREFIXES` constant |
| **Local SQLite column** (e.g. `zc_*` for `zcontacts`) | Add to validator's `SKIP_PREFIXES` |
| **Python variable, not SQL** | Suppress in YAML with reason |
| **Aliased column** (`SELECT x AS y`) | Already filtered — verify the alias filter catches it |

Output of triage: zero unsuppressed unknown columns. Then strict mode flips.

### 4. Test suite

`tests/test_validate_sql_columns.py`:
- **Fixture-based:** small fake `opera_snapshot.json` and a fake source file. Validator runs against fixtures, asserts exact findings.
- **Regression cases:** the typos we caught today (`pt_ref`, `st_ref`, `at_date`) — assert each is flagged with the right "did you mean" suggestion.
- **Suppression test:** a suppressed identifier doesn't appear in findings; an unsuppressed one does.
- **Alias filter test:** `SELECT x AS y` doesn't flag `y`.
- **Performance test:** runs in <10s on the real repo (not a fixture — actual scan).

### 5. CI integration

**GitHub Actions workflow** at `.github/workflows/sql-validator.yml`:
- Triggers: every push to a PR branch, every push to `main`.
- Runs: `python scripts/validate_sql_columns.py --strict`.
- Failure mode: PR is blocked from merge.
- Output: posts findings as a PR comment for fast triage.

**Pre-commit hook** at `.pre-commit-config.yaml` (already exists in many repos — confirm or add):
- Runs the validator on staged Python files only (faster than full scan).
- Refuses commit on unknown columns.
- Bypassable with `--no-verify` for emergencies (logged in commit message).

## Data flow

```
1. Developer edits foo.py adding `SELECT bar_x FROM ...`
2. Pre-commit hook runs validator on foo.py (only)
3. Validator extracts `bar_x` from the SQL string
4. Looks up against opera_snapshot.json
5. Not found → checks suppressions.yaml
6. Not suppressed → reports finding, exit 1, blocks commit
7. Developer either: fixes the typo, or adds a suppression with reason
```

## Error handling

- Snapshot file missing: validator exits 2 with clear message ("Run `scripts/snapshot_opera_schema.py` first"). Never silently passes.
- Suppression file malformed: validator exits 3 with line number. Never silently ignores.
- File unreadable: warned but doesn't block other files (one bad file ≠ skip whole repo).

## Testing strategy

- Unit tests: fixtures + assertions (above).
- Integration test: run the validator against the real repo and assert specific known-good output (zero findings after suppression file is populated).
- CI dry-run: open a PR introducing a deliberate typo, verify CI blocks it.

## Migration plan

1. Write fixture-based unit tests (TDD — test first, then code).
2. Harden the validator (`--strict` exit code, suppression loading).
3. Triage current 172 findings → fix real bugs (commits per fix) and populate suppression YAML.
4. Confirm zero unsuppressed findings.
5. Wire CI workflow + pre-commit hook.
6. Open a deliberately-broken test PR to verify CI blocks it.
7. Close the test PR; strict mode is live.

## Out of scope (separate spec if needed)

- **Opera 3 (DBF) field validation.** Different storage format, different schema source. Will be its own follow-up.
- **Validating non-Opera SQLite schemas** (`bank_aliases.db`, `email_data.db`, etc.). Different scope.
- **Generating the snapshot** itself — `scripts/snapshot_opera_schema.py` already exists.

## Done criteria

- [ ] All tests in `tests/test_validate_sql_columns.py` pass.
- [ ] `python scripts/validate_sql_columns.py --strict` exits 0 on clean main.
- [ ] CI workflow blocks a PR introducing a deliberate typo.
- [ ] Pre-commit hook blocks a local commit introducing a deliberate typo.
- [ ] All 172 current candidates classified: real bugs fixed, false positives suppressed with reason.
- [ ] No `# noqa: validator` style escape hatches inside source code.
- [ ] Documentation in repo README pointing devs at the suppression process.

## Risks / failure modes

- **Triage rabbit hole:** the 172 candidates may surface additional real bugs that themselves need fixing. **Mitigation:** each real bug is fixed in its own commit; we don't bundle.
- **False positives:** the validator may flag legitimate uses (especially aliases or computed columns). **Mitigation:** suppression YAML with required reason. Bias toward suppressing rather than weakening the validator.
- **CI flakiness:** validator must be deterministic. **Mitigation:** sorted output, no time-dependent behaviour, no network calls.
