"""Contract tests for bank reconciliation completion.

Pins the correct behaviour and prevents regression to the buggy patterns
(MAX(ae_reclnum)+1, flat ae_recbal, missing nk_reclnum updates) that
caused the Cloudsis batch 209 incident on 2026-05-04.

Background: there are three places where aentry.ae_reclnum used to get
written.

    Path A: apps/bank_reconcile/api/routes.py — /api/reconcile/bank/
            {bank_code}/confirm-matches endpoint
    Path B: sql_rag/statement_reconcile.py — StatementReconciler.reconcile_matches()
    Path C: sql_rag/opera_sql_import.py — OperaSQLImport.mark_entries_reconciled()
            (mirrored in sql_rag/opera3_foxpro_import.py)

Path C is the only correct implementation. Path A and B used MAX+1
(violating CLAUDE.md's mandatory rule to use nbank counters), set every
entry's ae_recbal to a flat statement_balance (no running balance), and
never updated nk_reclnum/nk_recldte (leaving Opera in a half-done state).

These tests:

  1. Forbid MAX(ae_reclnum) and MAX(nt_jrnl)/MAX(ae_entry) patterns in
     route handlers and statement_reconcile.py — the central rule from
     CLAUDE.md.

  2. Forbid Path B (reconcile_matches method) from existing as a
     callable code path; it must be deleted or re-pointed at Path C.

  3. Require Path A (the route handler) to delegate to Path C rather
     than re-implementing the rec write itself.

  4. Pin Path C's contract: it reads ae_reclnum from nbank, computes a
     running ae_recbal per entry, and updates every Stage B field on
     nbank (nk_recbal, nk_reclnum, nk_recldte, nk_lstrecl, nk_lststno,
     nk_lststdt, nk_recstdt, nk_recstfr, nk_recstto, nk_recstln,
     nk_reccfwd).
"""

import re
import textwrap
from pathlib import Path

ROUTES_FILE = Path(__file__).parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
RECONCILE_FILE = Path(__file__).parent.parent / "sql_rag" / "statement_reconcile.py"
OPERA_SQL_IMPORT = Path(__file__).parent.parent / "sql_rag" / "opera_sql_import.py"
OPERA3_FOXPRO_IMPORT = Path(__file__).parent.parent / "sql_rag" / "opera3_foxpro_import.py"


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Rule: no MAX(...)+1 sequence allocation in finance flows.
# ---------------------------------------------------------------------------

def _strip_comments_only(src: str) -> str:
    """Return source with single-line Python/SQL comments stripped.

    Do NOT strip triple-quoted strings: in this codebase the buggy SQL
    is interpolated inside `f\"\"\" ... \"\"\"` blocks, and stripping those
    would hide the bug from the test.

    Comments (Python `#` and SQL `--`) are stripped so that a legitimate
    comment discussing the historical bug doesn't trigger a false positive.
    """
    src = re.sub(r"#[^\n]*", "", src)
    src = re.sub(r"--[^\n]*", "", src)
    return src


# Match MAX(ae_reclnum) followed within 30 chars by + 1 — covers
# ISNULL(MAX(ae_reclnum), 0) + 1 and similar wrappings.
_MAX_PLUS_ONE = re.compile(
    r"MAX\(\s*ae_reclnum\s*\)[\s\S]{0,30}\+\s*1",
    re.IGNORECASE,
)


def test_routes_forbid_max_reclnum_plus_one():
    """The route handler must NOT compute the next batch number with MAX+1.

    Per CLAUDE.md: rec batch numbers come from nbank.nk_lstrecl (the
    canonical Opera counter), the same way journal numbers come from
    nparm.np_nexjrnl and entry numbers from atype.
    """
    src = _strip_comments_only(_read(ROUTES_FILE))
    matches = _MAX_PLUS_ONE.findall(src)
    assert not matches, (
        "apps/bank_reconcile/api/routes.py contains MAX(ae_reclnum)+1 — "
        "violates CLAUDE.md mandatory rule. Use nbank.nk_lstrecl as the "
        "rec batch counter. Found patterns: " + str(matches)
    )


def test_statement_reconcile_forbids_max_reclnum_plus_one():
    """sql_rag/statement_reconcile.py must NOT compute batch via MAX+1.

    Same rule. The legacy `reconcile_matches()` method used this pattern;
    it must be removed or rewritten to delegate to OperaSQLImport.
    mark_entries_reconciled().
    """
    src = _strip_comments_only(_read(RECONCILE_FILE))
    matches = _MAX_PLUS_ONE.findall(src)
    assert not matches, (
        "sql_rag/statement_reconcile.py contains MAX(ae_reclnum)+1 — "
        "violates CLAUDE.md mandatory rule. Found: " + str(matches)
    )


# ---------------------------------------------------------------------------
# Rule: Path B (reconcile_matches) must not exist as a callable method.
# ---------------------------------------------------------------------------

def test_path_b_reconcile_matches_is_removed():
    """The legacy `StatementReconciler.reconcile_matches` method must be
    removed (or re-pointed at Path C).

    It had no callers in the codebase but was a footgun for anyone
    copy-pasting from it. Confirmed dead code on 2026-05-04 audit.
    """
    src = _read(RECONCILE_FILE)
    # Match the method definition (allow whitespace differences).
    assert re.search(r"def\s+reconcile_matches\s*\(", src) is None, (
        "sql_rag/statement_reconcile.py still defines reconcile_matches() — "
        "delete this method (zero callers, contains the buggy MAX+1 pattern). "
        "Any new caller must use OperaSQLImport.mark_entries_reconciled()."
    )


# ---------------------------------------------------------------------------
# Rule: Path A (the route handler) delegates to Path C.
# ---------------------------------------------------------------------------

def test_confirm_matches_endpoint_delegates_to_mark_entries_reconciled():
    """The /confirm-matches endpoint must delegate to
    OperaSQLImport.mark_entries_reconciled() instead of writing aentry/
    nbank itself.

    Method: locate the function body and require that it calls
    `mark_entries_reconciled`. Also check that it does NOT contain the
    direct UPDATE patterns that bypass Path C.
    """
    src = _read(ROUTES_FILE)
    # Find the confirm_statement_matches function body
    fn_match = re.search(
        r"async def confirm_statement_matches\([^)]*\):([\s\S]+?)(?=\n@router\.|\nasync def\s|\ndef\s)",
        src,
    )
    assert fn_match, "Could not locate confirm_statement_matches() in routes.py"
    body = fn_match.group(1)

    assert "mark_entries_reconciled" in body, (
        "/confirm-matches must delegate to OperaSQLImport.mark_entries_reconciled() "
        "(Path C). Currently it does its own UPDATEs which use the buggy MAX+1 pattern."
    )

    # Specifically forbid direct UPDATE aentry SET ae_reclnum in this function
    body_no_strings = _strip_comments_only(body)
    direct_update = re.search(
        r"UPDATE\s+aentry\s+[\s\S]{0,200}?SET\s+[\s\S]{0,200}?ae_reclnum\s*=",
        body_no_strings,
        re.IGNORECASE,
    )
    assert direct_update is None, (
        "confirm_statement_matches() contains a direct UPDATE aentry SET ae_reclnum. "
        "Remove it — delegate to mark_entries_reconciled() instead."
    )


# ---------------------------------------------------------------------------
# Rule: Path C updates every Stage B field on nbank.
# ---------------------------------------------------------------------------

REQUIRED_NBANK_FIELDS_FOR_FULL_REC = (
    "nk_recbal",
    "nk_reclnum",
    "nk_recldte",
    "nk_lstrecl",
    "nk_lststno",
    "nk_lststdt",
    "nk_recstdt",
    "nk_recstfr",
    "nk_recstto",
    "nk_recstln",
    "nk_reccfwd",
)


def test_opera_sql_import_full_rec_updates_all_stage_b_fields():
    """The full (non-partial) reconciliation branch in
    OperaSQLImport.mark_entries_reconciled must update every Stage B
    field on nbank.

    Cloudsis batch 209 had nk_reclnum=0, nk_recldte=NULL, nk_lststdt
    stale because earlier code paths missed these. This test pins that
    Path C does it right.
    """
    src = _read(OPERA_SQL_IMPORT)
    fn = re.search(
        r"def mark_entries_reconciled\(([\s\S]+?)(?=\n    def |\nclass )",
        src,
    )
    assert fn, "Could not find mark_entries_reconciled in opera_sql_import.py"
    body = fn.group(1)

    # Locate the full-rec UPDATE nbank block: between the `else:` after
    # `if partial:` and the closing of the SQL string.
    full_rec_match = re.search(
        r"else:[\s\S]+?UPDATE\s+nbank[\s\S]+?WHERE\s+nk_acnt",
        body,
        re.IGNORECASE,
    )
    assert full_rec_match, (
        "Could not locate the full-rec UPDATE nbank block in mark_entries_reconciled."
    )
    full_rec_sql = full_rec_match.group(0)

    missing = [f for f in REQUIRED_NBANK_FIELDS_FOR_FULL_REC if f not in full_rec_sql]
    assert not missing, (
        "OperaSQLImport.mark_entries_reconciled (full rec branch) does not update "
        f"these required Stage B fields: {missing}. Without them Opera reports "
        f"will show stale 'last reconciled' info. Required: "
        f"{REQUIRED_NBANK_FIELDS_FOR_FULL_REC}"
    )


def test_opera_sql_import_uses_running_balance_not_flat_statement_balance():
    """Path C must compute a per-entry running balance, never a flat
    `ae_recbal = statement_balance × 100` for every entry.

    The flat pattern was the Path A/B bug; this test pins that Path C
    walks entries in statement-line order and accumulates.
    """
    src = _read(OPERA_SQL_IMPORT)
    fn = re.search(
        r"def mark_entries_reconciled\(([\s\S]+?)(?=\n    def |\nclass )",
        src,
    )
    assert fn, "mark_entries_reconciled not found"
    body = fn.group(1)

    # Must compute a running_balance variable
    assert re.search(r"running_balance\s*[+\-]?=", body), (
        "Path C must accumulate a running_balance — flat ae_recbal would put "
        "the same closing-balance value on every reconciled entry."
    )
    # ae_recbal must reference running_balance (or an equivalent per-entry
    # variable), not statement_balance directly.
    assert re.search(r"ae_recbal\s*=\s*\{?int\(running_balance\)?", body) or \
           re.search(r"ae_recbal\s*=\s*\{?int\(entry_rec_bal\)?", body), (
        "ae_recbal must come from the running balance, not from the closing "
        "statement_balance."
    )


def test_opera3_mark_entries_reconciled_uses_running_balance():
    """Opera 3 parity: same correct pattern in opera3_foxpro_import.py."""
    src = _read(OPERA3_FOXPRO_IMPORT)
    fn = re.search(
        r"def mark_entries_reconciled\(([\s\S]+?)(?=\n    def |\nclass )",
        src,
    )
    assert fn, "mark_entries_reconciled not found in opera3_foxpro_import.py"
    body = fn.group(1)
    assert re.search(r"running_balance\s*[+\-]?=", body), (
        "Opera 3 mark_entries_reconciled must accumulate a running_balance — "
        "same rule as the SE version."
    )
