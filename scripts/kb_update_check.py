"""Pre-commit + CI hook: refuse Opera-related code changes that don't
update the knowledge base (or carry an explicit kb-not-required:
annotation in the commit message).

Exit codes:
  0 — staged files satisfy the rule (or no Opera files touched).
  1 — Opera files touched, no KB update staged, no annotation.
  2 — configuration error (allowlist malformed / missing).

Usage as pre-commit hook:
  python scripts/kb_update_check.py

Usage with explicit file list (tests + CI):
  python scripts/kb_update_check.py --files sql_rag/opera_sql_import.py ...

Usage with custom allowlist:
  python scripts/kb_update_check.py --allowlist <path>

Commit message source:
  - Default: read from .git/COMMIT_EDITMSG (pre-commit hook context).
  - Override: GIT_COMMIT_MESSAGE env var (test convenience).
  - Override: --commit-msg-file <path> (CI convenience).
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


try:
    import yaml  # type: ignore
except ImportError:
    yaml = None


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ALLOWLIST = REPO_ROOT / "scripts" / "kb_update_allowlist.yaml"


def _load_allowlist(path: Path) -> dict:
    if yaml is None:
        print("ERROR: PyYAML required to read the allowlist.", file=sys.stderr)
        return {}
    if not path.exists():
        print(f"ERROR: allowlist not found at {path}", file=sys.stderr)
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        print(f"ERROR: malformed YAML in {path}: {e}", file=sys.stderr)
        return {}
    if not isinstance(data, dict):
        print(f"ERROR: {path} must be a mapping at top level", file=sys.stderr)
        return {}
    if "opera_files" not in data or "kb_files" not in data:
        print(f"ERROR: {path} must have 'opera_files' and 'kb_files' lists", file=sys.stderr)
        return {}
    return data


def _staged_files() -> List[str]:
    """Return git index-staged file paths (relative to repo root)."""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _matches_any(path: str, globs: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, g) for g in globs)


def _read_commit_message(commit_msg_file: Optional[str]) -> str:
    """Read the commit message body for the kb-not-required annotation.

    Sources, in order: --commit-msg-file, GIT_COMMIT_MESSAGE env,
    .git/COMMIT_EDITMSG, empty.
    """
    if commit_msg_file:
        try:
            return Path(commit_msg_file).read_text(encoding="utf-8")
        except OSError:
            return ""
    env_msg = os.environ.get("GIT_COMMIT_MESSAGE")
    if env_msg is not None:
        return env_msg
    editmsg = REPO_ROOT / ".git" / "COMMIT_EDITMSG"
    if editmsg.exists():
        try:
            return editmsg.read_text(encoding="utf-8")
        except OSError:
            return ""
    return ""


def _has_valid_annotation(commit_message: str) -> bool:
    """True if the commit message has a `kb-not-required:` line followed
    by at least one non-whitespace character (the reason).
    """
    for line in commit_message.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("kb-not-required:"):
            after = stripped.split(":", 1)[1].strip()
            return bool(after)
    return False


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--files", nargs="*", default=None,
        help="Explicit file list (test/CI convenience). If absent, reads `git diff --cached`.",
    )
    parser.add_argument(
        "--allowlist", type=Path, default=DEFAULT_ALLOWLIST,
        help="Path to allowlist YAML.",
    )
    parser.add_argument(
        "--commit-msg-file", type=str, default=None,
        help="Path to a commit message file (overrides .git/COMMIT_EDITMSG).",
    )
    args = parser.parse_args(argv)

    allowlist = _load_allowlist(args.allowlist)
    if not allowlist:
        return 2

    opera_globs = allowlist.get("opera_files", [])
    kb_globs = allowlist.get("kb_files", [])

    files = args.files if args.files is not None else _staged_files()
    if not files:
        return 0  # nothing staged

    opera_hits = [f for f in files if _matches_any(f, opera_globs)]
    if not opera_hits:
        return 0  # no Opera-related changes

    kb_hits = [f for f in files if _matches_any(f, kb_globs)]
    if kb_hits:
        return 0  # KB updated alongside the code change

    commit_message = _read_commit_message(args.commit_msg_file)
    if _has_valid_annotation(commit_message):
        # Log the bypass for audit
        print(
            "OK: kb-not-required annotation present; bypass logged.",
            file=sys.stderr,
        )
        return 0

    # Failure path: explain remediation
    print("✗ KB update required for Opera-related changes:", file=sys.stderr)
    for f in opera_hits:
        print(f"    {f}", file=sys.stderr)
    print(file=sys.stderr)
    print("Required action — choose one:", file=sys.stderr)
    print(
        "  1. Update apps/core/docs/opera_knowledge_base.md (and central KB) "
        "with a new section describing what changed, then re-stage.",
        file=sys.stderr,
    )
    print(
        "  2. Add 'kb-not-required: <specific reason>' to the commit message "
        "body explaining why this change does not represent new Opera knowledge.",
        file=sys.stderr,
    )
    print(file=sys.stderr)
    print(
        "Directory taxonomy for central KB:",
        file=sys.stderr,
    )
    print("  - schema/        → table/column documentation", file=sys.stderr)
    print("  - business-rules/→ posting rules, conventions, locking, sequence numbers", file=sys.stderr)
    print("  - query-patterns/→ reusable query examples", file=sys.stderr)
    print("  - transaction-library/ → snapshot JSON (auto-managed)", file=sys.stderr)
    print(file=sys.stderr)
    print("See docs/kb-update-guide.md for full rules.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
