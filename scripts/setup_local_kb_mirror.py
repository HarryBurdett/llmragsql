"""One-shot installer: replace the local field-reference copy with a
symlink to the central KB version.

Drift is impossible by construction once the symlink is in place — the
local path always reads the central file.

Idempotent: safe to run twice. If the local path is already a symlink
pointing at the right target, the script does nothing.

Failure modes:
  - Central repo not cloned: prints clear instructions and exits 1.
  - Local path is not a regular file or existing symlink: refuses to
    touch it (could be a directory).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_PATH = REPO_ROOT / "apps" / "core" / "docs" / "opera_transaction_field_reference.md"
CENTRAL_PATH = Path(os.path.expanduser(
    "~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md"
))


def main() -> int:
    if not CENTRAL_PATH.exists():
        print(f"ERROR: central KB file not found at {CENTRAL_PATH}", file=sys.stderr)
        print("Clone the central knowledge repo:", file=sys.stderr)
        print("  git clone https://github.com/jonathangintsys/aisam.git ~/opera-knowledge-ref", file=sys.stderr)
        return 1

    if LOCAL_PATH.is_symlink():
        target = LOCAL_PATH.resolve()
        if target == CENTRAL_PATH.resolve():
            print(f"OK: {LOCAL_PATH} already symlinked to central.")
            return 0
        print(f"NOTE: {LOCAL_PATH} is a symlink but points at {target}.")
        print("Replacing with link to central.")
        LOCAL_PATH.unlink()
    elif LOCAL_PATH.is_file():
        print(f"NOTE: {LOCAL_PATH} is a regular file. Backing up to .bak and replacing with symlink.")
        backup = LOCAL_PATH.with_suffix(".md.bak")
        LOCAL_PATH.rename(backup)
        print(f"  backup: {backup}")
    elif LOCAL_PATH.exists():
        print(f"ERROR: {LOCAL_PATH} exists and is neither a file nor symlink.", file=sys.stderr)
        return 1

    LOCAL_PATH.symlink_to(CENTRAL_PATH)
    print(f"OK: {LOCAL_PATH} → {CENTRAL_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
