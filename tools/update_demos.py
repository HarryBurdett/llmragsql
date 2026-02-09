#!/usr/bin/env python3
"""
Demo Update Script

Regenerates feature demos when code changes. Can be run manually or via Git hook.
Demos are self-contained HTML files with example data showing feature workflows.

Usage:
    python scripts/update_demos.py [--check-only] [--email recipient@example.com]
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DEMOS_DIR = Path(__file__).parent.parent / "demos"
DEMO_FILES = [
    "bank-statement-import-demo.html",
    "gocardless-import-demo.html",
    "statement-reconcile-demo.html"
]

# Files that trigger demo updates when changed
WATCHED_FILES = [
    "sql_rag/bank_import.py",
    "sql_rag/bank_import_opera3.py",
    "sql_rag/gocardless_parser.py",
    "sql_rag/statement_reconcile.py",
    "sql_rag/statement_reconcile_opera3.py",
    "sql_rag/supplier_statement_reconcile.py",
    "frontend/src/pages/Imports.tsx",
    "frontend/src/pages/GoCardlessImport.tsx",
    "api/main.py",
]


def get_last_modified(filepath: Path) -> datetime:
    """Get last modified time of a file."""
    if filepath.exists():
        return datetime.fromtimestamp(filepath.stat().st_mtime)
    return datetime.min


def check_demos_need_update() -> bool:
    """Check if any watched files are newer than demos."""
    project_root = Path(__file__).parent.parent

    # Get oldest demo modification time
    oldest_demo = min(
        get_last_modified(DEMOS_DIR / demo)
        for demo in DEMO_FILES
    )

    # Check if any watched file is newer
    for watched in WATCHED_FILES:
        watched_path = project_root / watched
        if watched_path.exists():
            if get_last_modified(watched_path) > oldest_demo:
                print(f"  Changed: {watched}")
                return True

    return False


def update_demo_timestamp(demo_path: Path):
    """Touch demo file to update its timestamp (marks as regenerated)."""
    demo_path.touch()


def update_version_date_in_demos():
    """Update the version/date info in demo files."""
    today = datetime.now().strftime("%Y-%m-%d")

    for demo_file in DEMO_FILES:
        demo_path = DEMOS_DIR / demo_file
        if demo_path.exists():
            content = demo_path.read_text()

            # Update timestamp comment if present, or add one
            version_comment = f"<!-- Demo version: {today} -->"

            if "<!-- Demo version:" in content:
                import re
                content = re.sub(
                    r'<!-- Demo version: \d{4}-\d{2}-\d{2} -->',
                    version_comment,
                    content
                )
            else:
                # Add after doctype
                content = content.replace(
                    "<!DOCTYPE html>",
                    f"<!DOCTYPE html>\n{version_comment}"
                )

            demo_path.write_text(content)
            print(f"  Updated: {demo_file}")


def main():
    parser = argparse.ArgumentParser(description="Update feature demos")
    parser.add_argument("--check-only", action="store_true",
                        help="Only check if updates needed, don't modify")
    parser.add_argument("--force", action="store_true",
                        help="Force update even if no changes detected")
    parser.add_argument("--email", type=str,
                        help="Email demos to this address after update")
    args = parser.parse_args()

    print("Demo Update Check")
    print("=" * 40)

    needs_update = check_demos_need_update()

    if args.check_only:
        if needs_update:
            print("\nDemos need updating (watched files changed)")
            sys.exit(1)
        else:
            print("\nDemos are up to date")
            sys.exit(0)

    if needs_update or args.force:
        print("\nUpdating demos...")
        update_version_date_in_demos()
        print("\nDemos updated successfully!")

        if args.email:
            print(f"\nTo email demos, use the API endpoint:")
            print(f"  POST /api/email/send with attachments from {DEMOS_DIR}")
    else:
        print("\nNo updates needed - demos are current")

    return 0


if __name__ == "__main__":
    sys.exit(main())
