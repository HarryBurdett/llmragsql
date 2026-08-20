#!/usr/bin/env python3
"""Screenshot data URIs for the decks, resolved once.

Kept separate so a deck module can pull an image without importing the builder
(which imports the deck modules — that would be circular).

The bank-reconcile-* images are the existing house workflow illustrations:
Playwright renders of hand-built HTML in the Finance Hub style, produced by
generate_screenshots.py. They are illustrations, not captures of the live app,
and the decks caption them that way.
"""
import base64
import pathlib
import sys

SHOTS_DIR = pathlib.Path(__file__).parent.parent / "screenshots"

_FILES = {
    "select": "bank-reconcile-1-select.png",
    "review": "bank-reconcile-2-review.png",
    "import": "bank-reconcile-3-import.png",
    "reconcile": "bank-reconcile-4-reconcile.png",
    "complete": "bank-reconcile-5-complete.png",
}


def _uri(name: str) -> str:
    path = SHOTS_DIR / name
    if not path.exists():
        sys.exit(f"missing screenshot: {path}")
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


SHOT = {key: _uri(name) for key, name in _FILES.items()}
