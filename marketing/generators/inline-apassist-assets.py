#!/usr/bin/env python3
"""Inline every asset into the APAssist commercial presentation.

The deck ships as a single self-contained HTML file — it is also published as an
Artifact, where a strict CSP blocks any external request, so fonts, logos and
screenshots must all be data URIs. Placeholders live in the deck as {{TOKEN}}
and this script fills them in.

Re-run after regenerating the screens from apassist-screens.html, or after
refreshing the brand assets in ../brand/.

Brand assets come from the live crakd.ai site:
    curl -o ../brand/logo-crakd-ai.png      https://crakd.ai/logo-crakd-ai.png
    curl -o ../brand/pegasus-opera-logo.png https://crakd.ai/pegasus-opera-logo.png
plus the two variable fonts the site loads from Google Fonts (Plus Jakarta Sans,
JetBrains Mono) saved as ../brand/jakarta.woff2 and ../brand/jetbrains.woff2.
"""
import base64
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent
SHOTS = HERE.parent / "screenshots"
BRAND = HERE.parent / "brand"
DECK = HERE.parent / "demos" / "apassist-commercial-presentation.html"

# token -> (path, mime)
ASSETS = {
    "{{FONT_JAKARTA}}":   (BRAND / "jakarta.woff2",            "font/woff2"),
    "{{FONT_JETBRAINS}}": (BRAND / "jetbrains.woff2",          "font/woff2"),
    "{{LOGO_CRAKD}}":     (BRAND / "logo-crakd-ai.png",        "image/png"),
    "{{LOGO_OPERA}}":     (BRAND / "pegasus-opera-logo.png",   "image/png"),
    "{{IMG_DASHBOARD}}":  (SHOTS / "apassist-dashboard.png",        "image/png"),
    "{{IMG_STATEMENT}}":  (SHOTS / "apassist-statement-detail.png", "image/png"),
    "{{IMG_ENQUIRY}}":    (SHOTS / "apassist-enquiry-detail.png",   "image/png"),
    "{{IMG_AGED}}":       (SHOTS / "apassist-aged-creditors.png",   "image/png"),
    "{{IMG_UNASSIGNED}}": (SHOTS / "apassist-unassigned-email.png", "image/png"),
}

if not DECK.exists():
    sys.exit(f"deck not found: {DECK}")

html = DECK.read_text(encoding="utf-8")

missing = [str(p) for p, _ in ASSETS.values() if not p.exists()]
if missing:
    sys.exit("missing assets:\n  " + "\n  ".join(missing))

for token, (path, mime) in ASSETS.items():
    if token not in html:
        print(f"  note: {token} already inlined or absent")
        continue
    b64 = base64.b64encode(path.read_bytes()).decode()
    html = html.replace(token, f"data:{mime};base64,{b64}")
    print(f"  inlined {path.name:34s} {len(b64) // 1024:5d} KB base64")

DECK.write_text(html, encoding="utf-8")

left = re.findall(r"\{\{[A-Z_]+\}\}", html)
size = DECK.stat().st_size // 1024
print(f"done: {DECK.name} is {size} KB, {len(left)} placeholders left")
if left:
    sys.exit(f"unfilled placeholders: {sorted(set(left))}")
if size > 16 * 1024:
    sys.exit(f"deck is {size} KB — over the 16 MB Artifact limit")
