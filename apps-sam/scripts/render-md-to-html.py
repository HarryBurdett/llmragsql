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
