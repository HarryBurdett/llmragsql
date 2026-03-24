#!/usr/bin/env python3
"""
Generate realistic bank reconciliation screenshots matching the Intsys Finance Hub design.
Uses Playwright to render self-contained HTML pages and capture as PNGs.

Design system: Dark sidebar (rgb(15,23,41)), white content area, shadcn/ui cards,
compact typography, colored KPI top borders, system-ui font stack.

Output: demos/screenshots/bank-reconcile-*.png
"""

import base64
from pathlib import Path
from playwright.sync_api import sync_playwright

DEMOS_DIR = Path(__file__).parent
SCREENSHOTS_DIR = DEMOS_DIR / "screenshots"
LOGO_PATH = Path(__file__).parent.parent / "frontend" / "public" / "opera-se-logo.png"

logo_b64 = ""
if LOGO_PATH.exists():
    logo_b64 = base64.b64encode(LOGO_PATH.read_bytes()).decode()


def shared_styles():
    return """<style>
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body { font-family: ui-sans-serif, system-ui, sans-serif, "Apple Color Emoji", "Segoe UI Emoji"; background: #f8fafc; color: #020817; display: flex; min-height: 100vh; }

      /* Sidebar */
      .sidebar { width: 240px; background: #0f1729; color: #f8fafc; display: flex; flex-direction: column; flex-shrink: 0; }
      .sidebar-brand { padding: 16px 20px; display: flex; align-items: center; gap: 10px; border-bottom: 1px solid rgba(255,255,255,0.06); }
      .sidebar-brand-icon { width: 32px; height: 32px; background: linear-gradient(135deg, #3b82f6, #6366f1); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 14px; color: white; }
      .sidebar-brand-text { font-size: 13px; font-weight: 600; }
      .sidebar-brand-sub { font-size: 10px; color: #94a3b8; }
      .sidebar-nav { padding: 12px 8px; flex: 1; }
      .sidebar-item { display: flex; align-items: center; gap: 10px; padding: 8px 12px; border-radius: 8px; font-size: 12px; color: #94a3b8; cursor: pointer; margin-bottom: 2px; }
      .sidebar-item.active { background: rgba(59,130,246,0.15); color: #60a5fa; }
      .sidebar-item:hover:not(.active) { background: rgba(255,255,255,0.04); }
      .sidebar-icon { width: 16px; height: 16px; opacity: 0.7; }

      /* Header */
      .topbar { height: 48px; background: white; border-bottom: 1px solid #e2e8f0; display: flex; align-items: center; padding: 0 16px; gap: 12px; }
      .topbar-breadcrumb { display: flex; align-items: center; gap: 6px; font-size: 12px; color: #64748b; }
      .topbar-breadcrumb .sep { color: #d1d5db; }
      .topbar-breadcrumb .current { color: #1e293b; font-weight: 500; }
      .topbar-right { margin-left: auto; display: flex; align-items: center; gap: 10px; }
      .topbar-badge { font-size: 10px; background: #dbeafe; color: #1e40af; padding: 2px 8px; border-radius: 9999px; font-weight: 500; }
      .topbar-avatar { width: 28px; height: 28px; background: #3b82f6; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; color: white; }

      /* Main content */
      .main { flex: 1; display: flex; flex-direction: column; }
      .content { padding: 16px 20px; flex: 1; }

      /* Page heading */
      .page-heading { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
      .page-heading h1 { font-size: 14px; font-weight: 700; color: #1e293b; letter-spacing: -0.01em; }

      /* Stage stepper */
      .stepper { display: flex; align-items: center; gap: 0; margin-bottom: 16px; background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 8px 12px; }
      .step { display: flex; align-items: center; gap: 6px; font-size: 11px; color: #94a3b8; font-weight: 500; padding: 4px 8px; }
      .step.active { color: #2563eb; }
      .step.done { color: #059669; }
      .step-dot { width: 20px; height: 20px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 600; background: #f1f5f9; color: #94a3b8; }
      .step.active .step-dot { background: #2563eb; color: white; }
      .step.done .step-dot { background: #059669; color: white; }
      .step-sep { color: #d1d5db; font-size: 10px; margin: 0 2px; }

      /* KPI Cards row */
      .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 16px; }
      .kpi { background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 10px 12px; position: relative; overflow: hidden; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
      .kpi::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; border-radius: 10px 10px 0 0; }
      .kpi.green::before { background: #10b981; }
      .kpi.amber::before { background: #f59e0b; }
      .kpi.red::before { background: #f43f5e; }
      .kpi.blue::before { background: #3b82f6; }
      .kpi-label { font-size: 10px; color: #64748b; font-weight: 500; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 4px; }
      .kpi-value { font-size: 18px; font-weight: 700; letter-spacing: -0.02em; }
      .kpi-change { font-size: 10px; font-weight: 500; margin-top: 2px; }
      .kpi-change.up { color: #059669; }
      .kpi-change.down { color: #dc2626; }

      /* Cards */
      .card { background: white; border: 1px solid #e2e8f0; border-radius: 10px; overflow: hidden; margin-bottom: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
      .card-head { padding: 10px 14px; border-bottom: 1px solid #f1f5f9; display: flex; align-items: center; justify-content: space-between; }
      .card-head h3 { font-size: 12px; font-weight: 600; color: #020817; letter-spacing: -0.01em; }
      .card-body { padding: 0; }

      /* Tables */
      table { width: 100%; border-collapse: collapse; font-size: 11px; }
      thead th { background: #fafbfc; padding: 8px 12px; text-align: left; font-size: 10px; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid #e2e8f0; }
      tbody td { padding: 7px 12px; border-bottom: 1px solid #f1f5f9; color: #334155; }
      tbody tr:hover { background: #fafbfc; }
      .pos { color: #059669; font-weight: 600; font-variant-numeric: tabular-nums; }
      .neg { color: #dc2626; font-weight: 600; font-variant-numeric: tabular-nums; }
      .neutral { color: #334155; font-weight: 600; font-variant-numeric: tabular-nums; }

      /* Badges */
      .badge { display: inline-flex; align-items: center; gap: 3px; padding: 1px 6px; border-radius: 9999px; font-size: 10px; font-weight: 500; }
      .badge-green { background: #dcfce7; color: #166534; }
      .badge-amber { background: #fef3c7; color: #92400e; }
      .badge-red { background: #fef2f2; color: #991b1b; }
      .badge-blue { background: #dbeafe; color: #1e40af; }
      .badge-gray { background: #f1f5f9; color: #475569; }

      /* Banners */
      .banner { display: flex; align-items: center; gap: 8px; padding: 10px 12px; border-radius: 8px; margin-bottom: 12px; font-size: 11px; }
      .banner.blue { background: #eff6ff; border: 1px solid #bfdbfe; color: #1e40af; }
      .banner.green { background: #f0fdf4; border: 1px solid #bbf7d0; color: #166534; }

      /* Buttons */
      .btn { padding: 5px 12px; border-radius: 6px; font-size: 11px; font-weight: 500; border: none; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; }
      .btn-primary { background: #2563eb; color: white; }
      .btn-outline { background: white; color: #334155; border: 1px solid #e2e8f0; }
      .btn-green { background: #059669; color: white; }

      /* Checkbox */
      .ck { width: 14px; height: 14px; border-radius: 3px; border: 1.5px solid #d1d5db; background: white; display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; }
      .ck.on { background: #2563eb; border-color: #2563eb; }
      .ck.on::after { content: '\\2713'; color: white; font-size: 9px; font-weight: 700; }

      /* Line number */
      .ln { display: inline-flex; align-items: center; justify-content: center; width: 20px; height: 20px; background: #dbeafe; color: #1e40af; font-size: 10px; font-weight: 600; border-radius: 4px; }

      .matched-row { background: #f0fdf4; }
      .matched-row td { color: #166534; }

      /* Success */
      .success-icon { width: 48px; height: 48px; background: #dcfce7; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin: 0 auto 12px; }
      .success-icon svg { width: 24px; height: 24px; color: #059669; }

      .footer-bar { display: flex; justify-content: flex-end; gap: 8px; padding: 8px 0; }
    </style>"""


def sidebar_html(active="Cashbook"):
    items = [
        ("Overview", "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0h4"),
        ("Sales Ledger", "M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0"),
        ("Purchase Ledger", "M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"),
        ("Nominal Ledger", "M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"),
        ("Cashbook", "M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3"),
        ("Sales Orders", "M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"),
        ("Purchase Orders", "M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17"),
        ("Stock", "M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"),
        ("Fixed Assets", "M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"),
        ("Settings", "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"),
    ]
    logo = f'<img src="data:image/png;base64,{logo_b64}" style="height:26px;" />' if logo_b64 else ''
    nav = ""
    for name, path_d in items:
        cls = "sidebar-item active" if name == active else "sidebar-item"
        nav += f'''<div class="{cls}">
            <svg class="sidebar-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="{path_d}"/></svg>
            {name}</div>'''

    return f"""<aside class="sidebar">
        <div class="sidebar-brand">{logo}
            <div><div class="sidebar-brand-text">Finance Hub</div><div class="sidebar-brand-sub">Opera Integration</div></div>
        </div>
        <nav class="sidebar-nav">{nav}</nav>
    </aside>"""


def topbar_html(crumb="Bank Reconciliation"):
    return f"""<div class="topbar">
        <div class="topbar-breadcrumb">Cashbook <span class="sep">/</span> <span class="current">{crumb}</span></div>
        <div class="topbar-right">
            <span class="topbar-badge">DEMO</span>
            <div class="topbar-avatar">CB</div>
        </div>
    </div>"""


def stepper(active=1, done=None):
    done = done or []
    names = ["Select Statement", "Review & Match", "Import to Opera", "Reconcile", "Complete"]
    parts = []
    for i, name in enumerate(names, 1):
        cls = "step"
        if i == active: cls += " active"
        elif i in done: cls += " done"
        dot = "&#10003;" if i in done else str(i)
        parts.append(f'<div class="{cls}"><div class="step-dot">{dot}</div>{name}</div>')
        if i < 5: parts.append('<span class="step-sep">&#9656;</span>')
    return f'<div class="stepper">{"".join(parts)}</div>'


def wrap(body_content, active_stage=1, done_stages=None, crumb="Bank Reconciliation"):
    done_stages = done_stages or []
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">{shared_styles()}</head><body>
    {sidebar_html()}
    <div class="main">
        {topbar_html(crumb)}
        <div class="content">
            <div class="page-heading"><h1>Bank Statement Reconciliation</h1></div>
            {stepper(active_stage, done_stages)}
            {body_content}
        </div>
    </div>
    </body></html>"""


# ──── STAGE 1 ────
def stage1():
    body = """
    <div class="kpi-row" style="grid-template-columns: 1fr 1fr;">
        <div class="kpi blue"><div class="kpi-label">Selected Bank</div>
            <div class="kpi-value" style="font-size:14px;color:#1e40af;">BC010 — Barclays Current Account</div>
            <div class="kpi-change" style="color:#64748b;">Sort: 20-17-19 &nbsp; Account: 30621080</div></div>
        <div class="kpi green"><div class="kpi-label">Opera Reconciled Balance</div>
            <div class="kpi-value" style="color:#059669;">£24,831.56</div>
            <div class="kpi-change" style="color:#64748b;">As at 28 Feb 2026</div></div>
    </div>
    <div class="banner blue">
        <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
        Scanning email inbox for bank statements matching <strong>BC010</strong>...
    </div>
    <div class="card">
        <div class="card-head"><h3>Available Statements</h3><button class="btn btn-outline">Refresh</button></div>
        <div class="card-body"><table>
            <thead><tr><th></th><th>Date</th><th>Subject</th><th>Opening</th><th>Closing</th><th>Lines</th><th>Status</th></tr></thead>
            <tbody>
                <tr style="background:#eff6ff;"><td><div class="ck on"></div></td><td>27 Feb 2026</td><td style="font-weight:500;">Barclays Business — Statement 27.02.26.pdf</td><td class="neutral">£24,831.56</td><td class="neutral">£31,204.18</td><td>23</td><td><span class="badge badge-green">&#10003; Valid</span></td></tr>
                <tr><td><div class="ck"></div></td><td>13 Feb 2026</td><td>Barclays Business — Statement 13.02.26.pdf</td><td class="neutral">£18,450.22</td><td class="neutral">£24,831.56</td><td>31</td><td><span class="badge badge-gray">Imported</span></td></tr>
                <tr><td><div class="ck"></div></td><td>30 Jan 2026</td><td>Barclays Business — Statement 30.01.26.pdf</td><td class="neutral">£22,103.78</td><td class="neutral">£18,450.22</td><td>28</td><td><span class="badge badge-gray">Imported</span></td></tr>
            </tbody>
        </table></div>
    </div>
    <div class="footer-bar"><button class="btn btn-outline">Upload PDF Instead</button><button class="btn btn-primary">Process Selected Statement &#8594;</button></div>
    """
    return wrap(body, 1)


# ──── STAGE 2 ────
def stage2():
    matched = [
        ("27/02","BGC SMITH & JONES LTD","+3,450.00","S024 — Smith & Jones Ltd","Sales Receipt","98%"),
        ("27/02","BGC HENDERSON PLUMBING","+1,200.00","H012 — Henderson Plumbing","Sales Receipt","95%"),
        ("26/02","DD BRITISH GAS","-342.18","B031 — British Gas","Purchase Payment","92%"),
        ("26/02","BGC RIVERSIDE ESTATES","+6,780.00","R015 — Riverside Estates","Sales Receipt","97%"),
        ("25/02","FPI FLANNERY CONSTRUCT","+2,712.00","P051 — P Flannery Construction","Sales Receipt","94%"),
        ("25/02","DD VODAFONE LTD","-89.40","V008 — Vodafone Ltd","Purchase Payment","91%"),
        ("24/02","BGC OAKWOOD SERVICES","+960.00","O012 — Oakwood Services","Sales Receipt","96%"),
        ("24/02","DD SAGE UK","-156.00","N/A — 7100 Software Licences","Nominal Payment","88%"),
    ]
    suggested = [
        ("25/02","CARD AMAZON MKTPLACE","-47.52","Suggested: 7200 Office Supplies","Nominal Payment","72%"),
        ("24/02","BGC LYSANDER SHIPPING","+1,717.38","Suggested: L033 — Lysander Shipping","Sales Receipt","68%"),
    ]

    def mrows(data, style=""):
        rows = ""
        for d, desc, amt, match, tp, sc in data:
            c = "pos" if amt.startswith("+") else "neg"
            bc = "badge-green" if int(sc.rstrip('%')) >= 80 else "badge-amber"
            tc = "badge-blue" if "Receipt" in tp else ("badge-amber" if "Nominal" in tp else "badge-red")
            rows += f'<tr {style}><td><div class="ck on"></div></td><td>{d}</td><td style="font-weight:500;">{desc}</td><td class="{c}">{amt}</td><td>{match}</td><td><span class="badge {tc}">{tp}</span></td><td><span class="badge {bc}">{sc}</span></td></tr>'
        return rows

    def srows():
        rows = ""
        for d, desc, amt, match, tp, sc in suggested:
            c = "pos" if amt.startswith("+") else "neg"
            rows += f'<tr style="background:#fffbeb;"><td><div class="ck"></div></td><td>{d}</td><td style="font-weight:500;">{desc}</td><td class="{c}">{amt}</td><td style="color:#92400e;">{match}</td><td><span class="badge badge-amber">{tp}</span></td><td><span class="badge badge-amber">{sc}</span></td></tr>'
        return rows

    body = f"""
    <div class="kpi-row" style="grid-template-columns: repeat(4,1fr);">
        <div class="kpi green"><div class="kpi-label">Auto-Matched</div><div class="kpi-value" style="color:#059669;">8</div><div class="kpi-change" style="color:#64748b;">Ready to import</div></div>
        <div class="kpi amber"><div class="kpi-label">Suggested</div><div class="kpi-value" style="color:#d97706;">2</div><div class="kpi-change" style="color:#64748b;">Review required</div></div>
        <div class="kpi red"><div class="kpi-label">Unmatched</div><div class="kpi-value" style="color:#dc2626;">2</div><div class="kpi-change" style="color:#64748b;">Manual assignment</div></div>
        <div class="kpi blue"><div class="kpi-label">Already in Opera</div><div class="kpi-value" style="color:#2563eb;">11</div><div class="kpi-change" style="color:#64748b;">Will auto-reconcile</div></div>
    </div>
    <div class="card">
        <div class="card-head"><h3>Auto-Matched Transactions <span class="badge badge-green" style="margin-left:6px;">8</span></h3>
            <div style="display:flex;gap:6px;"><button class="btn btn-outline">Deselect All</button><button class="btn btn-outline">Select All</button></div></div>
        <div class="card-body"><table>
            <thead><tr><th></th><th>Date</th><th>Description</th><th>Amount</th><th>Matched To</th><th>Type</th><th>Score</th></tr></thead>
            <tbody>{mrows(matched)}</tbody>
        </table></div>
    </div>
    <div class="card">
        <div class="card-head"><h3>Suggested Matches <span class="badge badge-amber" style="margin-left:6px;">2</span></h3></div>
        <div class="card-body"><table>
            <thead><tr><th></th><th>Date</th><th>Description</th><th>Amount</th><th>Suggested Match</th><th>Type</th><th>Score</th></tr></thead>
            <tbody>{srows()}</tbody>
        </table></div>
    </div>
    <div class="card">
        <div class="card-head"><h3>Unmatched <span class="badge badge-red" style="margin-left:6px;">2</span></h3></div>
        <div class="card-body"><table>
            <thead><tr><th></th><th>Date</th><th>Description</th><th>Amount</th><th>Assign To</th><th></th><th></th></tr></thead>
            <tbody>
                <tr style="background:#fef2f2;"><td><div class="ck"></div></td><td>26/02</td><td style="font-weight:500;">TFR TO SAVINGS</td><td class="neg">-5,000.00</td><td><select style="padding:3px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:10px;color:#6b7280;"><option>— Select account —</option></select></td><td></td><td></td></tr>
                <tr style="background:#fef2f2;"><td><div class="ck"></div></td><td>25/02</td><td style="font-weight:500;">CARD COSTA COFFEE</td><td class="neg">-8.60</td><td><select style="padding:3px 6px;border:1px solid #d1d5db;border-radius:4px;font-size:10px;color:#6b7280;"><option>— Select account —</option></select></td><td></td><td></td></tr>
            </tbody>
        </table></div>
    </div>
    <div class="footer-bar"><button class="btn btn-outline">&#8592; Back</button><button class="btn btn-primary">Import 10 Transactions to Opera &#8594;</button></div>
    """
    return wrap(body, 2, [1])


# ──── STAGE 3 ────
def stage3():
    imported = [
        ("27/02","BGC SMITH & JONES LTD","+3,450.00","S024","Sales Receipt"),
        ("27/02","BGC HENDERSON PLUMBING","+1,200.00","H012","Sales Receipt"),
        ("26/02","DD BRITISH GAS","-342.18","B031","Purchase Payment"),
        ("26/02","BGC RIVERSIDE ESTATES","+6,780.00","R015","Sales Receipt"),
        ("25/02","FPI FLANNERY CONSTRUCT","+2,712.00","P051","Sales Receipt"),
        ("25/02","DD VODAFONE LTD","-89.40","V008","Purchase Payment"),
        ("24/02","BGC OAKWOOD SERVICES","+960.00","O012","Sales Receipt"),
        ("24/02","DD SAGE UK","-156.00","7100","Nominal Payment"),
        ("25/02","CARD AMAZON MKTPLACE","-47.52","7200","Nominal Payment"),
        ("24/02","BGC LYSANDER SHIPPING","+1,717.38","L033","Sales Receipt"),
    ]
    rows = ""
    for d, desc, amt, code, tp in imported:
        c = "pos" if amt.startswith("+") else "neg"
        tc = "badge-blue" if "Receipt" in tp else ("badge-amber" if "Nominal" in tp else "badge-red")
        rows += f'<tr><td>{d}</td><td style="font-weight:500;">{desc}</td><td class="{c}">{amt}</td><td>{code}</td><td><span class="badge {tc}">{tp}</span></td><td><span class="badge badge-green">&#10003; Posted</span></td></tr>'

    body = f"""
    <div class="banner green">
        <svg width="14" height="14" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
        <strong>Import complete.</strong>&nbsp; 10 of 10 transactions posted successfully. Statement lines auto-assigned.
    </div>
    <div class="kpi-row" style="grid-template-columns: repeat(4,1fr);">
        <div class="kpi green"><div class="kpi-label">Posted</div><div class="kpi-value" style="color:#059669;">10 / 10</div></div>
        <div class="kpi green"><div class="kpi-label">Receipts</div><div class="kpi-value" style="color:#059669;">+£17,819.38</div></div>
        <div class="kpi red"><div class="kpi-label">Payments</div><div class="kpi-value" style="color:#dc2626;">-£635.10</div></div>
        <div class="kpi green"><div class="kpi-label">Auto-Allocated</div><div class="kpi-value" style="color:#059669;">7</div><div class="kpi-change" style="color:#64748b;">Matched to invoices</div></div>
    </div>
    <div class="card">
        <div class="card-head"><h3>Import Results</h3></div>
        <div class="card-body"><table>
            <thead><tr><th>Date</th><th>Description</th><th>Amount</th><th>Account</th><th>Type</th><th>Status</th></tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
    </div>
    <div class="footer-bar"><button class="btn btn-primary">Proceed to Reconcile &#8594;</button></div>
    """
    return wrap(body, 3, [1, 2])


# ──── STAGE 4 ────
def stage4():
    lines = [
        (1,"24/02","DD SAGE UK","-156.00","24,675.56",True),
        (2,"24/02","BGC OAKWOOD SERVICES","+960.00","25,635.56",True),
        (3,"24/02","BGC LYSANDER SHIPPING","+1,717.38","27,352.94",True),
        (4,"25/02","FPI FLANNERY CONSTRUCT","+2,712.00","30,064.94",True),
        (5,"25/02","DD VODAFONE LTD","-89.40","29,975.54",True),
        (6,"25/02","CARD AMAZON MKTPLACE","-47.52","29,928.02",True),
        (7,"25/02","CARD COSTA COFFEE","-8.60","29,919.42",False),
        (8,"25/02","CHQ 001247","-320.00","29,599.42",True),
        (9,"26/02","DD BRITISH GAS","-342.18","29,257.24",True),
        (10,"26/02","BGC RIVERSIDE ESTATES","+6,780.00","36,037.24",True),
        (11,"26/02","TFR TO SAVINGS","-5,000.00","31,037.24",False),
        (12,"27/02","BGC SMITH & JONES LTD","+3,450.00","34,487.24",True),
        (13,"27/02","BGC HENDERSON PLUMBING","+1,200.00","35,687.24",True),
        (14,"27/02","STO OFFICE RENT","-945.00","34,742.24",True),
        (15,"27/02","DD HMRC VAT","-3,538.06","31,204.18",True),
    ]
    rows = ""
    for ln, d, desc, amt, bal, ok in lines:
        cls = ' class="matched-row"' if ok else ''
        c = "pos" if amt.startswith("+") else "neg"
        ck = '<div class="ck on"></div>' if ok else '<div class="ck"></div>'
        tick = '<span class="badge badge-green" style="font-size:9px;">&#10003;</span>' if ok else ''
        rows += f'<tr{cls}><td>{ck}</td><td><span class="ln">{ln}</span></td><td>{d}</td><td style="font-weight:500;">{desc}</td><td class="{c}">{amt}</td><td class="neutral">£{bal}</td><td>{tick}</td></tr>'

    body = f"""
    <div class="kpi-row" style="grid-template-columns: repeat(4,1fr);">
        <div class="kpi blue"><div class="kpi-label">Opening Balance</div><div class="kpi-value" style="color:#1e40af;">£24,831.56</div></div>
        <div class="kpi blue"><div class="kpi-label">Closing Balance</div><div class="kpi-value" style="color:#1e40af;">£31,204.18</div></div>
        <div class="kpi green"><div class="kpi-label">Matched</div><div class="kpi-value" style="color:#059669;">13 / 15</div></div>
        <div class="kpi amber"><div class="kpi-label">Difference</div><div class="kpi-value" style="color:#d97706;">-£5,008.60</div><div class="kpi-change" style="color:#64748b;">2 items unreconciled</div></div>
    </div>
    <div class="card">
        <div class="card-head"><h3>Statement Lines</h3><div style="display:flex;align-items:center;gap:8px;"><span style="font-size:10px;color:#64748b;">13 of 15 reconciled</span><button class="btn btn-outline">Match All</button></div></div>
        <div class="card-body"><table>
            <thead><tr><th></th><th>Line</th><th>Date</th><th>Description</th><th>Amount</th><th>Balance</th><th></th></tr></thead>
            <tbody>{rows}</tbody>
        </table></div>
    </div>
    """
    return wrap(body, 4, [1, 2, 3])


# ──── STAGE 5 ────
def stage5():
    body = """
    <div style="text-align:center;padding:32px 0 20px;">
        <div class="success-icon">
            <svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
        </div>
        <h2 style="font-size:20px;font-weight:700;color:#059669;margin-bottom:6px;">Reconciliation Complete</h2>
        <p style="color:#64748b;font-size:12px;">All 15 statement lines matched. Difference is zero.</p>
    </div>
    <div class="kpi-row" style="grid-template-columns: repeat(3,1fr); max-width:700px; margin:0 auto 16px;">
        <div class="kpi green"><div class="kpi-label">Opening Balance</div><div class="kpi-value" style="color:#059669;">£24,831.56</div></div>
        <div class="kpi green"><div class="kpi-label">Closing Balance</div><div class="kpi-value" style="color:#059669;">£31,204.18</div></div>
        <div class="kpi green"><div class="kpi-label">Difference</div><div class="kpi-value" style="color:#059669;">£0.00</div></div>
    </div>
    <div style="max-width:700px;margin:0 auto;">
        <div class="card"><div class="card-body" style="padding:14px;">
            <table style="font-size:11px;">
                <tbody>
                    <tr><td style="color:#64748b;padding:5px 12px;width:200px;">Statement</td><td style="font-weight:500;padding:5px 12px;">Barclays Business — 27.02.26</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">Bank Account</td><td style="font-weight:500;padding:5px 12px;">BC010 — Barclays Current Account</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">Period</td><td style="font-weight:500;padding:5px 12px;">February 2026 (Period 11)</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">Transactions Imported</td><td style="font-weight:500;padding:5px 12px;">10 new entries posted</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">Auto-Allocated</td><td style="font-weight:500;padding:5px 12px;">7 receipts matched to invoices</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">Statement Lines</td><td style="font-weight:500;padding:5px 12px;">15 / 15 reconciled</td></tr>
                    <tr><td style="color:#64748b;padding:5px 12px;">New Reconciled Balance</td><td style="font-weight:600;color:#059669;padding:5px 12px;">£31,204.18</td></tr>
                </tbody>
            </table>
        </div></div>
    </div>
    <div style="text-align:center;margin-top:16px;"><button class="btn btn-green" style="padding:8px 20px;">&#10003; Done — Return to Cashbook</button></div>
    """
    return wrap(body, 5, [1, 2, 3, 4])


def capture():
    SCREENSHOTS_DIR.mkdir(exist_ok=True)
    stages = [
        ("bank-reconcile-1-select.png", stage1(), 1440, 720),
        ("bank-reconcile-2-review.png", stage2(), 1440, 960),
        ("bank-reconcile-3-import.png", stage3(), 1440, 820),
        ("bank-reconcile-4-reconcile.png", stage4(), 1440, 880),
        ("bank-reconcile-5-complete.png", stage5(), 1440, 750),
    ]
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for fn, html, w, h in stages:
            page = browser.new_page(viewport={"width": w, "height": h})
            page.set_content(html, wait_until="networkidle")
            page.wait_for_timeout(500)
            path = SCREENSHOTS_DIR / fn
            page.screenshot(path=str(path), full_page=True)
            page.close()
            print(f"  {fn}")
        browser.close()
    print(f"\nSaved to {SCREENSHOTS_DIR}/")


if __name__ == "__main__":
    print("Generating screenshots (Finance Hub style)...")
    capture()
    print("Done!")
