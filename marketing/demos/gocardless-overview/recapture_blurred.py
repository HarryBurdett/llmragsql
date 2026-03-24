#!/usr/bin/env python3
"""
Recapture GoCardless Mandates and Subscriptions screenshots
with customer names and company name blurred out.
Then regenerate the full overview HTML.
"""
import asyncio
import json
import subprocess
import sys
import os
import base64
from pathlib import Path

OVERVIEW_DIR = Path(__file__).parent
FRONTEND_URL = "http://localhost:5173"
API_URL = "http://localhost:8000"

# CSS to blur customer names, opera names, and account codes in the table
BLUR_CSS = """
/* Blur the company name in header */
.flex.items-center.gap-2 img + span,
[class*="OperaVersionBadge"],
header .text-sm.font-medium,
nav .text-sm {
    filter: blur(5px) !important;
    user-select: none !important;
}

/* Blur customer names in table cells - first column typically */
table tbody td:first-child,
table tbody td:nth-child(2) {
    filter: blur(5px) !important;
    user-select: none !important;
}

/* Blur the "Pending Mandates Refresh" section customer names */
.bg-amber-50 td,
.bg-amber-50 .font-medium {
    filter: blur(5px) !important;
}

/* Blur GoCardless name column and Opera name column */
table thead th { filter: none !important; }
"""

# More targeted approach: inject JS to blur specific elements
BLUR_JS_MANDATES = """
() => {
    // Blur company name in header area
    document.querySelectorAll('header, nav').forEach(el => {
        const texts = el.querySelectorAll('span, div');
        texts.forEach(t => {
            if (t.textContent && (t.textContent.includes('Crakd') || t.textContent.includes('CRAKD') || t.textContent.includes('crakd'))) {
                t.style.filter = 'blur(6px)';
                t.style.userSelect = 'none';
            }
        });
    });

    // Find all table rows and blur customer/company name cells
    document.querySelectorAll('table tbody tr').forEach(row => {
        const cells = row.querySelectorAll('td');
        // Typically: Account, Opera Name, GoCardless Name, Mandate ID, Status, Actions
        // Blur first 3 cells (account code, opera name, gocardless name)
        for (let i = 0; i < Math.min(3, cells.length); i++) {
            cells[i].style.filter = 'blur(6px)';
            cells[i].style.userSelect = 'none';
        }
    });

    // Also blur the pending mandates section
    document.querySelectorAll('.bg-amber-50, .bg-yellow-50, [class*="amber"]').forEach(section => {
        section.querySelectorAll('td, .font-medium, .text-sm').forEach(el => {
            if (el.tagName === 'TD' || el.textContent.length > 2) {
                el.style.filter = 'blur(6px)';
                el.style.userSelect = 'none';
            }
        });
    });
}
"""

BLUR_JS_SUBSCRIPTIONS = """
() => {
    // Blur company name in header area
    document.querySelectorAll('header, nav').forEach(el => {
        const texts = el.querySelectorAll('span, div');
        texts.forEach(t => {
            if (t.textContent && (t.textContent.includes('Crakd') || t.textContent.includes('CRAKD') || t.textContent.includes('crakd'))) {
                t.style.filter = 'blur(6px)';
                t.style.userSelect = 'none';
            }
        });
    });

    // Find all table rows and blur customer name column
    document.querySelectorAll('table tbody tr').forEach(row => {
        const cells = row.querySelectorAll('td');
        // Subscriptions: Customer, Linked Subscription, GC Amount, Opera Total, Frequency, Status, Actions
        // Blur first cell (customer name) and possibly second
        if (cells.length > 0) {
            cells[0].style.filter = 'blur(6px)';
            cells[0].style.userSelect = 'none';
        }
    });
}
"""


async def get_auth_token():
    """Get auth token from the API."""
    import urllib.request
    req = urllib.request.Request(
        f"{API_URL}/api/auth/login",
        data=json.dumps({"username": "admin", "password": "Harry"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
        return data.get("token") or data.get("access_token")


async def capture_blurred_screenshots():
    """Capture mandates and subscriptions with blurred names."""
    from playwright.async_api import async_playwright

    token = await get_auth_token()
    if not token:
        print("ERROR: Could not get auth token")
        sys.exit(1)
    print(f"Got auth token: {token[:20]}...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            device_scale_factor=2
        )
        page = await context.new_page()

        # Set auth token
        await page.goto(FRONTEND_URL)
        await page.evaluate(f"localStorage.setItem('auth_token', '{token}')")
        await page.evaluate(f"localStorage.setItem('token', '{token}')")

        # --- Mandates screenshot ---
        print("Capturing Mandates page (blurred)...")
        await page.goto(f"{FRONTEND_URL}/cashbook/gocardless-requests")
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(4)

        # Debug: save what we see
        await page.screenshot(path=str(OVERVIEW_DIR / "_debug_requests.png"))
        print("  Saved debug screenshot")

        # Click the Mandates tab — use text locator
        mandates_btn = page.locator("button", has_text="Mandates").first
        await mandates_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(4)

        # Apply blur
        await page.evaluate(BLUR_JS_MANDATES)
        await asyncio.sleep(0.5)

        await page.screenshot(
            path=str(OVERVIEW_DIR / "07_requests_mandates.png"),
            full_page=True
        )
        print("  Saved 07_requests_mandates.png (blurred)")

        # --- Subscriptions screenshot ---
        print("Capturing Subscriptions page (blurred)...")
        subs_btn = page.locator("button", has_text="Subscriptions").first
        await subs_btn.click()
        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(4)

        # Apply blur
        await page.evaluate(BLUR_JS_SUBSCRIPTIONS)
        await asyncio.sleep(0.5)

        await page.screenshot(
            path=str(OVERVIEW_DIR / "08_requests_subscriptions.png"),
            full_page=True
        )
        print("  Saved 08_requests_subscriptions.png (blurred)")

        await browser.close()

    print("Screenshots recaptured with blurred names.")


def regenerate_overview_html():
    """Regenerate the overview HTML with updated screenshots."""
    print("Regenerating overview HTML...")

    # Read all screenshot files and encode as base64
    images = {}
    for png_file in sorted(OVERVIEW_DIR.glob("*.png")):
        if png_file.name.startswith("_"):
            continue
        key = png_file.stem
        with open(png_file, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        images[key] = f"data:image/png;base64,{b64}"
        print(f"  Encoded {png_file.name} ({len(b64) // 1024}KB)")

    # Build the HTML (same structure as before)
    html = build_overview_html(images)

    output_path = OVERVIEW_DIR / "GoCardless-Workflow-Overview.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Generated {output_path} ({output_path.stat().st_size // 1024}KB)")


def build_overview_html(images: dict) -> str:
    """Build the self-contained HTML overview document."""

    def img_tag(key, caption=""):
        src = images.get(key, "")
        if not src:
            return f'<p class="text-red-500">Missing image: {key}</p>'
        cap = f'<p class="screen-caption">{caption}</p>' if caption else ""
        return f'''<div class="screen">
            <img src="{src}" alt="{key}" style="width:100%; border-radius:8px; border:1px solid #e2e8f0;" />
            {cap}
        </div>'''

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GoCardless Integration - Complete Workflow Overview</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #1a202c; background: #f7fafc; line-height: 1.6; }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 40px 24px; }}
  h1 {{ font-size: 2.2rem; font-weight: 700; margin-bottom: 8px; color: #1a365d; }}
  h2 {{ font-size: 1.6rem; font-weight: 600; margin: 48px 0 16px; color: #2d3748; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }}
  h3 {{ font-size: 1.2rem; font-weight: 600; margin: 32px 0 12px; color: #4a5568; }}
  p {{ margin: 12px 0; color: #4a5568; }}
  .subtitle {{ font-size: 1.1rem; color: #718096; margin-bottom: 32px; }}
  .screen {{ background: white; border-radius: 12px; padding: 24px; margin: 24px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .screen-title {{ font-size: 1rem; font-weight: 600; color: #2b6cb0; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .screen img {{ display: block; margin: 0 auto; }}
  .screen-caption {{ font-size: 0.9rem; color: #718096; margin-top: 12px; font-style: italic; text-align: center; }}
  .flow-step {{ display: flex; align-items: flex-start; gap: 16px; margin: 20px 0; }}
  .step-num {{ background: #3182ce; color: white; width: 32px; height: 32px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 0.9rem; flex-shrink: 0; }}
  .step-content {{ flex: 1; }}
  .step-content h4 {{ font-size: 1rem; font-weight: 600; color: #2d3748; margin-bottom: 4px; }}
  .step-content p {{ margin: 4px 0; font-size: 0.95rem; }}
  .highlight {{ background: #ebf8ff; border-left: 4px solid #3182ce; padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 16px 0; }}
  .highlight p {{ color: #2b6cb0; margin: 0; }}
  .feature-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin: 16px 0; }}
  .feature-card {{ background: white; border-radius: 10px; padding: 20px; border: 1px solid #e2e8f0; }}
  .feature-card h4 {{ font-size: 1rem; font-weight: 600; color: #2d3748; margin-bottom: 8px; }}
  .feature-card p {{ font-size: 0.9rem; color: #718096; margin: 0; }}
  table.posting {{ width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 0.9rem; }}
  table.posting th {{ background: #edf2f7; padding: 10px 12px; text-align: left; font-weight: 600; color: #2d3748; border-bottom: 2px solid #cbd5e0; }}
  table.posting td {{ padding: 8px 12px; border-bottom: 1px solid #e2e8f0; color: #4a5568; }}
  table.posting tr:hover {{ background: #f7fafc; }}
  .toc {{ background: white; border-radius: 12px; padding: 24px; margin: 24px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .toc h3 {{ margin-top: 0; }}
  .toc ul {{ list-style: none; padding: 0; }}
  .toc li {{ padding: 6px 0; }}
  .toc a {{ color: #3182ce; text-decoration: none; font-weight: 500; }}
  .toc a:hover {{ text-decoration: underline; }}
  @media print {{ body {{ background: white; }} .container {{ padding: 20px; }} }}
</style>
</head>
<body>
<div class="container">

<h1>GoCardless Integration</h1>
<p class="subtitle">Complete Workflow Overview &mdash; Opera Accounting System Integration</p>

<div class="toc">
  <h3>Contents</h3>
  <ul>
    <li><a href="#overview">Overview</a></li>
    <li><a href="#module1">Module 1: Payout Import</a></li>
    <li><a href="#module2">Module 2: Payment Requests</a></li>
    <li><a href="#module3">Module 3: Mandate Management</a></li>
    <li><a href="#module4">Module 4: Subscription Management</a></li>
    <li><a href="#settings">Settings &amp; Configuration</a></li>
    <li><a href="#history">Import History</a></li>
    <li><a href="#posting">Opera Posting Details</a></li>
  </ul>
</div>

<h2 id="overview">Overview</h2>
<p>The GoCardless integration provides a complete workflow for managing Direct Debit payments within the Opera accounting system. It connects to the GoCardless API to import payouts, create payment requests, manage mandates, and handle subscriptions &mdash; all with automatic posting to Opera's Sales Ledger, Cashbook, and Nominal Ledger.</p>

<div class="feature-grid">
  <div class="feature-card">
    <h4>Automated Payout Import</h4>
    <p>Monitors email for GoCardless payout notifications. Automatically extracts payment details, matches to customers, and posts as sales receipts to Opera.</p>
  </div>
  <div class="feature-card">
    <h4>Payment Requests</h4>
    <p>Create one-off or recurring payment requests linked to Opera invoices. Track pending, completed, and failed payments in real time.</p>
  </div>
  <div class="feature-card">
    <h4>Mandate Management</h4>
    <p>View and manage Direct Debit mandates. Link GoCardless mandates to Opera customer accounts for automatic matching.</p>
  </div>
  <div class="feature-card">
    <h4>Subscription Management</h4>
    <p>Set up recurring payment schedules linked to GoCardless mandates. Manage frequency, amounts, and status from a single interface.</p>
  </div>
</div>

{img_tag("01_gocardless_import", "GoCardless Import — main entry point showing payout import and batch processing")}

<h2 id="module1">Module 1: Payout Import</h2>
<p>The payout import workflow automates the process of importing GoCardless Direct Debit payments into Opera. It monitors the email inbox for payout notification emails, extracts payment details, and matches them to Opera customer accounts.</p>

<h3>Workflow Steps</h3>
<div class="flow-step">
  <div class="step-num">1</div>
  <div class="step-content">
    <h4>Select Bank &amp; Fetch Payouts</h4>
    <p>Choose the Opera bank account that receives GoCardless funds. The system fetches available payouts from GoCardless API, showing amount, date, and payment count.</p>
  </div>
</div>
<div class="flow-step">
  <div class="step-num">2</div>
  <div class="step-content">
    <h4>Review Payments</h4>
    <p>Each payout contains individual customer payments. The system auto-matches payments to Opera customers using mandate references, invoice numbers, and customer names.</p>
  </div>
</div>
<div class="flow-step">
  <div class="step-num">3</div>
  <div class="step-content">
    <h4>Import to Opera</h4>
    <p>Posts matched payments as sales receipts in Opera. Creates cashbook entries, updates customer balances, posts to nominal ledger, and optionally auto-allocates to invoices.</p>
  </div>
</div>
<div class="flow-step">
  <div class="step-num">4</div>
  <div class="step-content">
    <h4>GoCardless Fees</h4>
    <p>GoCardless transaction fees are automatically split out and posted as a separate nominal entry with VAT tracking (input VAT, reclaimable).</p>
  </div>
</div>

{img_tag("04_import_with_payouts", "Fetching payouts from GoCardless API — shows available batches with amounts and dates")}

<h2 id="module2">Module 2: Payment Requests</h2>
<p>Create and manage payment requests against Opera invoices. Outstanding invoices are shown with their GoCardless mandate status, allowing one-click payment collection.</p>

{img_tag("06_requests_invoices", "Outstanding Invoices tab — showing invoices eligible for GoCardless collection with mandate status")}

<h3>Request Lifecycle</h3>
<div class="flow-step">
  <div class="step-num">1</div>
  <div class="step-content">
    <h4>Create Request</h4>
    <p>Select one or more outstanding invoices and create a payment request. The system validates the customer has an active GoCardless mandate.</p>
  </div>
</div>
<div class="flow-step">
  <div class="step-num">2</div>
  <div class="step-content">
    <h4>Pending</h4>
    <p>Payment requests are submitted to GoCardless and enter a pending state. GoCardless processes Direct Debit collections over 3-5 business days.</p>
  </div>
</div>
<div class="flow-step">
  <div class="step-num">3</div>
  <div class="step-content">
    <h4>Completed / Failed</h4>
    <p>Requests complete successfully or fail (insufficient funds, cancelled mandate). Failed payments are flagged for review.</p>
  </div>
</div>

{img_tag("09_requests_pending", "Pending Requests tab — showing in-progress payment collections")}
{img_tag("10_requests_history", "Payment History tab — completed and failed request history")}

<h2 id="module3">Module 3: Mandate Management</h2>
<p>The Mandates tab shows all GoCardless Direct Debit mandates and their linkage to Opera customer accounts. Mandates can be in various states: pending customer approval, active, or cancelled.</p>

<div class="highlight">
  <p><strong>Customer Matching:</strong> The system automatically matches GoCardless mandates to Opera customers by comparing customer names and email addresses. Unmatched mandates are highlighted for manual linking.</p>
</div>

{img_tag("07_requests_mandates", "Mandates tab — showing all Direct Debit mandates with Opera customer linking (names redacted)")}

<h2 id="module4">Module 4: Subscription Management</h2>
<p>Subscriptions allow recurring payment schedules to be set up against GoCardless mandates. Each subscription defines the amount, frequency (weekly, monthly, yearly), and links to the Opera customer account.</p>

{img_tag("08_requests_subscriptions", "Subscriptions tab — recurring payment schedules with amounts and frequencies (names redacted)")}

<h2 id="settings">Settings &amp; Configuration</h2>
<p>The GoCardless settings page manages the API connection, including the access token, webhook secret, and environment selection (sandbox/live).</p>

{img_tag("03_gocardless_settings", "GoCardless Settings — API configuration and connection status")}

<h2 id="history">Import History</h2>
<p>The import history provides a complete audit trail of all GoCardless payout imports, showing dates, amounts, customer matches, and Opera posting status.</p>

{img_tag("11_import_history", "Import History — audit trail of all processed GoCardless payouts")}

<h2 id="posting">Opera Posting Details</h2>
<p>Each GoCardless payout import creates a complete set of Opera accounting entries. The following tables detail exactly what is posted.</p>

<h3>Sales Receipt (per customer payment)</h3>
<table class="posting">
  <tr><th>Opera Table</th><th>Description</th><th>Key Fields</th></tr>
  <tr><td>aentry</td><td>Cashbook header</td><td>ae_cbtype, ae_entry, ae_amount (pence)</td></tr>
  <tr><td>atran</td><td>Cashbook detail</td><td>at_type=4 (sales receipt), at_value (pence, +ve)</td></tr>
  <tr><td>stran</td><td>Sales ledger transaction</td><td>st_trtype='R', st_trvalue (pounds, -ve)</td></tr>
  <tr><td>anoml</td><td>NL transfer file</td><td>ax_source='S', ax_done='Y'</td></tr>
  <tr><td>ntran</td><td>Nominal ledger entries</td><td>Debit bank, Credit debtors control</td></tr>
  <tr><td>nacnt</td><td>Nominal balances</td><td>Period balances updated</td></tr>
  <tr><td>nbank</td><td>Bank balance</td><td>nk_curbal += amount (pence)</td></tr>
  <tr><td>sname</td><td>Customer balance</td><td>sn_currbal -= amount (pounds)</td></tr>
</table>

<h3>GoCardless Fees</h3>
<table class="posting">
  <tr><th>Opera Table</th><th>Description</th><th>Key Fields</th></tr>
  <tr><td>aentry</td><td>Cashbook header</td><td>ae_cbtype, ae_entry, ae_amount (pence, -ve)</td></tr>
  <tr><td>atran</td><td>Cashbook detail</td><td>at_type=1 (nominal payment), at_value (pence, -ve)</td></tr>
  <tr><td>anoml</td><td>NL transfer file</td><td>ax_source='A' (Admin)</td></tr>
  <tr><td>ntran</td><td>Nominal ledger entries</td><td>Debit fees account, Credit bank; Debit VAT input</td></tr>
  <tr><td>nacnt</td><td>Nominal balances</td><td>Period balances updated for fees + VAT accounts</td></tr>
  <tr><td>nbank</td><td>Bank balance</td><td>nk_curbal -= fee amount (pence)</td></tr>
  <tr><td>zvtran</td><td>VAT analysis</td><td>VAT amount for return</td></tr>
  <tr><td>nvat</td><td>VAT return tracking</td><td>nv_vattype='P' (input/purchase VAT)</td></tr>
</table>

<h3>Auto-Allocation</h3>
<table class="posting">
  <tr><th>Opera Table</th><th>Description</th><th>Key Fields</th></tr>
  <tr><td>salloc</td><td>Allocation record</td><td>Links receipt to invoice(s)</td></tr>
  <tr><td>stran (receipt)</td><td>Receipt marked allocated</td><td>st_paid='A', st_payflag=N</td></tr>
  <tr><td>stran (invoice)</td><td>Invoice marked paid</td><td>st_paid='P', st_trbal=0, st_payflag=N</td></tr>
</table>

<div class="highlight">
  <p><strong>Double-Entry Integrity:</strong> Every posting maintains double-entry balance. Debits always equal credits in the nominal ledger. Control account reconciliation can verify this via the Balance Check utility.</p>
</div>

<hr style="margin: 48px 0; border: none; border-top: 2px solid #e2e8f0;" />
<p style="text-align: center; color: #a0aec0; font-size: 0.85rem;">
  Generated for Opera Accounting System Integration &bull; GoCardless Direct Debit Module
</p>

</div>
</body>
</html>'''


if __name__ == "__main__":
    asyncio.run(capture_blurred_screenshots())
    regenerate_overview_html()
    print("\nDone! Overview regenerated with blurred customer/company names.")
