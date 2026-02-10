#!/usr/bin/env python3
"""
Daily Knowledge Email Script

Sends daily updates of application knowledge documentation to specified recipients.
Intended to be run via cron job.

Cron entry (9 AM daily):
0 9 * * * cd /Users/maccb/llmragsql && /Users/maccb/llmragsql/venv/bin/python scripts/daily_knowledge_email.py
"""

import json
import smtplib
import sqlite3
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# Configuration
RECIPIENTS = [
    'costas@systemscloud.co.uk'
]
FROM_EMAIL = 'intsys@wimbledoncloud.net'
SUBJECT_PREFIX = 'Opera Integration - Daily Knowledge Update'

# Knowledge files to send
KNOWLEDGE_FILES = [
    'docs/bank_statement_import.md',
    'docs/gocardless_integration.md',
    'docs/ap_automation.md',
    'docs/cashbook_reconcile.md',
    'docs/balance_check.md'
]

# Email server settings
DB_PATH = 'email_data.db'


def get_smtp_config():
    """Get SMTP configuration from email_data.db"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT config_json FROM email_providers
        WHERE enabled = 1 AND provider_type = 'imap'
        LIMIT 1
    ''')
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise Exception("No enabled email provider found")

    config = json.loads(row['config_json'])
    return {
        'server': config.get('server', '10.10.100.12'),
        'port': 587,
        'username': config.get('username', ''),
        'password': config.get('password', '')
    }


def build_email_body():
    """Build HTML email body with knowledge summary"""
    today = datetime.now().strftime('%d %B %Y')

    return f'''<html>
<head>
<style>
body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
h2 {{ color: #2980b9; margin-top: 30px; }}
ul {{ margin: 10px 0; }}
li {{ margin: 5px 0; }}
.feature {{ background: #f8f9fa; padding: 15px; margin: 15px 0; border-left: 4px solid #3498db; }}
.date {{ color: #666; font-size: 14px; }}
</style>
</head>
<body>
<h1>Opera Integration - Daily Knowledge Update</h1>
<p class="date">Generated: {today}</p>

<p>Please find attached the latest documentation for each Opera integration application:</p>

<div class="feature">
<h2>1. Bank Statement Import</h2>
<p>Automated import of bank statements (CSV, OFX, PDF) into Opera cashbook with intelligent customer/supplier matching.</p>
</div>

<div class="feature">
<h2>2. GoCardless Integration</h2>
<p>Automatic processing of GoCardless payment notifications from email into Opera sales receipts.</p>
</div>

<div class="feature">
<h2>3. AP Automation</h2>
<p>Supplier statement reconciliation with AI-powered extraction and matching against Purchase Ledger.</p>
</div>

<div class="feature">
<h2>4. Cashbook Reconciliation</h2>
<p>Bank statement reconciliation - tick off cashbook entries against bank statement.</p>
</div>

<div class="feature">
<h2>5. Balance Check</h2>
<p>Control account reconciliation - verify sub-ledger totals match nominal ledger.</p>
</div>

<p style="margin-top: 30px; color: #666;">
<em>Full documentation for each application is attached as Markdown files.</em>
</p>

</body>
</html>'''


def send_email(recipient, smtp_config):
    """Send email with knowledge attachments"""
    msg = MIMEMultipart()
    msg['From'] = FROM_EMAIL
    msg['To'] = recipient
    msg['Subject'] = f"{SUBJECT_PREFIX} - {datetime.now().strftime('%d/%m/%Y')}"

    # Add body
    msg.attach(MIMEText(build_email_body(), 'html'))

    # Add attachments
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for filepath in KNOWLEDGE_FILES:
        full_path = os.path.join(base_path, filepath)
        if os.path.exists(full_path):
            filename = os.path.basename(full_path)
            with open(full_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
        else:
            print(f"Warning: File not found: {full_path}")

    # Send
    with smtplib.SMTP(smtp_config['server'], smtp_config['port'], timeout=30) as server:
        server.starttls()
        server.login(smtp_config['username'], smtp_config['password'])
        server.send_message(msg)

    print(f"Email sent to {recipient}")


def main():
    """Main function"""
    print(f"Starting daily knowledge email - {datetime.now()}")

    # Change to project directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    try:
        smtp_config = get_smtp_config()

        for recipient in RECIPIENTS:
            try:
                send_email(recipient, smtp_config)
            except Exception as e:
                print(f"Failed to send to {recipient}: {e}")

        print("Daily knowledge email complete")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == '__main__':
    main()
