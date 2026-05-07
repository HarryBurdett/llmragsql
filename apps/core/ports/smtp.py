"""SMTP port — send outbound email.

Used by suppliers (remittance, contact email) and gocardless
(remittance). The local adapter uses smtplib directly with config
from EMAIL_SMTP_* env vars. The HTTP adapter delegates to a SAM-
provided email service.
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class SMTPPort(Protocol):
    """Send an email via SMTP.

    The port is intentionally minimal. Apps construct the message
    body (HTML / plain) themselves; the port just hands it to the
    transport.
    """

    def send(
        self,
        *,
        to_address: str,
        subject: str,
        body_html: Optional[str] = None,
        body_plain: Optional[str] = None,
        from_address: Optional[str] = None,
        attachments: Optional[list[str]] = None,
        cc: Optional[list[str]] = None,
        reply_to: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send and return {'success': bool, 'message_id': str|None,
        'error': str|None}.

        Either body_html or body_plain (or both) must be supplied.
        attachments is a list of file paths.
        """
        ...
