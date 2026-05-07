"""Local SMTP adapter — direct smtplib using EMAIL_SMTP_* env vars."""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalSMTPAdapter:
    """Send via smtplib using env-var configuration.

    Reads at construction time:
      EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT, EMAIL_SMTP_USERNAME,
      EMAIL_SMTP_PASSWORD, EMAIL_FROM_ADDRESS

    Falls back to per-company settings via apps.core.state if env
    vars unset — preserves the in-process behaviour today.
    """

    def __init__(
        self,
        *,
        server: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        from_address: Optional[str] = None,
    ):
        from apps.core.env_config import env_int, env_str
        self._server = server or env_str('EMAIL_SMTP_SERVER', '')
        self._port = port if port is not None else env_int('EMAIL_SMTP_PORT', 587)
        self._username = username or env_str('EMAIL_SMTP_USERNAME', '')
        self._password = password or env_str('EMAIL_SMTP_PASSWORD', '')
        self._from_address = from_address or env_str('EMAIL_FROM_ADDRESS', '')

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
        if not (body_html or body_plain):
            return {
                'success': False,
                'message_id': None,
                'error': 'Must provide body_html or body_plain',
            }
        if not self._server:
            return {
                'success': False,
                'message_id': None,
                'error': 'EMAIL_SMTP_SERVER is not configured',
            }

        msg = EmailMessage()
        msg['From'] = from_address or self._from_address or self._username
        msg['To'] = to_address
        msg['Subject'] = subject
        msg['Message-ID'] = make_msgid()
        if cc:
            msg['Cc'] = ', '.join(cc)
        if reply_to:
            msg['Reply-To'] = reply_to

        if body_plain and body_html:
            msg.set_content(body_plain)
            msg.add_alternative(body_html, subtype='html')
        elif body_html:
            msg.set_content(body_html, subtype='html')
        else:
            msg.set_content(body_plain)

        for path in attachments or []:
            try:
                p = Path(path)
                if not p.exists():
                    logger.warning(f"SMTP attachment not found: {path}")
                    continue
                content = p.read_bytes()
                msg.add_attachment(
                    content,
                    maintype='application',
                    subtype='octet-stream',
                    filename=p.name,
                )
            except Exception as e:
                logger.warning(f"Could not attach {path}: {e}")

        try:
            with smtplib.SMTP(self._server, self._port, timeout=30) as s:
                s.ehlo()
                if self._port == 587:
                    s.starttls()
                    s.ehlo()
                if self._username and self._password:
                    s.login(self._username, self._password)
                s.send_message(msg)
            return {
                'success': True,
                'message_id': msg['Message-ID'],
                'error': None,
            }
        except Exception as e:
            logger.error(f"SMTP send failed: {e}")
            return {
                'success': False,
                'message_id': None,
                'error': str(e),
            }
