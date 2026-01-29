"""
Gmail email provider implementation.
Uses Google Gmail API for email access.
"""
from __future__ import annotations

import base64
import logging
import os
import pickle
from typing import List, Dict, Any, Optional
from datetime import datetime
from email.utils import parsedate_to_datetime

try:
    from google.oauth2.credentials import Credentials
    from google.oauth2 import service_account
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    GOOGLE_API_AVAILABLE = True
except ImportError:
    Credentials = None  # type: ignore
    service_account = None  # type: ignore
    InstalledAppFlow = None  # type: ignore
    Request = None  # type: ignore
    build = None  # type: ignore
    GOOGLE_API_AVAILABLE = False

from .base import EmailProvider, EmailMessage, EmailFolder, EmailAttachment, ProviderType

logger = logging.getLogger(__name__)


class GmailProvider(EmailProvider):
    """
    Gmail email provider using Google API.

    Requires Google API packages:
    pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
    """

    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Gmail provider.

        Config keys:
            credentials_file: Path to OAuth2 credentials JSON file
            token_file: Path to store/load OAuth2 token (default: gmail_token.pickle)
            user_email: User email address (for service account delegation)
        """
        super().__init__(config)

        if not GOOGLE_API_AVAILABLE:
            raise ImportError(
                "Gmail provider requires Google API packages. Install with: "
                "pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
            )

        self.credentials_file = config.get('credentials_file', 'credentials.json')
        self.token_file = config.get('token_file', 'gmail_token.pickle')
        self.user_email = config.get('user_email')
        self._service = None
        self._credentials = None

    @property
    def provider_type(self) -> ProviderType:
        return ProviderType.GMAIL

    def _get_credentials(self) -> Any:
        """Get or refresh OAuth2 credentials."""
        creds = None

        # Load existing token
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)

        # Refresh or get new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # Check if credentials file exists
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(
                        f"Credentials file not found: {self.credentials_file}. "
                        "Download OAuth2 credentials from Google Cloud Console."
                    )

                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            # Save credentials
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)

        return creds

    def _get_service(self):
        """Get or create Gmail API service."""
        if self._service is None:
            self._credentials = self._get_credentials()
            self._service = build('gmail', 'v1', credentials=self._credentials)
        return self._service

    def _decode_base64(self, data: str) -> str:
        """Decode base64url encoded data."""
        try:
            # Add padding if needed
            padding = 4 - len(data) % 4
            if padding != 4:
                data += '=' * padding
            return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
        except Exception:
            return ""

    def _get_header(self, headers: List[Dict], name: str) -> str:
        """Get header value by name."""
        for header in headers:
            if header.get('name', '').lower() == name.lower():
                return header.get('value', '')
        return ""

    def _parse_email(self, msg: Dict[str, Any]) -> EmailMessage:
        """Parse Gmail API message to EmailMessage."""
        headers = msg.get('payload', {}).get('headers', [])

        # Parse from address
        from_header = self._get_header(headers, 'From')
        from_address = ""
        from_name = None
        if '<' in from_header and '>' in from_header:
            from_name = from_header.split('<')[0].strip().strip('"')
            from_address = from_header.split('<')[1].split('>')[0]
        else:
            from_address = from_header

        # Parse to addresses
        to_header = self._get_header(headers, 'To')
        to_addresses = []
        if to_header:
            for addr in to_header.split(','):
                addr = addr.strip()
                if '<' in addr:
                    addr = addr.split('<')[1].split('>')[0]
                if addr:
                    to_addresses.append(addr)

        # Parse CC addresses
        cc_header = self._get_header(headers, 'Cc')
        cc_addresses = []
        if cc_header:
            for addr in cc_header.split(','):
                addr = addr.strip()
                if '<' in addr:
                    addr = addr.split('<')[1].split('>')[0]
                if addr:
                    cc_addresses.append(addr)

        # Parse date
        date_header = self._get_header(headers, 'Date')
        received_at = datetime.now()
        if date_header:
            try:
                received_at = parsedate_to_datetime(date_header)
            except Exception:
                pass

        # Parse body
        body_text = ""
        body_html = None
        payload = msg.get('payload', {})

        def extract_body(part: Dict) -> tuple:
            text = ""
            html = None
            if part.get('mimeType') == 'text/plain':
                data = part.get('body', {}).get('data', '')
                if data:
                    text = self._decode_base64(data)
            elif part.get('mimeType') == 'text/html':
                data = part.get('body', {}).get('data', '')
                if data:
                    html = self._decode_base64(data)
            elif 'parts' in part:
                for subpart in part['parts']:
                    sub_text, sub_html = extract_body(subpart)
                    if sub_text:
                        text = sub_text
                    if sub_html:
                        html = sub_html
            return text, html

        body_text, body_html = extract_body(payload)

        # Parse attachments
        attachments = []

        def extract_attachments(part: Dict, depth: int = 0):
            filename = part.get('filename', '')
            if filename:
                attachments.append(EmailAttachment(
                    attachment_id=part.get('body', {}).get('attachmentId', ''),
                    filename=filename,
                    content_type=part.get('mimeType', 'application/octet-stream'),
                    size_bytes=part.get('body', {}).get('size', 0)
                ))
            if 'parts' in part:
                for subpart in part['parts']:
                    extract_attachments(subpart, depth + 1)

        extract_attachments(payload)

        # Check labels for read status
        labels = msg.get('labelIds', [])
        is_read = 'UNREAD' not in labels
        is_flagged = 'STARRED' in labels

        return EmailMessage(
            message_id=msg.get('id', ''),
            thread_id=msg.get('threadId'),
            folder_id='INBOX',  # Gmail uses labels, map to folder
            from_address=from_address,
            from_name=from_name,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            subject=self._get_header(headers, 'Subject'),
            body_preview=msg.get('snippet', '')[:500],
            body_html=body_html,
            body_text=body_text,
            received_at=received_at,
            sent_at=received_at,
            is_read=is_read,
            is_flagged=is_flagged,
            has_attachments=len(attachments) > 0,
            attachments=attachments,
            raw_headers={h['name']: h['value'] for h in headers}
        )

    async def authenticate(self) -> bool:
        """Authenticate with Gmail API."""
        try:
            self._get_service()
            self._authenticated = True
            logger.info("Gmail API authenticated successfully")
            return True
        except Exception as e:
            logger.error(f"Gmail authentication failed: {e}")
            self._authenticated = False
            return False

    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to Gmail API."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()
            profile = service.users().getProfile(userId='me').execute()
            return {
                'success': True,
                'message': f"Connected as {profile.get('emailAddress', 'unknown')}"
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def list_folders(self) -> List[EmailFolder]:
        """Get all Gmail labels (folders)."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()
            results = service.users().labels().list(userId='me').execute()

            folders = []
            for label in results.get('labels', []):
                # Get label details for counts
                try:
                    label_details = service.users().labels().get(
                        userId='me',
                        id=label['id']
                    ).execute()

                    folders.append(EmailFolder(
                        folder_id=label['id'],
                        name=label['name'],
                        unread_count=label_details.get('messagesUnread', 0),
                        total_count=label_details.get('messagesTotal', 0)
                    ))
                except Exception:
                    folders.append(EmailFolder(
                        folder_id=label['id'],
                        name=label['name']
                    ))

            return folders
        except Exception as e:
            logger.error(f"Error listing labels: {e}")
            return []

    async def fetch_emails(
        self,
        folder_id: str,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[EmailMessage]:
        """Fetch emails from a label (folder)."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()

            # Build query
            query_parts = [f'label:{folder_id}'] if folder_id != 'INBOX' else ['in:inbox']
            if since:
                after_str = since.strftime("%Y/%m/%d")
                query_parts.append(f'after:{after_str}')

            query = ' '.join(query_parts)

            # List messages
            results = service.users().messages().list(
                userId='me',
                q=query,
                maxResults=limit
            ).execute()

            emails = []
            for msg_ref in results.get('messages', []):
                try:
                    # Get full message
                    msg = service.users().messages().get(
                        userId='me',
                        id=msg_ref['id'],
                        format='full'
                    ).execute()

                    email = self._parse_email(msg)
                    emails.append(email)
                except Exception as e:
                    logger.warning(f"Error parsing email {msg_ref['id']}: {e}")

            return emails
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []

    async def get_email_content(self, message_id: str) -> Optional[EmailMessage]:
        """Get full email content."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()
            msg = service.users().messages().get(
                userId='me',
                id=message_id,
                format='full'
            ).execute()

            return self._parse_email(msg)
        except Exception as e:
            logger.error(f"Error getting email content: {e}")
            return None

    async def mark_as_read(self, message_id: str) -> bool:
        """Mark email as read by removing UNREAD label."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()
            service.users().messages().modify(
                userId='me',
                id=message_id,
                body={'removeLabelIds': ['UNREAD']}
            ).execute()
            return True
        except Exception as e:
            logger.error(f"Error marking email as read: {e}")
            return False

    async def download_attachment(
        self,
        message_id: str,
        attachment_id: str
    ) -> Optional[bytes]:
        """Download email attachment."""
        try:
            if not self._authenticated:
                await self.authenticate()

            service = self._get_service()
            attachment = service.users().messages().attachments().get(
                userId='me',
                messageId=message_id,
                id=attachment_id
            ).execute()

            data = attachment.get('data', '')
            if data:
                # Add padding if needed
                padding = 4 - len(data) % 4
                if padding != 4:
                    data += '=' * padding
                return base64.urlsafe_b64decode(data)
            return None
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None

    async def disconnect(self) -> None:
        """Disconnect from Gmail API."""
        self._service = None
        self._credentials = None
        self._authenticated = False
