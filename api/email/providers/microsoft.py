"""
Microsoft 365 / Outlook email provider implementation.
Uses Microsoft Graph API for email access.
"""
from __future__ import annotations

import logging
import asyncio
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from datetime import datetime

try:
    import msal
    import httpx
    MSAL_AVAILABLE = True
except ImportError:
    msal = None  # type: ignore
    httpx = None  # type: ignore
    MSAL_AVAILABLE = False

from .base import EmailProvider, EmailMessage, EmailFolder, EmailAttachment, ProviderType

logger = logging.getLogger(__name__)


class MicrosoftProvider(EmailProvider):
    """
    Microsoft 365 / Outlook email provider using Graph API.

    Requires the 'msal' package: pip install msal httpx
    """

    GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
    SCOPES = ["https://graph.microsoft.com/.default"]

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Microsoft provider.

        Config keys:
            tenant_id: Azure AD tenant ID
            client_id: Application (client) ID
            client_secret: Client secret value
            user_email: User email address to access
        """
        super().__init__(config)

        if not MSAL_AVAILABLE:
            raise ImportError("Microsoft provider requires 'msal' and 'httpx' packages. Install with: pip install msal httpx")

        self.tenant_id = config.get('tenant_id')
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.user_email = config.get('user_email')
        self._access_token: Optional[str] = None
        self._msal_app: Optional[Any] = None

    @property
    def provider_type(self) -> ProviderType:
        return ProviderType.MICROSOFT

    def _get_msal_app(self) -> Any:
        """Get or create MSAL application instance."""
        if self._msal_app is None:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            self._msal_app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority,
                client_credential=self.client_secret
            )
        return self._msal_app

    async def _get_access_token(self) -> str:
        """Get access token using client credentials flow."""
        app = self._get_msal_app()

        # Try to get token from cache first
        result = app.acquire_token_silent(self.SCOPES, account=None)

        if not result:
            # Get new token
            result = app.acquire_token_for_client(scopes=self.SCOPES)

        if "access_token" in result:
            self._access_token = result["access_token"]
            return self._access_token
        else:
            error = result.get("error_description", result.get("error", "Unknown error"))
            raise Exception(f"Failed to acquire token: {error}")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make authenticated request to Graph API."""
        if not self._access_token:
            await self._get_access_token()

        url = f"{self.GRAPH_BASE_URL}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                timeout=30.0
            )

            if response.status_code == 401:
                # Token might be expired, refresh and retry
                await self._get_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = await client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=30.0
                )

            response.raise_for_status()
            return response.json() if response.content else {}

    def _parse_email(self, msg: Dict[str, Any]) -> EmailMessage:
        """Parse Graph API email message to EmailMessage."""
        # Parse from address
        from_data = msg.get('from', {}).get('emailAddress', {})
        from_address = from_data.get('address', '')
        from_name = from_data.get('name')

        # Parse to addresses
        to_recipients = msg.get('toRecipients', [])
        to_addresses = [r.get('emailAddress', {}).get('address', '') for r in to_recipients]

        # Parse CC addresses
        cc_recipients = msg.get('ccRecipients', [])
        cc_addresses = [r.get('emailAddress', {}).get('address', '') for r in cc_recipients]

        # Parse dates
        received_str = msg.get('receivedDateTime', '')
        received_at = datetime.fromisoformat(received_str.replace('Z', '+00:00')) if received_str else datetime.now()

        sent_str = msg.get('sentDateTime', '')
        sent_at = datetime.fromisoformat(sent_str.replace('Z', '+00:00')) if sent_str else None

        # Parse body
        body = msg.get('body', {})
        body_content = body.get('content', '')
        body_type = body.get('contentType', 'text')

        if body_type == 'html':
            body_html = body_content
            body_text = msg.get('bodyPreview', '')
        else:
            body_html = None
            body_text = body_content

        # Parse attachments
        attachments = []
        if msg.get('hasAttachments'):
            for att in msg.get('attachments', []):
                attachments.append(EmailAttachment(
                    attachment_id=att.get('id', ''),
                    filename=att.get('name', 'attachment'),
                    content_type=att.get('contentType', 'application/octet-stream'),
                    size_bytes=att.get('size', 0)
                ))

        return EmailMessage(
            message_id=msg.get('id', ''),
            thread_id=msg.get('conversationId'),
            folder_id=msg.get('parentFolderId', ''),
            from_address=from_address,
            from_name=from_name,
            to_addresses=to_addresses,
            cc_addresses=cc_addresses,
            subject=msg.get('subject', ''),
            body_preview=msg.get('bodyPreview', '')[:500],
            body_html=body_html,
            body_text=body_text,
            received_at=received_at,
            sent_at=sent_at,
            is_read=msg.get('isRead', False),
            is_flagged=msg.get('flag', {}).get('flagStatus') == 'flagged',
            has_attachments=msg.get('hasAttachments', False),
            attachments=attachments,
            raw_headers={}
        )

    async def authenticate(self) -> bool:
        """Authenticate with Microsoft Graph API."""
        try:
            await self._get_access_token()
            self._authenticated = True
            logger.info("Microsoft Graph API authenticated successfully")
            return True
        except Exception as e:
            logger.error(f"Microsoft authentication failed: {e}")
            self._authenticated = False
            return False

    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to Microsoft Graph API."""
        try:
            if not self._authenticated:
                await self.authenticate()

            # Try to get user info
            result = await self._make_request("GET", f"/users/{self.user_email}")
            return {
                'success': True,
                'message': f"Connected as {result.get('displayName', self.user_email)}"
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def list_folders(self) -> List[EmailFolder]:
        """Get all mail folders."""
        try:
            if not self._authenticated:
                await self.authenticate()

            result = await self._make_request(
                "GET",
                f"/users/{self.user_email}/mailFolders",
                params={"$top": 100}
            )

            folders = []
            for folder in result.get('value', []):
                folders.append(EmailFolder(
                    folder_id=folder.get('id', ''),
                    name=folder.get('displayName', ''),
                    unread_count=folder.get('unreadItemCount', 0),
                    total_count=folder.get('totalItemCount', 0)
                ))

            return folders
        except Exception as e:
            logger.error(f"Error listing folders: {e}")
            return []

    async def fetch_emails(
        self,
        folder_id: str,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[EmailMessage]:
        """Fetch emails from a folder."""
        try:
            if not self._authenticated:
                await self.authenticate()

            # Build filter
            filter_parts = []
            if since:
                since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
                filter_parts.append(f"receivedDateTime ge {since_str}")

            params = {
                "$top": limit,
                "$orderby": "receivedDateTime desc",
                "$select": "id,conversationId,parentFolderId,from,toRecipients,ccRecipients,subject,bodyPreview,body,receivedDateTime,sentDateTime,isRead,flag,hasAttachments"
            }
            if filter_parts:
                params["$filter"] = " and ".join(filter_parts)

            result = await self._make_request(
                "GET",
                f"/users/{self.user_email}/mailFolders/{folder_id}/messages",
                params=params
            )

            emails = []
            for msg in result.get('value', []):
                try:
                    email = self._parse_email(msg)
                    emails.append(email)
                except Exception as e:
                    logger.warning(f"Error parsing email {msg.get('id')}: {e}")

            return emails
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []

    async def get_email_content(self, message_id: str) -> Optional[EmailMessage]:
        """Get full email content."""
        try:
            if not self._authenticated:
                await self.authenticate()

            result = await self._make_request(
                "GET",
                f"/users/{self.user_email}/messages/{message_id}",
                params={
                    "$expand": "attachments",
                    "$select": "id,conversationId,parentFolderId,from,toRecipients,ccRecipients,subject,bodyPreview,body,receivedDateTime,sentDateTime,isRead,flag,hasAttachments,attachments"
                }
            )

            return self._parse_email(result)
        except Exception as e:
            logger.error(f"Error getting email content: {e}")
            return None

    async def mark_as_read(self, message_id: str) -> bool:
        """Mark email as read."""
        try:
            if not self._authenticated:
                await self.authenticate()

            await self._make_request(
                "PATCH",
                f"/users/{self.user_email}/messages/{message_id}",
                json_data={"isRead": True}
            )
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

            url = f"{self.GRAPH_BASE_URL}/users/{self.user_email}/messages/{message_id}/attachments/{attachment_id}/$value"
            headers = {"Authorization": f"Bearer {self._access_token}"}

            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, timeout=60.0)
                response.raise_for_status()
                return response.content
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None

    async def disconnect(self) -> None:
        """Disconnect from Microsoft Graph API."""
        self._access_token = None
        self._authenticated = False
