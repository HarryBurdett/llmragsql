"""
Abstract base class for email providers.
Defines the interface that all email providers must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ProviderType(str, Enum):
    """Supported email provider types."""
    MICROSOFT = "microsoft"
    GMAIL = "gmail"
    IMAP = "imap"


@dataclass
class EmailAttachment:
    """Represents an email attachment."""
    attachment_id: str
    filename: str
    content_type: str
    size_bytes: int


@dataclass
class EmailMessage:
    """Standardized email message structure across all providers."""
    message_id: str
    folder_id: str
    from_address: str
    subject: str
    received_at: datetime

    # Optional fields
    thread_id: Optional[str] = None
    from_name: Optional[str] = None
    to_addresses: List[str] = field(default_factory=list)
    cc_addresses: List[str] = field(default_factory=list)
    body_preview: str = ""
    body_html: Optional[str] = None
    body_text: Optional[str] = None
    sent_at: Optional[datetime] = None
    is_read: bool = False
    is_flagged: bool = False
    has_attachments: bool = False
    attachments: List[EmailAttachment] = field(default_factory=list)
    raw_headers: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'message_id': self.message_id,
            'thread_id': self.thread_id,
            'folder_id': self.folder_id,
            'from_address': self.from_address,
            'from_name': self.from_name,
            'to_addresses': self.to_addresses,
            'cc_addresses': self.cc_addresses,
            'subject': self.subject,
            'body_preview': self.body_preview,
            'body_html': self.body_html,
            'body_text': self.body_text,
            'received_at': self.received_at.isoformat() if self.received_at else None,
            'sent_at': self.sent_at.isoformat() if self.sent_at else None,
            'is_read': self.is_read,
            'is_flagged': self.is_flagged,
            'has_attachments': self.has_attachments,
            'attachments': [
                {
                    'attachment_id': a.attachment_id,
                    'filename': a.filename,
                    'content_type': a.content_type,
                    'size_bytes': a.size_bytes
                }
                for a in self.attachments
            ],
            'raw_headers': self.raw_headers
        }


@dataclass
class EmailFolder:
    """Represents an email folder/label."""
    folder_id: str
    name: str
    unread_count: int = 0
    total_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'folder_id': self.folder_id,
            'name': self.name,
            'unread_count': self.unread_count,
            'total_count': self.total_count
        }


class EmailProvider(ABC):
    """
    Abstract base class for email providers.

    All email providers (Microsoft 365, Gmail, IMAP) must implement this interface.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the email provider.

        Args:
            config: Provider-specific configuration dictionary
        """
        self.config = config
        self._authenticated = False

    @property
    @abstractmethod
    def provider_type(self) -> ProviderType:
        """Return the provider type identifier."""
        pass

    @property
    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        return self._authenticated

    @abstractmethod
    async def authenticate(self) -> bool:
        """
        Authenticate with the email provider.

        Returns:
            True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to the email provider.

        Returns:
            Dictionary with 'success' boolean and optional 'error' message
        """
        pass

    @abstractmethod
    async def list_folders(self) -> List[EmailFolder]:
        """
        Get all available folders/labels.

        Returns:
            List of EmailFolder objects
        """
        pass

    @abstractmethod
    async def fetch_emails(
        self,
        folder_id: str,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[EmailMessage]:
        """
        Fetch emails from a specific folder.

        Args:
            folder_id: The folder/label ID to fetch from
            since: Only fetch emails received after this datetime
            limit: Maximum number of emails to fetch

        Returns:
            List of EmailMessage objects
        """
        pass

    @abstractmethod
    async def get_email_content(self, message_id: str) -> Optional[EmailMessage]:
        """
        Get full email content including body.

        Args:
            message_id: The message ID to fetch

        Returns:
            EmailMessage with full content, or None if not found
        """
        pass

    @abstractmethod
    async def mark_as_read(self, message_id: str) -> bool:
        """
        Mark an email as read.

        Args:
            message_id: The message ID to mark

        Returns:
            True if successful, False otherwise
        """
        pass

    async def download_attachment(
        self,
        message_id: str,
        attachment_id: str
    ) -> Optional[bytes]:
        """
        Download an email attachment.

        Args:
            message_id: The message ID
            attachment_id: The attachment ID

        Returns:
            Attachment content as bytes, or None if not found
        """
        # Default implementation - providers can override
        raise NotImplementedError("Attachment download not implemented for this provider")

    async def disconnect(self) -> None:
        """Disconnect from the email provider."""
        self._authenticated = False
