"""
Email module for reading and routing emails from multiple providers.
Supports Microsoft 365, Gmail, and generic IMAP.
"""

from .providers.base import EmailProvider, EmailMessage, EmailFolder
from .storage import EmailStorage
from .categorizer import EmailCategorizer
from .sync import EmailSyncManager

__all__ = [
    'EmailProvider',
    'EmailMessage',
    'EmailFolder',
    'EmailStorage',
    'EmailCategorizer',
    'EmailSyncManager',
]
