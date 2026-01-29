"""
Email providers for different email services.
"""

from .base import EmailProvider, EmailMessage, EmailFolder
from .imap import IMAPProvider

# Optional providers - import only if dependencies available
try:
    from .microsoft import MicrosoftProvider
except ImportError:
    MicrosoftProvider = None

try:
    from .gmail import GmailProvider
except ImportError:
    GmailProvider = None

__all__ = [
    'EmailProvider',
    'EmailMessage',
    'EmailFolder',
    'IMAPProvider',
    'MicrosoftProvider',
    'GmailProvider',
]
