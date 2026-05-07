"""Phase B ports — interface definitions for app dependencies.

Apps import ports from here:

    from apps.core.ports import OperaSQLPort, EmailStoragePort

…and obtain runtime instances via the factory:

    from apps.core.adapters.factory import get_opera_sql, get_email_storage

The ports are Protocols (PEP 544) — duck-typed, runtime-checkable.
Mocks satisfy them automatically; no ABC inheritance required.
"""

from .auth import AuthPort
from .email_storage import EmailStoragePort
from .email_sync import EmailSyncPort
from .opera3_reader import Opera3ReaderPort
from .opera3_writer import Opera3WriterPort
from .opera_sql import OperaSQLPort, to_records
from .smtp import SMTPPort

__all__ = [
    "AuthPort",
    "EmailStoragePort",
    "EmailSyncPort",
    "Opera3ReaderPort",
    "Opera3WriterPort",
    "OperaSQLPort",
    "SMTPPort",
    "to_records",
]
