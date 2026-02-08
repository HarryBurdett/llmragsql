"""
IMAP email provider implementation.
Supports generic IMAP servers with SSL/TLS.
"""

import imaplib
import email
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
import logging
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

from .base import EmailProvider, EmailMessage, EmailFolder, EmailAttachment, ProviderType

logger = logging.getLogger(__name__)


class IMAPProvider(EmailProvider):
    """
    IMAP email provider for generic email servers.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize IMAP provider.

        Config keys:
            server: IMAP server hostname
            port: IMAP port (default 993 for SSL)
            username: Email username
            password: Email password
            use_ssl: Use SSL/TLS connection (default True)
        """
        super().__init__(config)
        self.server = config.get('server')
        self.port = int(config.get('port', 993))
        self.username = config.get('username')
        self.password = config.get('password')
        self.use_ssl = config.get('use_ssl', True)
        self._connection: Optional[imaplib.IMAP4_SSL | imaplib.IMAP4] = None

    @property
    def provider_type(self) -> ProviderType:
        return ProviderType.IMAP

    def _decode_header_value(self, value: str) -> str:
        """Decode email header value."""
        if not value:
            return ""
        decoded_parts = decode_header(value)
        result = []
        for content, charset in decoded_parts:
            if isinstance(content, bytes):
                try:
                    result.append(content.decode(charset or 'utf-8', errors='replace'))
                except (LookupError, UnicodeDecodeError):
                    result.append(content.decode('utf-8', errors='replace'))
            else:
                result.append(content)
        return ''.join(result)

    def _parse_email_address(self, header_value: str) -> tuple[str, str]:
        """Parse email address from header, returns (name, address)."""
        if not header_value:
            return ("", "")
        name, address = parseaddr(header_value)
        name = self._decode_header_value(name)
        return (name, address)

    def _parse_address_list(self, header_value: str) -> List[str]:
        """Parse multiple email addresses from header."""
        if not header_value:
            return []
        addresses = []
        for addr in header_value.split(','):
            _, email_addr = parseaddr(addr.strip())
            if email_addr:
                addresses.append(email_addr)
        return addresses

    def _get_body_content(self, msg: email.message.Message) -> tuple[str, str, str]:
        """Extract body content from email message. Returns (preview, html, text)."""
        body_text = ""
        body_html = ""

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in content_disposition:
                    continue

                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or 'utf-8'
                        text = payload.decode(charset, errors='replace')

                        if content_type == "text/plain":
                            body_text = text
                        elif content_type == "text/html":
                            body_html = text
                except Exception as e:
                    logger.warning(f"Error decoding email part: {e}")
        else:
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = msg.get_content_charset() or 'utf-8'
                    text = payload.decode(charset, errors='replace')

                    if msg.get_content_type() == "text/html":
                        body_html = text
                    else:
                        body_text = text
            except Exception as e:
                logger.warning(f"Error decoding email body: {e}")

        # Generate preview from text or stripped HTML
        preview = body_text[:500] if body_text else ""
        if not preview and body_html:
            # Basic HTML stripping for preview
            import re
            preview = re.sub(r'<[^>]+>', ' ', body_html)
            preview = ' '.join(preview.split())[:500]

        return preview, body_html, body_text

    def _get_attachments(self, msg: email.message.Message) -> List[EmailAttachment]:
        """Extract attachment metadata from email."""
        attachments = []

        if msg.is_multipart():
            for i, part in enumerate(msg.walk()):
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in content_disposition or part.get_filename():
                    filename = part.get_filename()
                    if filename:
                        filename = self._decode_header_value(filename)

                    content_type = part.get_content_type()

                    # Get size from payload
                    payload = part.get_payload(decode=True)
                    size = len(payload) if payload else 0

                    attachments.append(EmailAttachment(
                        attachment_id=str(i),
                        filename=filename or f"attachment_{i}",
                        content_type=content_type,
                        size_bytes=size
                    ))

        return attachments

    def _parse_message(self, msg_data: bytes, folder_id: str) -> Optional[EmailMessage]:
        """Parse raw email data into EmailMessage."""
        try:
            msg = email.message_from_bytes(msg_data)

            # Get message ID
            message_id = msg.get('Message-ID', '')
            if not message_id:
                # Generate a fallback ID
                message_id = f"imap_{hash(msg_data)}"

            # Parse From
            from_name, from_address = self._parse_email_address(msg.get('From', ''))

            # Parse To and CC
            to_addresses = self._parse_address_list(msg.get('To', ''))
            cc_addresses = self._parse_address_list(msg.get('Cc', ''))

            # Parse Subject
            subject = self._decode_header_value(msg.get('Subject', ''))

            # Parse Date
            date_str = msg.get('Date')
            received_at = datetime.now()
            if date_str:
                try:
                    received_at = parsedate_to_datetime(date_str)
                except Exception:
                    pass

            # Get body content
            preview, body_html, body_text = self._get_body_content(msg)

            # Get attachments
            attachments = self._get_attachments(msg)

            # Check flags (would need to be passed from fetch)
            is_read = False  # Set by caller based on FLAGS

            return EmailMessage(
                message_id=message_id.strip('<>'),
                folder_id=folder_id,
                from_address=from_address,
                from_name=from_name,
                to_addresses=to_addresses,
                cc_addresses=cc_addresses,
                subject=subject,
                body_preview=preview,
                body_html=body_html,
                body_text=body_text,
                received_at=received_at,
                is_read=is_read,
                has_attachments=len(attachments) > 0,
                attachments=attachments,
                raw_headers={
                    'From': msg.get('From', ''),
                    'To': msg.get('To', ''),
                    'Subject': msg.get('Subject', ''),
                    'Date': msg.get('Date', ''),
                    'Message-ID': msg.get('Message-ID', '')
                }
            )
        except Exception as e:
            logger.error(f"Error parsing email: {e}")
            return None

    async def authenticate(self) -> bool:
        """Connect and authenticate to IMAP server."""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._connect)
            self._authenticated = True
            logger.info(f"IMAP authenticated to {self.server}")
            return True
        except Exception as e:
            logger.error(f"IMAP authentication failed: {e}")
            self._authenticated = False
            return False

    def _connect(self):
        """Synchronous connection method."""
        if self.use_ssl:
            self._connection = imaplib.IMAP4_SSL(self.server, self.port)
        else:
            self._connection = imaplib.IMAP4(self.server, self.port)

        self._connection.login(self.username, self.password)

    async def test_connection(self) -> Dict[str, Any]:
        """Test IMAP connection."""
        try:
            if not self._connection:
                await self.authenticate()

            loop = asyncio.get_event_loop()
            status, _ = await loop.run_in_executor(
                None, lambda: self._connection.noop()
            )

            if status == 'OK':
                return {'success': True, 'message': 'Connection successful'}
            else:
                return {'success': False, 'error': f'IMAP status: {status}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    async def list_folders(self) -> List[EmailFolder]:
        """Get all IMAP folders."""
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()
            status, folder_list = await loop.run_in_executor(
                None, lambda: self._connection.list()
            )

            folders = []
            if status == 'OK':
                for folder_data in folder_list:
                    if folder_data:
                        # Parse folder response: (flags) delimiter "name"
                        parts = folder_data.decode().split(' "')
                        if len(parts) >= 2:
                            folder_name = parts[-1].strip('"')
                            # Clean up folder name - remove leading quotes/slashes
                            folder_name = folder_name.lstrip('/"').strip()

                            # Skip examining each folder (causes connection drops on some servers)
                            # Just add the folder name without counts
                            folders.append(EmailFolder(
                                folder_id=folder_name,
                                name=folder_name,
                                unread_count=0,
                                total_count=0
                            ))

            # Always ensure INBOX is in the list
            if not any(f.name.upper() == 'INBOX' for f in folders):
                folders.insert(0, EmailFolder(
                    folder_id='INBOX',
                    name='INBOX',
                    unread_count=0,
                    total_count=0
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
        # Ensure we have a fresh connection
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Select folder - reconnect if connection was dropped
            try:
                await loop.run_in_executor(
                    None, lambda: self._connection.select(folder_id, readonly=True)
                )
            except Exception as select_err:
                logger.warning(f"Select failed, reconnecting: {select_err}")
                await self.authenticate()
                await loop.run_in_executor(
                    None, lambda: self._connection.select(folder_id, readonly=True)
                )

            # Build search criteria
            if since:
                date_str = since.strftime('%d-%b-%Y')
                search_criteria = f'(SINCE {date_str})'
            else:
                search_criteria = 'ALL'

            # Search for messages
            status, msg_ids = await loop.run_in_executor(
                None, lambda: self._connection.search(None, search_criteria)
            )

            if status != 'OK' or not msg_ids[0]:
                return []

            # Get message IDs and limit
            all_ids = msg_ids[0].split()
            # Get most recent messages (last N)
            selected_ids = all_ids[-limit:] if len(all_ids) > limit else all_ids

            emails = []
            for msg_id in selected_ids:
                try:
                    # Fetch message
                    status, msg_data = await loop.run_in_executor(
                        None, lambda mid=msg_id: self._connection.fetch(mid, '(RFC822 FLAGS)')
                    )

                    if status == 'OK' and msg_data[0]:
                        # Check if it's a tuple with message data
                        if isinstance(msg_data[0], tuple):
                            raw_email = msg_data[0][1]
                            flags = msg_data[0][0].decode() if msg_data[0][0] else ""

                            email_msg = self._parse_message(raw_email, folder_id)
                            if email_msg:
                                # Check if read
                                email_msg.is_read = '\\Seen' in flags
                                email_msg.is_flagged = '\\Flagged' in flags
                                emails.append(email_msg)
                except Exception as e:
                    logger.warning(f"Error fetching message {msg_id}: {e}")

            return emails
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            return []

    async def get_email_content(self, message_id: str) -> Optional[EmailMessage]:
        """Get full email content by message ID."""
        # For IMAP, we typically already have full content from fetch_emails
        # This method would need to search by Message-ID header
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Search by Message-ID header
            await loop.run_in_executor(
                None, lambda: self._connection.select('INBOX', readonly=True)
            )

            search_criteria = f'(HEADER Message-ID "<{message_id}>")'
            status, msg_ids = await loop.run_in_executor(
                None, lambda: self._connection.search(None, search_criteria)
            )

            if status != 'OK' or not msg_ids[0]:
                return None

            msg_id = msg_ids[0].split()[0]
            status, msg_data = await loop.run_in_executor(
                None, lambda: self._connection.fetch(msg_id, '(RFC822 FLAGS)')
            )

            if status == 'OK' and msg_data[0]:
                if isinstance(msg_data[0], tuple):
                    raw_email = msg_data[0][1]
                    return self._parse_message(raw_email, 'INBOX')

            return None
        except Exception as e:
            logger.error(f"Error getting email content: {e}")
            return None

    async def mark_as_read(self, message_id: str) -> bool:
        """Mark email as read."""
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Search for the message
            await loop.run_in_executor(
                None, lambda: self._connection.select('INBOX')
            )

            search_criteria = f'(HEADER Message-ID "<{message_id}>")'
            status, msg_ids = await loop.run_in_executor(
                None, lambda: self._connection.search(None, search_criteria)
            )

            if status != 'OK' or not msg_ids[0]:
                return False

            msg_id = msg_ids[0].split()[0]

            # Add Seen flag
            status, _ = await loop.run_in_executor(
                None, lambda: self._connection.store(msg_id, '+FLAGS', '\\Seen')
            )

            return status == 'OK'
        except Exception as e:
            logger.error(f"Error marking email as read: {e}")
            return False

    async def create_folder(self, folder_name: str) -> bool:
        """Create a folder if it doesn't exist."""
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Check if folder already exists
            status, folder_list = await loop.run_in_executor(
                None, lambda: self._connection.list('', folder_name)
            )

            if status == 'OK' and folder_list and folder_list[0]:
                # Folder already exists
                logger.info(f"Folder '{folder_name}' already exists")
                return True

            # Create the folder
            status, _ = await loop.run_in_executor(
                None, lambda: self._connection.create(folder_name)
            )

            if status == 'OK':
                logger.info(f"Created folder '{folder_name}'")
                return True
            else:
                logger.warning(f"Failed to create folder '{folder_name}': {status}")
                return False
        except Exception as e:
            logger.error(f"Error creating folder '{folder_name}': {e}")
            return False

    async def move_email(self, message_id: str, source_folder: str, dest_folder: str) -> bool:
        """
        Move an email from source folder to destination folder.

        Args:
            message_id: The Message-ID header value (without angle brackets)
            source_folder: Source folder name (e.g., 'INBOX')
            dest_folder: Destination folder name (e.g., 'Archive/GoCardless')

        Returns:
            True if move successful, False otherwise
        """
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Ensure destination folder exists
            await self.create_folder(dest_folder)

            # Select source folder (writable mode)
            try:
                await loop.run_in_executor(
                    None, lambda: self._connection.select(source_folder)
                )
            except Exception as select_err:
                logger.warning(f"Select failed, reconnecting: {select_err}")
                await self.authenticate()
                await loop.run_in_executor(
                    None, lambda: self._connection.select(source_folder)
                )

            # Search for the message by Message-ID
            search_criteria = f'(HEADER Message-ID "<{message_id}>")'
            status, msg_ids = await loop.run_in_executor(
                None, lambda: self._connection.search(None, search_criteria)
            )

            if status != 'OK' or not msg_ids[0]:
                logger.warning(f"Could not find email with Message-ID: {message_id}")
                return False

            msg_id = msg_ids[0].split()[0]

            # Copy to destination folder
            status, _ = await loop.run_in_executor(
                None, lambda mid=msg_id: self._connection.copy(mid, dest_folder)
            )

            if status != 'OK':
                logger.error(f"Failed to copy email to '{dest_folder}': {status}")
                return False

            # Mark original as deleted
            status, _ = await loop.run_in_executor(
                None, lambda mid=msg_id: self._connection.store(mid, '+FLAGS', '\\Deleted')
            )

            if status != 'OK':
                logger.warning(f"Failed to mark email as deleted: {status}")
                # Continue anyway - copy succeeded

            # Expunge deleted messages
            await loop.run_in_executor(
                None, lambda: self._connection.expunge()
            )

            logger.info(f"Moved email {message_id} to '{dest_folder}'")
            return True

        except Exception as e:
            logger.error(f"Error moving email: {e}")
            return False

    async def download_attachment(
        self,
        message_id: str,
        attachment_id: str,
        folder_id: str = 'INBOX'
    ) -> Optional[tuple[bytes, str, str]]:
        """
        Download an attachment from an email.

        Args:
            message_id: The Message-ID header value (without angle brackets)
            attachment_id: The attachment index (from EmailAttachment.attachment_id)
            folder_id: The folder containing the email

        Returns:
            Tuple of (content_bytes, filename, content_type) or None if not found
        """
        if not self._connection:
            await self.authenticate()

        try:
            loop = asyncio.get_event_loop()

            # Select the folder - reconnect if needed
            try:
                await loop.run_in_executor(
                    None, lambda: self._connection.select(folder_id, readonly=True)
                )
            except Exception as select_err:
                logger.warning(f"Select failed, reconnecting: {select_err}")
                await self.authenticate()
                await loop.run_in_executor(
                    None, lambda: self._connection.select(folder_id, readonly=True)
                )

            # Try multiple search strategies
            msg_id = None

            # Strategy 1: Search by Message-ID header
            search_criteria = f'(HEADER Message-ID "<{message_id}>")'
            logger.info(f"Strategy 1: Searching with criteria: {search_criteria}")
            status, msg_ids = await loop.run_in_executor(
                None, lambda: self._connection.search(None, search_criteria)
            )

            if status == 'OK' and msg_ids[0]:
                msg_id = msg_ids[0].split()[0]
                logger.info(f"Found email with Message-ID search: {msg_id}")
            else:
                # Strategy 2: Search all recent emails and match by Message-ID in content
                logger.info(f"Message-ID search failed, trying fallback strategy")
                status, all_ids = await loop.run_in_executor(
                    None, lambda: self._connection.search(None, 'ALL')
                )

                if status == 'OK' and all_ids[0]:
                    all_msg_ids = all_ids[0].split()
                    logger.info(f"Searching through {len(all_msg_ids)} emails")

                    # Search through recent emails (last 50)
                    for imap_id in all_msg_ids[-50:]:
                        try:
                            status, headers = await loop.run_in_executor(
                                None, lambda mid=imap_id: self._connection.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])')
                            )
                            if status == 'OK' and headers[0]:
                                header_data = headers[0][1] if isinstance(headers[0], tuple) else headers[0]
                                if isinstance(header_data, bytes):
                                    header_str = header_data.decode('utf-8', errors='replace')
                                    # Check if this email's Message-ID matches
                                    if message_id in header_str:
                                        msg_id = imap_id
                                        logger.info(f"Found email via header scan: {msg_id}")
                                        break
                        except Exception as scan_err:
                            logger.debug(f"Error scanning email {imap_id}: {scan_err}")
                            continue

            if not msg_id:
                logger.warning(f"Could not find email with Message-ID: {message_id} in folder {folder_id}")
                return None

            # Fetch the full message
            logger.info(f"Fetching full message: {msg_id}")
            status, msg_data = await loop.run_in_executor(
                None, lambda mid=msg_id: self._connection.fetch(mid, '(RFC822)')
            )

            if status != 'OK' or not msg_data[0]:
                logger.warning(f"Failed to fetch message: status={status}")
                return None

            if isinstance(msg_data[0], tuple):
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)

                # Find the attachment by walk index (attachment_id is the enumerate index from msg.walk())
                target_walk_idx = int(attachment_id)

                logger.info(f"Looking for attachment at walk index {target_walk_idx}, is_multipart={msg.is_multipart()}")

                if msg.is_multipart():
                    for i, part in enumerate(msg.walk()):
                        content_disposition = str(part.get("Content-Disposition", ""))
                        filename = part.get_filename()

                        if "attachment" in content_disposition or filename:
                            logger.debug(f"Found attachment at walk index {i}, filename={filename}")

                            # Compare against walk index, not attachment count
                            if i == target_walk_idx:
                                # Found the attachment
                                if filename:
                                    filename = self._decode_header_value(filename)
                                else:
                                    filename = f"attachment_{target_walk_idx}"

                                content_type = part.get_content_type()
                                payload = part.get_payload(decode=True)

                                if payload:
                                    logger.info(f"Downloaded attachment: {filename} ({len(payload)} bytes)")
                                    return (payload, filename, content_type)

            logger.warning(f"Attachment {attachment_id} not found in email {message_id}")
            return None

        except Exception as e:
            logger.error(f"Error downloading attachment: {e}", exc_info=True)
            return None

    async def disconnect(self) -> None:
        """Disconnect from IMAP server."""
        if self._connection:
            try:
                self._connection.logout()
            except Exception:
                pass
            self._connection = None
        self._authenticated = False
