"""
Email synchronization manager.
Handles periodic email fetching and processing.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from .providers.base import EmailProvider, ProviderType
from .storage import EmailStorage
from .categorizer import EmailCategorizer, CustomerLinker

logger = logging.getLogger(__name__)


class EmailSyncManager:
    """
    Manages email synchronization from all configured providers.
    """

    def __init__(
        self,
        storage: EmailStorage,
        categorizer: Optional[EmailCategorizer] = None,
        linker: Optional[CustomerLinker] = None
    ):
        """
        Initialize sync manager.

        Args:
            storage: EmailStorage instance
            categorizer: EmailCategorizer instance (optional)
            linker: CustomerLinker instance (optional)
        """
        self.storage = storage
        self.categorizer = categorizer
        self.linker = linker
        self.providers: Dict[int, EmailProvider] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def register_provider(self, provider_id: int, provider: EmailProvider):
        """Register an email provider for syncing."""
        self.providers[provider_id] = provider
        logger.info(f"Registered provider {provider_id} ({provider.provider_type.value})")

    def unregister_provider(self, provider_id: int):
        """Unregister an email provider."""
        if provider_id in self.providers:
            del self.providers[provider_id]
            logger.info(f"Unregistered provider {provider_id}")

    async def start_periodic_sync(self, interval_minutes: int = 15):
        """Start periodic background sync."""
        if self._running:
            logger.warning("Sync already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop(interval_minutes))
        logger.info(f"Started periodic email sync (every {interval_minutes} minutes)")

    async def stop_periodic_sync(self):
        """Stop periodic background sync."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Stopped periodic email sync")

    async def _sync_loop(self, interval_minutes: int):
        """Main sync loop."""
        while self._running:
            try:
                await self.sync_all_providers()
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")

            # Wait for next sync
            await asyncio.sleep(interval_minutes * 60)

    async def sync_all_providers(self) -> Dict[str, Any]:
        """
        Sync emails from all registered providers.

        Returns:
            Dictionary with sync results per provider
        """
        results = {}
        enabled_providers = self.storage.get_all_providers(enabled_only=True)

        for provider_info in enabled_providers:
            provider_id = provider_info['id']
            if provider_id not in self.providers:
                logger.warning(f"Provider {provider_id} not registered")
                continue

            try:
                result = await self.sync_provider(provider_id)
                results[provider_info['name']] = result
            except Exception as e:
                logger.error(f"Error syncing provider {provider_id}: {e}")
                results[provider_info['name']] = {'success': False, 'error': str(e)}

        return results

    async def sync_provider(self, provider_id: int) -> Dict[str, Any]:
        """
        Sync emails from a specific provider.

        Args:
            provider_id: Provider database ID

        Returns:
            Dictionary with sync results
        """
        if provider_id not in self.providers:
            return {'success': False, 'error': 'Provider not registered'}

        provider = self.providers[provider_id]

        # Start sync log
        log_id = self.storage.start_sync_log(provider_id)

        try:
            # Ensure authenticated
            if not provider.is_authenticated:
                if not await provider.authenticate():
                    raise Exception("Authentication failed")

            # Get monitored folders
            folders = self.storage.get_folders(provider_id, monitored_only=True)
            if not folders:
                # If no folders configured, try to set up INBOX
                all_folders = await provider.list_folders()
                for folder in all_folders:
                    monitored = folder.name.upper() == 'INBOX'
                    self.storage.add_folder(
                        provider_id,
                        folder.folder_id,
                        folder.name,
                        monitored=monitored
                    )
                folders = self.storage.get_folders(provider_id, monitored_only=True)

            total_synced = 0

            for folder in folders:
                # Calculate since date (1 hour before last sync for overlap)
                since = None
                if folder.get('last_sync'):
                    try:
                        last_sync = datetime.fromisoformat(folder['last_sync'])
                        since = last_sync - timedelta(hours=1)
                    except Exception:
                        pass

                # Fetch emails
                logger.info(f"Fetching emails from {folder['folder_name']} (since: {since})")
                emails = await provider.fetch_emails(
                    folder['folder_id'],
                    since=since,
                    limit=100
                )

                for email in emails:
                    # Store email
                    email_id = self.storage.store_email(
                        provider_id,
                        folder['id'],
                        email
                    )

                    # Categorize if enabled
                    if self.categorizer and self.categorizer.llm:
                        try:
                            category_result = self.categorizer.categorize(
                                subject=email.subject,
                                from_address=email.from_address,
                                body=email.body_text or email.body_preview
                            )
                            self.storage.update_email_category(
                                email_id,
                                category_result['category'],
                                category_result['confidence'],
                                category_result.get('reason')
                            )
                        except Exception as e:
                            logger.warning(f"Error categorizing email {email_id}: {e}")

                    # Auto-link to customer if enabled
                    if self.linker and self.linker.sql_connector:
                        try:
                            customer = self.linker.find_customer_by_email(email.from_address)
                            if customer:
                                self.storage.link_email_to_customer(
                                    email_id,
                                    customer['sn_account'],
                                    linked_by='auto'
                                )
                        except Exception as e:
                            logger.warning(f"Error linking email {email_id}: {e}")

                    total_synced += 1

                # Update folder sync time
                self.storage.update_folder_sync(folder['id'])

            # Complete sync log
            self.storage.complete_sync_log(log_id, 'success', total_synced)

            logger.info(f"Sync completed for provider {provider_id}: {total_synced} emails")
            return {
                'success': True,
                'emails_synced': total_synced,
                'folders_synced': len(folders)
            }

        except Exception as e:
            logger.error(f"Sync failed for provider {provider_id}: {e}")
            self.storage.complete_sync_log(log_id, 'failed', error=str(e))
            return {'success': False, 'error': str(e)}

    def get_sync_status(self) -> Dict[str, Any]:
        """Get current sync status."""
        providers_status = []
        for provider_info in self.storage.get_all_providers():
            status = {
                'id': provider_info['id'],
                'name': provider_info['name'],
                'type': provider_info['provider_type'],
                'enabled': provider_info['enabled'],
                'last_sync': provider_info.get('last_sync'),
                'sync_status': provider_info.get('sync_status', 'unknown'),
                'registered': provider_info['id'] in self.providers
            }
            providers_status.append(status)

        return {
            'running': self._running,
            'providers': providers_status
        }
