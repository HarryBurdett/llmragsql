"""
Opera 3 Configuration Utilities

Provides functions to read Opera 3 FoxPro configuration including:
- Control account codes (debtors, creditors)
- System parameters
- Period validation (Open Period Accounting)

This mirrors opera_config.py but reads from DBF files instead of SQL.

Control accounts vary by installation and are stored in:
- Primary: sprfls table (sc_dbtctrl for debtors)
- Primary: pprfls table (pc_crdctrl for creditors)
- Fallback: nparm table (np_dca for debtors, np_cca for creditors)
"""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Opera3ControlAccounts:
    """Opera 3 control account configuration"""
    debtors_control: str  # Sales Ledger control account
    creditors_control: str  # Purchase Ledger control account
    source: str  # Where the values came from ('sprfls', 'pprfls', 'nparm', 'default')


@dataclass
class Opera3PeriodValidationResult:
    """Result of period validation for Opera 3"""
    is_valid: bool
    error_message: Optional[str] = None
    year: Optional[int] = None
    period: Optional[int] = None
    open_period_accounting: bool = False


class Opera3Config:
    """
    Configuration reader for Opera 3 FoxPro databases.

    Reads from DBF files to get control accounts, period settings,
    and other system configuration.
    """

    def __init__(self, data_path: str, encoding: str = 'cp1252'):
        """
        Initialize the Opera 3 config reader.

        Args:
            data_path: Path to Opera 3 company data folder
            encoding: Character encoding for DBF files
        """
        self.data_path = Path(data_path)
        self.encoding = encoding
        self._control_accounts_cache: Optional[Opera3ControlAccounts] = None

        # Lazy import to avoid circular dependencies
        self._reader = None

    def _get_reader(self):
        """Get or create the Opera3Reader instance"""
        if self._reader is None:
            from sql_rag.opera3_foxpro import Opera3Reader
            self._reader = Opera3Reader(str(self.data_path), encoding=self.encoding)
        return self._reader

    def _read_table_safe(self, table_name: str) -> List[Dict[str, Any]]:
        """Safely read a table, returning empty list if not found"""
        try:
            return self._get_reader().read_table(table_name)
        except FileNotFoundError:
            logger.debug(f"Table {table_name} not found")
            return []
        except Exception as e:
            logger.warning(f"Error reading table {table_name}: {e}")
            return []

    def get_control_accounts(self, use_cache: bool = True) -> Opera3ControlAccounts:
        """
        Get control account codes from Opera 3 configuration tables.

        Reads from:
        1. sprfls table (sc_dbtctrl) - debtors control
        2. pprfls table (pc_crdctrl) - creditors control
        3. nparm table (np_dca, np_cca) - fallback
        4. Default values if none found

        Args:
            use_cache: Whether to use cached values

        Returns:
            Opera3ControlAccounts with debtors and creditors control codes
        """
        if use_cache and self._control_accounts_cache:
            return self._control_accounts_cache

        debtors_control = None
        creditors_control = None
        source = 'default'

        # Try sprfls for debtors control
        sprfls = self._read_table_safe('sprfls')
        if sprfls:
            for record in sprfls:
                ctrl = record.get('SC_DBTCTRL', record.get('sc_dbtctrl', ''))
                if isinstance(ctrl, str):
                    ctrl = ctrl.strip()
                if ctrl:
                    debtors_control = ctrl
                    source = 'sprfls'
                    break

        # Try pprfls for creditors control
        pprfls = self._read_table_safe('pprfls')
        if pprfls:
            for record in pprfls:
                ctrl = record.get('PC_CRDCTRL', record.get('pc_crdctrl', ''))
                if isinstance(ctrl, str):
                    ctrl = ctrl.strip()
                if ctrl:
                    creditors_control = ctrl
                    if source == 'default':
                        source = 'pprfls'
                    break

        # Try nparm as fallback
        if not debtors_control or not creditors_control:
            nparm = self._read_table_safe('nparm')
            if nparm and len(nparm) > 0:
                row = nparm[0]
                if not debtors_control:
                    dca = row.get('NP_DCA', row.get('np_dca', ''))
                    if isinstance(dca, str):
                        dca = dca.strip()
                    if dca:
                        debtors_control = dca
                        if source == 'default':
                            source = 'nparm'

                if not creditors_control:
                    cca = row.get('NP_CCA', row.get('np_cca', ''))
                    if isinstance(cca, str):
                        cca = cca.strip()
                    if cca:
                        creditors_control = cca
                        if source == 'default':
                            source = 'nparm'

        # Use defaults if still not found
        if not debtors_control:
            debtors_control = 'BB020'
            logger.warning(f"Using default debtors control account: {debtors_control}")
        if not creditors_control:
            creditors_control = 'CA030'
            logger.warning(f"Using default creditors control account: {creditors_control}")

        result = Opera3ControlAccounts(
            debtors_control=debtors_control,
            creditors_control=creditors_control,
            source=source
        )

        self._control_accounts_cache = result
        logger.info(f"Opera 3 control accounts from {source}: debtors={debtors_control}, creditors={creditors_control}")
        return result

    def clear_cache(self):
        """Clear all cached values"""
        self._control_accounts_cache = None

    def is_open_period_accounting_enabled(self) -> bool:
        """
        Check if Open Period Accounting is enabled.

        Reads from opera3sesystem.co_opanl field.

        Returns:
            True if OPA is enabled, False otherwise
        """
        # Try opera3sesystem table
        try:
            opera3sesystem = self._read_table_safe('opera3sesystem')
            if opera3sesystem and len(opera3sesystem) > 0:
                row = opera3sesystem[0]
                co_opanl = row.get('CO_OPANL', row.get('co_opanl', ''))
                if isinstance(co_opanl, str):
                    co_opanl = co_opanl.strip().upper()
                enabled = co_opanl == 'Y'
                logger.debug(f"Open Period Accounting enabled: {enabled} (opera3sesystem.co_opanl='{co_opanl}')")
                return enabled
        except Exception as e:
            logger.debug(f"Could not read opera3sesystem: {e}")

        # Default to disabled (stricter mode)
        logger.warning("Could not determine Open Period Accounting setting, defaulting to disabled")
        return False

    def get_current_period_info(self) -> Dict[str, Any]:
        """
        Get current period information from nparm.

        Returns:
            Dictionary with np_year, np_perno, np_periods
        """
        try:
            nparm = self._read_table_safe('nparm')
            if nparm and len(nparm) > 0:
                row = nparm[0]
                np_year = row.get('NP_YEAR', row.get('np_year'))
                np_perno = row.get('NP_PERNO', row.get('np_perno'))
                np_periods = row.get('NP_PERIODS', row.get('np_periods', 12))

                return {
                    'np_year': int(np_year) if np_year else None,
                    'np_perno': int(np_perno) if np_perno else None,
                    'np_periods': int(np_periods) if np_periods else 12
                }
        except Exception as e:
            logger.warning(f"Could not read current period from nparm: {e}")

        return {'np_year': None, 'np_perno': None, 'np_periods': 12}

    def get_period_status(self, year: int, period: int, ledger_type: str) -> Optional[int]:
        """
        Get the period status for a specific ledger from nclndd.

        Args:
            year: Financial year
            period: Period number (1-12)
            ledger_type: One of 'NL', 'SL', 'PL', 'ST', 'WG', 'FA'

        Returns:
            Status value (0=Open, 1=Current, 2=Closed) or None if not found
        """
        status_field_map = {
            'NL': ('NCD_NLSTAT', 'ncd_nlstat'),
            'SL': ('NCD_SLSTAT', 'ncd_slstat'),
            'PL': ('NCD_PLSTAT', 'ncd_plstat'),
            'ST': ('NCD_STSTAT', 'ncd_ststat'),
            'WG': ('NCD_WGSTAT', 'ncd_wgstat'),
            'FA': ('NCD_FASTAT', 'ncd_fastat')
        }

        if ledger_type not in status_field_map:
            raise ValueError(f"Invalid ledger_type: {ledger_type}. Must be one of {list(status_field_map.keys())}")

        field_upper, field_lower = status_field_map[ledger_type]

        try:
            nclndd = self._read_table_safe('nclndd')
            if not nclndd:
                return None

            for row in nclndd:
                ncd_year = row.get('NCD_YEAR', row.get('ncd_year'))
                ncd_period = row.get('NCD_PERIOD', row.get('ncd_period'))

                if ncd_year is not None and ncd_period is not None:
                    if int(ncd_year) == year and int(ncd_period) == period:
                        status = row.get(field_upper, row.get(field_lower))
                        if status is not None:
                            status_int = int(status)
                            logger.debug(f"Period {period}/{year} {ledger_type} status: {status_int}")
                            return status_int
                        break
        except Exception as e:
            logger.warning(f"Could not read period status from nclndd: {e}")

        return None

    def validate_posting_period(
        self,
        post_date,
        ledger_type: str = 'NL'
    ) -> Opera3PeriodValidationResult:
        """
        Validate that a transaction can be posted to the target period.

        This implements Opera's period control logic:
        - If Open Period Accounting is OFF: Only current period is allowed
        - If Open Period Accounting is ON: Check nclndd for per-ledger status

        Args:
            post_date: Date of transaction (date object or string 'YYYY-MM-DD')
            ledger_type: Ledger type - 'NL' (Nominal), 'SL' (Sales), 'PL' (Purchase),
                        'ST' (Stock), 'WG' (Wages), 'FA' (Fixed Assets)

        Returns:
            Opera3PeriodValidationResult with is_valid, error_message, and period info
        """
        # Parse post_date if string
        if isinstance(post_date, str):
            post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

        year = post_date.year
        period = post_date.month

        # Check if Open Period Accounting is enabled
        open_period_enabled = self.is_open_period_accounting_enabled()

        if not open_period_enabled:
            # Stricter mode: Only current period allowed
            current = self.get_current_period_info()

            if current['np_year'] is None or current['np_perno'] is None:
                logger.warning("Could not determine current period - allowing post")
                return Opera3PeriodValidationResult(
                    is_valid=True,
                    year=year,
                    period=period,
                    open_period_accounting=False
                )

            if year != current['np_year'] or period != current['np_perno']:
                return Opera3PeriodValidationResult(
                    is_valid=False,
                    error_message=f"Period {period}/{year} is not the current period. "
                                  f"Current period is {current['np_perno']}/{current['np_year']}. "
                                  f"Open Period Accounting is disabled.",
                    year=year,
                    period=period,
                    open_period_accounting=False
                )

            return Opera3PeriodValidationResult(
                is_valid=True,
                year=year,
                period=period,
                open_period_accounting=False
            )

        else:
            # Open Period Accounting enabled: Check nclndd for ledger-specific status
            status = self.get_period_status(year, period, ledger_type)

            if status is None:
                return Opera3PeriodValidationResult(
                    is_valid=False,
                    error_message=f"Period {period}/{year} not found in calendar (nclndd)",
                    year=year,
                    period=period,
                    open_period_accounting=True
                )

            if status == 2:  # Closed
                ledger_names = {
                    'NL': 'Nominal Ledger',
                    'SL': 'Sales Ledger',
                    'PL': 'Purchase Ledger',
                    'ST': 'Stock',
                    'WG': 'Wages',
                    'FA': 'Fixed Assets'
                }
                ledger_name = ledger_names.get(ledger_type, ledger_type)
                return Opera3PeriodValidationResult(
                    is_valid=False,
                    error_message=f"{ledger_name} is closed for period {period}/{year}",
                    year=year,
                    period=period,
                    open_period_accounting=True
                )

            # Status 0 (Open) or 1 (Current) - allow posting
            return Opera3PeriodValidationResult(
                is_valid=True,
                year=year,
                period=period,
                open_period_accounting=True
            )


def get_ledger_type_for_transaction(transaction_type: str) -> str:
    """
    Get the appropriate ledger type for period validation based on transaction type.

    Args:
        transaction_type: Type of transaction (e.g., 'sales_receipt', 'purchase_payment')

    Returns:
        Ledger type code ('NL', 'SL', 'PL', etc.)
    """
    ledger_map = {
        'sales_receipt': 'SL',
        'sales_invoice': 'SL',
        'sales_credit': 'SL',
        'purchase_payment': 'PL',
        'purchase_invoice': 'PL',
        'purchase_credit': 'PL',
        'nominal_journal': 'NL',
        'bank_receipt': 'NL',
        'bank_payment': 'NL',
        'stock_adjustment': 'ST',
        'payroll': 'WG',
        'fixed_asset': 'FA'
    }
    return ledger_map.get(transaction_type.lower(), 'NL')


# =============================================================================
# PERIOD POSTING RULES
# =============================================================================

@dataclass
class Opera3PeriodPostingDecision:
    """
    Decision on how to post a transaction based on period rules.

    Rules:
    1. If transaction year != current year -> REJECT
    2. If transaction period == current nominal period -> Post to NL + transfer file (done='Y')
    3. If transaction period != current period but same year -> Transfer file only (done=' ')
    """
    can_post: bool
    post_to_nominal: bool = False
    post_to_transfer_file: bool = False
    transfer_file_done_flag: str = ' '  # 'Y' = posted to NL, ' ' = pending
    error_message: Optional[str] = None
    current_year: Optional[int] = None
    current_period: Optional[int] = None
    transaction_year: Optional[int] = None
    transaction_period: Optional[int] = None


def get_period_posting_decision(config: Opera3Config, post_date) -> Opera3PeriodPostingDecision:
    """
    Determine how a transaction should be posted based on period rules.

    Rules:
    1. Transaction NOT in current year -> REJECT (don't post to any ledger)
    2. Transaction in current nominal period -> Post to ntran + transfer file with done='Y'
    3. Transaction in current year but different period -> Transfer file only with done=' '

    Args:
        config: Opera3Config instance
        post_date: Transaction date (date object or 'YYYY-MM-DD' string)

    Returns:
        Opera3PeriodPostingDecision with posting instructions
    """
    # Parse post_date if string
    if isinstance(post_date, str):
        post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

    txn_year = post_date.year
    txn_period = post_date.month

    # Get current period info from nparm
    current_info = config.get_current_period_info()
    current_year = current_info.get('np_year')
    current_period = current_info.get('np_perno')

    if current_year is None or current_period is None:
        logger.warning("Could not determine current period - defaulting to allow posting")
        return Opera3PeriodPostingDecision(
            can_post=True,
            post_to_nominal=True,
            post_to_transfer_file=True,
            transfer_file_done_flag='Y',
            current_year=current_year,
            current_period=current_period,
            transaction_year=txn_year,
            transaction_period=txn_period
        )

    # Rule 1: Transaction NOT in current year -> REJECT
    if txn_year != current_year:
        return Opera3PeriodPostingDecision(
            can_post=False,
            post_to_nominal=False,
            post_to_transfer_file=False,
            error_message=f"Transaction year {txn_year} does not match current financial year {current_year}. "
                         f"Transactions can only be posted to the current year.",
            current_year=current_year,
            current_period=current_period,
            transaction_year=txn_year,
            transaction_period=txn_period
        )

    # Rule 2: Transaction in current nominal period -> Post to NL + transfer file (done='Y')
    if txn_period == current_period:
        return Opera3PeriodPostingDecision(
            can_post=True,
            post_to_nominal=True,
            post_to_transfer_file=True,
            transfer_file_done_flag='Y',
            current_year=current_year,
            current_period=current_period,
            transaction_year=txn_year,
            transaction_period=txn_period
        )

    # Rule 3: Transaction in current year but different period -> Transfer file only (done=' ')
    return Opera3PeriodPostingDecision(
        can_post=True,
        post_to_nominal=False,
        post_to_transfer_file=True,
        transfer_file_done_flag=' ',
        current_year=current_year,
        current_period=current_period,
        transaction_year=txn_year,
        transaction_period=txn_period
    )


# Convenience function to create config instance
def get_opera3_config(data_path: str, encoding: str = 'cp1252') -> Opera3Config:
    """Create an Opera3Config instance for the given data path"""
    return Opera3Config(data_path, encoding)
