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

        # Raise error if not found — control accounts vary by company, never hardcode
        if not debtors_control:
            raise ValueError(
                "Debtors control account not found in Opera 3 configuration "
                "(checked sprfls.sc_dbtctrl and nparm.np_dca). "
                "Verify the Opera 3 data is accessible and control accounts are configured."
            )
        if not creditors_control:
            raise ValueError(
                "Creditors control account not found in Opera 3 configuration "
                "(checked pprfls.pc_crdctrl and nparm.np_cca). "
                "Verify the Opera 3 data is accessible and control accounts are configured."
            )

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

        Reads from seqco.co_opanl field (company profile).

        Returns:
            True if OPA is enabled, False otherwise
        """
        # Try seqco table first (company profile)
        try:
            seqco = self._read_table_safe('seqco')
            if seqco and len(seqco) > 0:
                row = seqco[0]
                co_opanl = row.get('CO_OPANL', row.get('co_opanl', ''))
                if isinstance(co_opanl, str):
                    co_opanl = co_opanl.strip().upper()
                    enabled = co_opanl == 'Y'
                elif isinstance(co_opanl, bool):
                    enabled = co_opanl
                else:
                    enabled = bool(co_opanl)
                logger.debug(f"Open Period Accounting enabled: {enabled} (seqco.co_opanl='{co_opanl}')")
                return enabled
        except Exception as e:
            logger.debug(f"Could not read seqco: {e}")

        # Default to disabled (stricter mode)
        logger.warning("Could not determine Open Period Accounting setting, defaulting to disabled")
        return False

    def is_real_time_update_enabled(self) -> bool:
        """
        Check if Real Time Update is enabled.

        Real Time Update determines whether the Nominal Ledger is updated immediately
        when transactions are posted in other applications (Sales, Purchase, Cashbook, etc.)
        or whether transactions go to transfer files for batch processing.

        Reads from seqco.co_rtupdnl field.

        Returns:
            True if Real Time Update is enabled, False otherwise
        """
        try:
            seqco = self._read_table_safe('seqco')
            if seqco and len(seqco) > 0:
                row = seqco[0]
                co_rtupdnl = row.get('CO_RTUPDNL', row.get('co_rtupdnl', ''))
                if isinstance(co_rtupdnl, str):
                    co_rtupdnl = co_rtupdnl.strip().upper()
                    enabled = co_rtupdnl == 'Y'
                elif isinstance(co_rtupdnl, bool):
                    enabled = co_rtupdnl
                else:
                    enabled = bool(co_rtupdnl)
                logger.debug(f"Real Time Update enabled: {enabled} (seqco.co_rtupdnl='{co_rtupdnl}')")
                return enabled
        except Exception as e:
            logger.debug(f"Could not read seqco: {e}")

        # Default to disabled (batch transfer mode)
        logger.warning("Could not determine Real Time Update setting, defaulting to disabled")
        return False

    def get_advanced_nominal_config(self) -> Dict[str, Any]:
        """
        Check if Advanced Nominal analysis levels (Project/Department) are enabled,
        and read the custom field labels from seqsys.

        Reads CO_ADVPROJ and CO_ADVJOB from seqco company profile (per-company).
        Reads SY_NLPROJ and SY_NLJOB from seqsys for custom field names.

        Returns:
            Dictionary with project_enabled (bool), department_enabled (bool),
            project_label (str), department_label (str)
        """
        result = {
            "project_enabled": False,
            "department_enabled": False,
            "project_label": "Project",
            "department_label": "Department",
        }

        try:
            seqco = self._read_table_safe('seqco')
            if seqco and len(seqco) > 0:
                row = seqco[0]
                for field_name, key in [('CO_ADVPROJ', 'project_enabled'), ('CO_ADVJOB', 'department_enabled')]:
                    value = row.get(field_name, row.get(field_name.lower(), ''))
                    if isinstance(value, str):
                        value = value.strip().upper()
                        result[key] = value in ('Y', '1', 'T', 'TRUE')
                    elif isinstance(value, bool):
                        result[key] = value
                    else:
                        result[key] = bool(value)
                logger.debug(f"Advanced Nominal config: project={result['project_enabled']}, department={result['department_enabled']}")
        except Exception as e:
            logger.debug(f"Could not read advanced nominal config from seqco: {e}")

        # Read custom field labels from seqsys (System Preferences)
        try:
            seqsys = self._read_table_safe('seqsys')
            if seqsys and len(seqsys) > 0:
                row = seqsys[0]
                for field_name, key in [('SY_NLPROJ', 'project_label'), ('SY_NLJOB', 'department_label')]:
                    value = row.get(field_name, row.get(field_name.lower(), ''))
                    if isinstance(value, str):
                        value = value.strip()
                        if value:
                            result[key] = value
                logger.debug(f"Custom field labels: project='{result['project_label']}', department='{result['department_label']}'")
        except Exception as e:
            logger.debug(f"Could not read custom field labels from seqsys: {e}")

        return result

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

    def get_period_for_date(self, post_date) -> tuple:
        """
        Look up the correct financial period and year for a date from the nominal calendar.

        Uses nclndd table to map dates to Opera's financial periods.

        Args:
            post_date: Transaction date (date object or 'YYYY-MM-DD' string)

        Returns:
            Tuple of (period, year)
        """
        if isinstance(post_date, str):
            post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

        try:
            nclndd = self._read_table_safe('nclndd')
            if nclndd:
                for row in nclndd:
                    ncd_stdate = row.get('NCD_STDATE', row.get('ncd_stdate'))
                    ncd_endate = row.get('NCD_ENDATE', row.get('ncd_endate'))
                    ncd_period = row.get('NCD_PERIOD', row.get('ncd_period'))
                    ncd_year = row.get('NCD_YEAR', row.get('ncd_year'))

                    if ncd_stdate and ncd_endate and ncd_period is not None and ncd_year is not None:
                        # Parse dates if they're strings
                        if isinstance(ncd_stdate, str):
                            ncd_stdate = datetime.strptime(ncd_stdate, '%Y-%m-%d').date()
                        if isinstance(ncd_endate, str):
                            ncd_endate = datetime.strptime(ncd_endate, '%Y-%m-%d').date()
                        if hasattr(ncd_stdate, 'date'):
                            ncd_stdate = ncd_stdate.date()
                        if hasattr(ncd_endate, 'date'):
                            ncd_endate = ncd_endate.date()

                        if ncd_stdate <= post_date <= ncd_endate:
                            period = int(ncd_period)
                            year = int(ncd_year)
                            logger.debug(f"Date {post_date} maps to period {period}/{year} from nclndd")
                            return (period, year)
        except Exception as e:
            logger.warning(f"Could not look up period from nclndd for date {post_date}: {e}")

        # Fallback: use calendar month
        logger.warning(f"No nclndd entry found for date {post_date} - falling back to calendar month {post_date.month}")
        return (post_date.month, post_date.year)

    def get_period_status(self, year: int, period: int, ledger_type: str) -> Optional[int]:
        """
        Get the period status for a specific ledger from nclndd.

        Args:
            year: Financial year
            period: Period number (1-12)
            ledger_type: One of 'NL', 'SL', 'PL', 'ST', 'WG', 'FA'

        Returns:
            Status value (0=Open, 1=Blocked, 2=Closed) or None if not found
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

        # Look up correct period/year from nominal calendar
        period, year = self.get_period_for_date(post_date)

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
                    error_message=f"Period {period}/{year} is blocked. "
                                  f"Current period is {current['np_perno']}/{current['np_year']}.",
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
            # Open Period Accounting enabled: Always check NL first (master gatekeeper)
            nl_status = self.get_period_status(year, period, 'NL')

            if nl_status is None:
                return Opera3PeriodValidationResult(
                    is_valid=False,
                    error_message=f"Period {period}/{year} not found in calendar (nclndd)",
                    year=year,
                    period=period,
                    open_period_accounting=True
                )

            # NL blocked/closed -> reject everything (NL is master gatekeeper)
            if nl_status != 0:
                status_desc = "closed" if nl_status == 2 else "blocked"
                return Opera3PeriodValidationResult(
                    is_valid=False,
                    error_message=f"Nominal Ledger is {status_desc} for period {period}/{year} — all ledgers blocked",
                    year=year,
                    period=period,
                    open_period_accounting=True
                )

            # If sub-ledger specified, also check its status
            if ledger_type != 'NL':
                sub_status = self.get_period_status(year, period, ledger_type)
                if sub_status is not None and sub_status != 0:
                    ledger_names = {
                        'SL': 'Sales Ledger',
                        'PL': 'Purchase Ledger',
                        'ST': 'Stock',
                        'WG': 'Wages',
                        'FA': 'Fixed Assets'
                    }
                    ledger_name = ledger_names.get(ledger_type, ledger_type)
                    status_desc = "closed" if sub_status == 2 else "blocked"
                    return Opera3PeriodValidationResult(
                        is_valid=False,
                        error_message=f"{ledger_name} is {status_desc} for period {period}/{year}",
                        year=year,
                        period=period,
                        open_period_accounting=True
                    )

            # All required ledgers open - allow posting
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

    Rules (OPA enabled):
    1. NL blocked/closed for period -> REJECT (NL is master gatekeeper)
    2. Sub-ledger (SL/PL) blocked/closed for period -> REJECT
    3. Both open + period >= current -> Post to NL immediately + transfer file (done='Y')
    4. Both open + period < current (backdated) -> Transfer file only (done=' ')

    Rules (OPA disabled):
    1. Only current period allowed, otherwise REJECT
    2. Post to NL immediately + transfer file (done='Y')
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


def get_period_posting_decision(config: Opera3Config, post_date, ledger_type: str = 'NL') -> Opera3PeriodPostingDecision:
    """
    Determine how a transaction should be posted based on OPA and period rules.

    Decision Logic:
    1. Check OPA setting
       - If OFF: Only current period allowed, otherwise REJECT
       - If ON: Check nclndd period calendar for each ledger
    2. When OPA ON, check nclndd statuses:
       - NL (ncd_nlstat) MUST be open — NL is master gatekeeper for all ledgers
       - Sub-ledger (ncd_slstat/ncd_plstat) MUST also be open if ledger_type is SL/PL
       - If either is blocked/closed -> REJECT
    3. Period rules (when all required ledgers open):
       - Period >= current: Post to NL immediately + transfer file (done='Y')
       - Period < current (backdated): Transfer file only (done=' '), manual transfer or period end needed

    Args:
        config: Opera3Config instance
        post_date: Transaction date (date object or 'YYYY-MM-DD' string)
        ledger_type: Ledger type ('NL', 'SL', 'PL', etc.) — NL is always checked in addition

    Returns:
        Opera3PeriodPostingDecision with posting instructions
    """
    # Parse post_date if string
    if isinstance(post_date, str):
        post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

    # Look up correct period/year from nominal calendar
    txn_period, txn_year = config.get_period_for_date(post_date)

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

    # Check if Open Period Accounting is enabled
    opa_enabled = config.is_open_period_accounting_enabled()

    # Step 1: Check period is allowed
    if opa_enabled:
        # OPA ON: Always check NL status first (master gatekeeper)
        nl_status = config.get_period_status(txn_year, txn_period, 'NL')
        if nl_status is None:
            return Opera3PeriodPostingDecision(
                can_post=False,
                post_to_nominal=False,
                post_to_transfer_file=False,
                error_message=f"Period {txn_period}/{txn_year} not found in calendar (nclndd)",
                current_year=current_year,
                current_period=current_period,
                transaction_year=txn_year,
                transaction_period=txn_period
            )
        # NL blocked/closed -> reject everything (NL is master gatekeeper)
        if nl_status != 0:
            status_desc = "closed" if nl_status == 2 else "blocked"
            return Opera3PeriodPostingDecision(
                can_post=False,
                post_to_nominal=False,
                post_to_transfer_file=False,
                error_message=f"Period {txn_period}/{txn_year} is {status_desc} for NL — all ledgers blocked",
                current_year=current_year,
                current_period=current_period,
                transaction_year=txn_year,
                transaction_period=txn_period
            )

        # If sub-ledger specified (SL/PL), also check its status
        if ledger_type != 'NL':
            sub_status = config.get_period_status(txn_year, txn_period, ledger_type)
            if sub_status is None:
                return Opera3PeriodPostingDecision(
                    can_post=False,
                    post_to_nominal=False,
                    post_to_transfer_file=False,
                    error_message=f"Period {txn_period}/{txn_year} not found in calendar for {ledger_type}",
                    current_year=current_year,
                    current_period=current_period,
                    transaction_year=txn_year,
                    transaction_period=txn_period
                )
            if sub_status != 0:
                status_desc = "closed" if sub_status == 2 else "blocked"
                return Opera3PeriodPostingDecision(
                    can_post=False,
                    post_to_nominal=False,
                    post_to_transfer_file=False,
                    error_message=f"Period {txn_period}/{txn_year} is {status_desc} for {ledger_type}",
                    current_year=current_year,
                    current_period=current_period,
                    transaction_year=txn_year,
                    transaction_period=txn_period
                )

        # Both NL and sub-ledger are open — continue to period check
    else:
        # OPA OFF: Only current period allowed
        if txn_year != current_year or txn_period != current_period:
            return Opera3PeriodPostingDecision(
                can_post=False,
                post_to_nominal=False,
                post_to_transfer_file=False,
                error_message=f"Period {txn_period}/{txn_year} is blocked. "
                             f"Current period is {current_period}/{current_year}. "
                             f"Open Period Accounting is disabled.",
                current_year=current_year,
                current_period=current_period,
                transaction_year=txn_year,
                transaction_period=txn_period
            )

    # Step 2: All required ledgers open — check period vs current
    # Period >= current: post to NL immediately
    if txn_year > current_year or (txn_year == current_year and txn_period >= current_period):
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

    # Period < current (backdated): transfer file only, manual transfer or period end needed
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
