"""
Opera Configuration Utilities

Provides functions to read Opera system configuration including:
- Control account codes (debtors, creditors)
- System parameters

Control accounts vary by installation and are stored in:
- Primary: sprfls table (sc_dbtctrl for debtors, pc_crdctrl for creditors)
- Fallback: nparm table (np_dca for debtors, np_cca for creditors)
"""

import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OperaControlAccounts:
    """Opera control account configuration"""
    debtors_control: str  # Sales Ledger control account
    creditors_control: str  # Purchase Ledger control account
    source: str  # Where the values came from ('sprfls', 'nparm', 'default')


def get_control_accounts(sql_connector, use_cache: bool = True) -> OperaControlAccounts:
    """
    Get control account codes from Opera configuration tables.

    Reads from:
    1. sprfls table (sc_dbtctrl, pc_crdctrl) - primary source
    2. nparm table (np_dca, np_cca) - fallback if sprfls values are blank
    3. Default values if neither found

    Args:
        sql_connector: SQLConnector instance
        use_cache: Whether to use cached values (default True)

    Returns:
        OperaControlAccounts with debtors and creditors control account codes
    """
    # Check cache
    if use_cache and hasattr(get_control_accounts, '_cache'):
        return get_control_accounts._cache

    debtors_control = None
    creditors_control = None
    source = 'default'

    # Try sprfls (Sales Profiles) for debtors control
    try:
        sprfls_query = """
            SELECT TOP 1 RTRIM(ISNULL(sc_dbtctrl, '')) as debtors_control
            FROM sprfls
        """
        df = sql_connector.execute_query(sprfls_query)

        if not df.empty and df.iloc[0]['debtors_control']:
            debtors_control = df.iloc[0]['debtors_control']
            source = 'sprfls'
            logger.debug(f"sprfls debtors control: {debtors_control}")
    except Exception as e:
        logger.warning(f"Could not read sprfls table: {e}")

    # Try pprfls (Purchase Profiles) for creditors control
    try:
        pprfls_query = """
            SELECT TOP 1 RTRIM(ISNULL(pc_crdctrl, '')) as creditors_control
            FROM pprfls
        """
        df = sql_connector.execute_query(pprfls_query)

        if not df.empty and df.iloc[0]['creditors_control']:
            creditors_control = df.iloc[0]['creditors_control']
            if source == 'default':
                source = 'pprfls'
            logger.debug(f"pprfls creditors control: {creditors_control}")
    except Exception as e:
        logger.warning(f"Could not read pprfls table: {e}")

    # Try nparm as fallback
    if not debtors_control or not creditors_control:
        try:
            nparm_query = """
                SELECT TOP 1
                    RTRIM(ISNULL(np_dca, '')) as debtors_control,
                    RTRIM(ISNULL(np_cca, '')) as creditors_control
                FROM nparm
            """
            df = sql_connector.execute_query(nparm_query)

            if not df.empty:
                row = df.iloc[0]
                if not debtors_control and row['debtors_control']:
                    debtors_control = row['debtors_control']
                    if source == 'default':
                        source = 'nparm'
                if not creditors_control and row['creditors_control']:
                    creditors_control = row['creditors_control']
                    if source == 'default':
                        source = 'nparm'

            logger.debug(f"nparm control accounts: debtors={debtors_control}, creditors={creditors_control}")
        except Exception as e:
            logger.warning(f"Could not read nparm table: {e}")

    # Use defaults if still not found
    if not debtors_control:
        debtors_control = 'BB020'  # Common default
        logger.warning(f"Using default debtors control account: {debtors_control}")
    if not creditors_control:
        creditors_control = 'CA030'  # Common default
        logger.warning(f"Using default creditors control account: {creditors_control}")

    result = OperaControlAccounts(
        debtors_control=debtors_control,
        creditors_control=creditors_control,
        source=source
    )

    # Cache result
    get_control_accounts._cache = result

    logger.info(f"Control accounts from {source}: debtors={debtors_control}, creditors={creditors_control}")
    return result


def clear_control_accounts_cache():
    """Clear the cached control accounts (use when switching companies)"""
    if hasattr(get_control_accounts, '_cache'):
        delattr(get_control_accounts, '_cache')


def get_supplier_control_account(sql_connector, supplier_account: str) -> str:
    """
    Get the creditors control account for a specific supplier.

    Looks up the supplier's profile (pn_sprfl) and gets the control account
    from the profile (pc_crdctrl). If blank or not found, returns company default.

    Args:
        sql_connector: SQLConnector instance
        supplier_account: Supplier account code (e.g., 'S001')

    Returns:
        Creditors control account code
    """
    try:
        # Get supplier's profile and its control account
        query = f"""
            SELECT
                RTRIM(ISNULL(p.pn_sprfl, '')) as profile_code,
                RTRIM(ISNULL(pp.pc_crdctrl, '')) as control_account
            FROM pname p
            LEFT JOIN pprfls pp ON RTRIM(p.pn_sprfl) = RTRIM(pp.pc_code)
            WHERE RTRIM(p.pn_account) = '{supplier_account}'
        """
        df = sql_connector.execute_query(query)

        if not df.empty:
            control = df.iloc[0]['control_account']
            if control:
                logger.debug(f"Supplier {supplier_account} has control account {control} from profile")
                return control
            else:
                profile = df.iloc[0]['profile_code']
                logger.debug(f"Supplier {supplier_account} profile '{profile}' has no control account, using default")

    except Exception as e:
        logger.warning(f"Could not get profile control for supplier {supplier_account}: {e}")

    # Fall back to company default
    defaults = get_control_accounts(sql_connector)
    logger.debug(f"Using default creditors control {defaults.creditors_control} for supplier {supplier_account}")
    return defaults.creditors_control


def get_customer_control_account(sql_connector, customer_account: str) -> str:
    """
    Get the debtors control account for a specific customer.

    Looks up the customer's profile (sn_cprfl) and gets the control account
    from the profile (sc_dbtctrl). If blank or not found, returns company default.

    Args:
        sql_connector: SQLConnector instance
        customer_account: Customer account code (e.g., 'C001')

    Returns:
        Debtors control account code
    """
    try:
        # Get customer's profile and its control account
        query = f"""
            SELECT
                RTRIM(ISNULL(s.sn_cprfl, '')) as profile_code,
                RTRIM(ISNULL(sp.sc_dbtctrl, '')) as control_account
            FROM sname s
            LEFT JOIN sprfls sp ON RTRIM(s.sn_cprfl) = RTRIM(sp.sc_code)
            WHERE RTRIM(s.sn_account) = '{customer_account}'
        """
        df = sql_connector.execute_query(query)

        if not df.empty:
            control = df.iloc[0]['control_account']
            if control:
                logger.debug(f"Customer {customer_account} has control account {control} from profile")
                return control
            else:
                profile = df.iloc[0]['profile_code']
                logger.debug(f"Customer {customer_account} profile '{profile}' has no control account, using default")

    except Exception as e:
        logger.warning(f"Could not get profile control for customer {customer_account}: {e}")

    # Fall back to company default
    defaults = get_control_accounts(sql_connector)
    logger.debug(f"Using default debtors control {defaults.debtors_control} for customer {customer_account}")
    return defaults.debtors_control


def get_bank_account_nominal(sql_connector, bank_code: str) -> Optional[str]:
    """
    Get the nominal account code for a bank account.

    Args:
        sql_connector: SQLConnector instance
        bank_code: Bank account code (e.g., 'BC010')

    Returns:
        Nominal account code or None if not found
    """
    try:
        query = f"""
            SELECT RTRIM(nk_nlcode) as nominal_code
            FROM nbank
            WHERE RTRIM(nk_acnt) = '{bank_code}'
        """
        df = sql_connector.execute_query(query)
        if not df.empty:
            return df.iloc[0]['nominal_code']
    except Exception as e:
        logger.warning(f"Could not get nominal code for bank {bank_code}: {e}")

    return None


def get_next_reference_number(sql_connector, ref_type: str) -> int:
    """
    Get the next reference number from Opera's sequence tables.

    Args:
        sql_connector: SQLConnector instance
        ref_type: Type of reference ('receipt', 'payment', 'journal', etc.)

    Returns:
        Next available reference number
    """
    # Reference types map to different sequence fields
    ref_map = {
        'receipt': ('sysparm', 'sp_rcptno'),
        'payment': ('sysparm', 'sp_paymno'),
        'journal': ('sysparm', 'sp_jrnlno'),
        'invoice': ('sysparm', 'sp_invno'),
    }

    if ref_type not in ref_map:
        raise ValueError(f"Unknown reference type: {ref_type}")

    table, field = ref_map[ref_type]

    try:
        query = f"SELECT {field} as next_ref FROM {table}"
        df = sql_connector.execute_query(query)
        if not df.empty:
            return int(df.iloc[0]['next_ref'])
    except Exception as e:
        logger.warning(f"Could not get next {ref_type} reference: {e}")

    return 1


def get_current_financial_year(sql_connector) -> Optional[int]:
    """
    Get the current financial year from Opera system parameters.

    Returns:
        Current financial year number or None
    """
    try:
        query = "SELECT TOP 1 sp_year as current_year FROM sysparm"
        df = sql_connector.execute_query(query)
        if not df.empty:
            return int(df.iloc[0]['current_year'])
    except Exception as e:
        logger.warning(f"Could not get current financial year: {e}")

    return None


def get_opera_system_info(sql_connector) -> Dict[str, Any]:
    """
    Get general Opera system information.

    Returns:
        Dictionary with system info (version, company name, etc.)
    """
    info = {}

    # Get control accounts
    try:
        control = get_control_accounts(sql_connector)
        info['debtors_control'] = control.debtors_control
        info['creditors_control'] = control.creditors_control
        info['control_source'] = control.source
    except Exception as e:
        logger.warning(f"Could not get control accounts: {e}")

    # Get financial year
    try:
        year = get_current_financial_year(sql_connector)
        if year:
            info['financial_year'] = year
    except Exception:
        pass

    # Get company info from seqco if available
    try:
        query = "SELECT TOP 1 co_name as company_name FROM seqco"
        df = sql_connector.execute_query(query)
        if not df.empty:
            info['company_name'] = df.iloc[0]['company_name'].strip()
    except Exception:
        pass

    return info


# =============================================================================
# PERIOD VALIDATION FUNCTIONS
# =============================================================================

@dataclass
class PeriodValidationResult:
    """Result of period validation"""
    is_valid: bool
    error_message: Optional[str] = None
    year: Optional[int] = None
    period: Optional[int] = None
    open_period_accounting: bool = False


def is_open_period_accounting_enabled(sql_connector) -> bool:
    """
    Check if Open Period Accounting is enabled.

    Tries multiple sources:
    1. opera3sesystem.co_opanl (Opera 3 system table)
    2. nparm.np_opawarn (SQL SE nominal parameters)

    Args:
        sql_connector: SQLConnector instance

    Returns:
        True if Open Period Accounting is enabled, False otherwise
    """
    # Try opera3sesystem table first (Opera 3 style)
    try:
        query = """
            SELECT TOP 1 RTRIM(ISNULL(co_opanl, '')) as co_opanl
            FROM opera3sesystem
        """
        df = sql_connector.execute_query(query)
        if not df.empty:
            value = df.iloc[0]['co_opanl'].upper()
            enabled = value == 'Y'
            logger.debug(f"Open Period Accounting enabled: {enabled} (opera3sesystem.co_opanl='{value}')")
            return enabled
    except Exception as e:
        logger.debug(f"opera3sesystem table not found, trying nparm: {e}")

    # Try nparm.np_opawarn (SQL SE style)
    try:
        query = """
            SELECT TOP 1 np_opawarn
            FROM nparm
        """
        df = sql_connector.execute_query(query)
        if not df.empty:
            value = df.iloc[0]['np_opawarn']
            # np_opawarn is a bit field - True means OPA is enabled
            enabled = bool(value)
            logger.debug(f"Open Period Accounting enabled: {enabled} (nparm.np_opawarn={value})")
            return enabled
    except Exception as e:
        logger.warning(f"Could not read np_opawarn from nparm: {e}")

    # Default to disabled (stricter mode) if we can't read the setting
    logger.warning("Could not determine Open Period Accounting setting, defaulting to disabled")
    return False


def get_current_period_info(sql_connector) -> Dict[str, Any]:
    """
    Get current period information from nparm.

    Returns:
        Dictionary with np_year, np_perno, np_periods
    """
    try:
        query = """
            SELECT TOP 1
                np_year,
                np_perno,
                np_periods
            FROM nparm
        """
        df = sql_connector.execute_query(query)
        if not df.empty:
            row = df.iloc[0]
            return {
                'np_year': int(row['np_year']) if row['np_year'] else None,
                'np_perno': int(row['np_perno']) if row['np_perno'] else None,
                'np_periods': int(row['np_periods']) if row['np_periods'] else 12
            }
    except Exception as e:
        logger.warning(f"Could not read current period from nparm: {e}")

    return {'np_year': None, 'np_perno': None, 'np_periods': 12}


def get_period_status(sql_connector, year: int, period: int, ledger_type: str) -> Optional[int]:
    """
    Get the period status for a specific ledger from nclndd.

    Args:
        sql_connector: SQLConnector instance
        year: Financial year
        period: Period number (1-12)
        ledger_type: One of 'NL', 'SL', 'PL', 'ST', 'WG', 'FA'

    Returns:
        Status value (0=Open, 1=Current, 2=Closed) or None if not found
    """
    status_field_map = {
        'NL': 'ncd_nlstat',
        'SL': 'ncd_slstat',
        'PL': 'ncd_plstat',
        'ST': 'ncd_ststat',
        'WG': 'ncd_wgstat',
        'FA': 'ncd_fastat'
    }

    if ledger_type not in status_field_map:
        raise ValueError(f"Invalid ledger_type: {ledger_type}. Must be one of {list(status_field_map.keys())}")

    status_field = status_field_map[ledger_type]

    try:
        query = f"""
            SELECT {status_field} as period_status
            FROM nclndd
            WHERE ncd_year = {year} AND ncd_period = {period}
        """
        df = sql_connector.execute_query(query)
        if not df.empty:
            status = int(df.iloc[0]['period_status'])
            logger.debug(f"Period {period}/{year} {ledger_type} status: {status}")
            return status
    except Exception as e:
        logger.warning(f"Could not read period status from nclndd: {e}")

    return None


def validate_posting_period(
    sql_connector,
    post_date,
    ledger_type: str = 'NL'
) -> PeriodValidationResult:
    """
    Validate that a transaction can be posted to the target period.

    This implements Opera's period control logic:
    - If Open Period Accounting is OFF: Only current period is allowed
    - If Open Period Accounting is ON: Check nclndd for per-ledger status

    Args:
        sql_connector: SQLConnector instance
        post_date: Date of transaction (date object or string 'YYYY-MM-DD')
        ledger_type: Ledger type - 'NL' (Nominal), 'SL' (Sales), 'PL' (Purchase),
                     'ST' (Stock), 'WG' (Wages), 'FA' (Fixed Assets)

    Returns:
        PeriodValidationResult with is_valid, error_message, and period info
    """
    from datetime import date, datetime

    # Parse post_date if string
    if isinstance(post_date, str):
        post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

    year = post_date.year
    period = post_date.month

    # Check if Open Period Accounting is enabled
    open_period_enabled = is_open_period_accounting_enabled(sql_connector)

    if not open_period_enabled:
        # Stricter mode: Only current period allowed
        current = get_current_period_info(sql_connector)

        if current['np_year'] is None or current['np_perno'] is None:
            logger.warning("Could not determine current period - allowing post")
            return PeriodValidationResult(
                is_valid=True,
                year=year,
                period=period,
                open_period_accounting=False
            )

        if year != current['np_year'] or period != current['np_perno']:
            return PeriodValidationResult(
                is_valid=False,
                error_message=f"Period {period}/{year} is blocked. "
                              f"Current period is {current['np_perno']}/{current['np_year']}.",
                year=year,
                period=period,
                open_period_accounting=False
            )

        return PeriodValidationResult(
            is_valid=True,
            year=year,
            period=period,
            open_period_accounting=False
        )

    else:
        # Open Period Accounting enabled: Check nclndd for ledger-specific status
        status = get_period_status(sql_connector, year, period, ledger_type)

        if status is None:
            return PeriodValidationResult(
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
            return PeriodValidationResult(
                is_valid=False,
                error_message=f"{ledger_name} is closed for period {period}/{year}",
                year=year,
                period=period,
                open_period_accounting=True
            )

        # Status 0 (Open) or 1 (Current) - allow posting
        return PeriodValidationResult(
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
        'sales_refund': 'SL',
        'purchase_payment': 'PL',
        'purchase_invoice': 'PL',
        'purchase_credit': 'PL',
        'purchase_refund': 'PL',
        'nominal_journal': 'NL',
        'bank_receipt': 'NL',
        'bank_payment': 'NL',
        'repeat_entry': 'NL',
        'stock_adjustment': 'ST',
        'payroll': 'WG',
        'fixed_asset': 'FA'
    }
    return ledger_map.get(transaction_type.lower(), 'NL')


# =============================================================================
# PERIOD POSTING RULES
# =============================================================================

@dataclass
class PeriodPostingDecision:
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


def get_period_posting_decision(sql_connector, post_date) -> PeriodPostingDecision:
    """
    Determine how a transaction should be posted based on period rules.

    Rules:
    1. Transaction NOT in current year -> REJECT (don't post to any ledger)
    2. Transaction in current nominal period -> Post to ntran + transfer file with done='Y'
    3. Transaction in current year but different period -> Transfer file only with done=' '

    Args:
        sql_connector: SQLConnector instance
        post_date: Transaction date (date object or 'YYYY-MM-DD' string)

    Returns:
        PeriodPostingDecision with posting instructions
    """
    from datetime import date, datetime

    # Parse post_date if string
    if isinstance(post_date, str):
        post_date = datetime.strptime(post_date, '%Y-%m-%d').date()

    txn_year = post_date.year
    txn_period = post_date.month

    # Get current period info from nparm
    current_info = get_current_period_info(sql_connector)
    current_year = current_info.get('np_year')
    current_period = current_info.get('np_perno')

    if current_year is None or current_period is None:
        logger.warning("Could not determine current period - defaulting to allow posting")
        return PeriodPostingDecision(
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
        return PeriodPostingDecision(
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
        return PeriodPostingDecision(
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
    return PeriodPostingDecision(
        can_post=True,
        post_to_nominal=False,
        post_to_transfer_file=True,
        transfer_file_done_flag=' ',
        current_year=current_year,
        current_period=current_period,
        transaction_year=txn_year,
        transaction_period=txn_period
    )
