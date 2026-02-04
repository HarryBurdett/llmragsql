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

    # Try sprfls first (Sales/Purchase Profiles)
    try:
        sprfls_query = """
            SELECT TOP 1
                RTRIM(ISNULL(sc_dbtctrl, '')) as debtors_control,
                RTRIM(ISNULL(pc_crdctrl, '')) as creditors_control
            FROM sprfls
        """
        df = sql_connector.execute_query(sprfls_query)

        if not df.empty:
            row = df.iloc[0]
            if row['debtors_control']:
                debtors_control = row['debtors_control']
                source = 'sprfls'
            if row['creditors_control']:
                creditors_control = row['creditors_control']
                source = 'sprfls'

        logger.debug(f"sprfls control accounts: debtors={debtors_control}, creditors={creditors_control}")
    except Exception as e:
        logger.warning(f"Could not read sprfls table: {e}")

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
