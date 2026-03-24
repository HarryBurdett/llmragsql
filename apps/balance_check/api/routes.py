"""
Balance Check API routes.

Extracted from api/main.py — provides endpoints for balance check / reconciliation
queries for both Opera SQL SE and Opera 3 (FoxPro). These are READ-ONLY endpoints
that check internal Opera balances agree (debtors control vs NL, creditors control
vs NL, cashbook vs nbank, VAT, trial balance).

Does NOT include bank reconciliation endpoints (those belong to bank_reconcile app).
"""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# SQL SE Balance Check Endpoints
# ============================================================

# Reconciliation Endpoints
# ============================================================

def _get_control_accounts_for_reconciliation():
    """Get control accounts from Opera config for reconciliation"""
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection — cannot determine control accounts")
    try:
        from sql_rag.opera_config import get_control_accounts
        control = get_control_accounts(sql_connector)
        return {
            'debtors': control.debtors_control,
            'creditors': control.creditors_control
        }
    except Exception as e:
        logger.error(f"Could not load control accounts from config: {e}")
        raise HTTPException(status_code=500, detail=f"Could not determine control accounts for this company: {e}")

@router.get("/api/reconcile/creditors")
async def reconcile_creditors():
    """
    Reconcile Purchase Ledger (ptran) to Creditors Control Account (Nominal Ledger).
    Compares outstanding balances in ptran with the control account in nacnt/ntran.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get dynamic control accounts from config
        control_accounts = _get_control_accounts_for_reconciliation()
        creditors_control = control_accounts['creditors']

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "purchase_ledger": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": [],
            "control_account_used": creditors_control
        }

        # ========== PURCHASE LEDGER (ptran) ==========
        # Get outstanding transactions from purchase ledger
        # Only include transactions for suppliers that still exist in pname (exclude deleted suppliers)
        pl_outstanding_sql = """
            SELECT
                COUNT(*) AS transaction_count,
                SUM(pt_trbal) AS total_outstanding
            FROM ptran WITH (NOLOCK)
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
        """
        pl_result = sql_connector.execute_query(pl_outstanding_sql)
        if hasattr(pl_result, 'to_dict'):
            pl_result = pl_result.to_dict('records')

        pl_total = float(pl_result[0]['total_outstanding'] or 0) if pl_result else 0
        pl_count = int(pl_result[0]['transaction_count'] or 0) if pl_result else 0

        # Get breakdown by transaction type (only active suppliers)
        pl_breakdown_sql = """
            SELECT
                pt_trtype AS type,
                COUNT(*) AS count,
                SUM(pt_trbal) AS total
            FROM ptran WITH (NOLOCK)
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
            GROUP BY pt_trtype
            ORDER BY pt_trtype
        """
        pl_breakdown = sql_connector.execute_query(pl_breakdown_sql)
        if hasattr(pl_breakdown, 'to_dict'):
            pl_breakdown = pl_breakdown.to_dict('records')

        type_names = {'I': 'Invoices', 'C': 'Credit Notes', 'P': 'Payments', 'B': 'Brought Forward'}
        pl_by_type = []
        for row in pl_breakdown or []:
            pl_by_type.append({
                "type": row['type'].strip() if row['type'] else 'Unknown',
                "description": type_names.get(row['type'].strip(), row['type'].strip()) if row['type'] else 'Unknown',
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        # Verify against supplier master (pname)
        pname_sql = """
            SELECT
                COUNT(*) AS supplier_count,
                SUM(pn_currbal) AS total_balance
            FROM pname WITH (NOLOCK)
            WHERE pn_currbal <> 0
        """
        pname_result = sql_connector.execute_query(pname_sql)
        if hasattr(pname_result, 'to_dict'):
            pname_result = pname_result.to_dict('records')

        pname_total = float(pname_result[0]['total_balance'] or 0) if pname_result else 0
        pname_count = int(pname_result[0]['supplier_count'] or 0) if pname_result else 0

        # Detailed verification: Find suppliers where pn_currbal doesn't match SUM(pt_trbal)
        balance_mismatch_sql = """
            SELECT
                p.pn_account AS account,
                RTRIM(p.pn_name) AS name,
                p.pn_currbal AS master_balance,
                COALESCE(t.txn_balance, 0) AS transaction_balance,
                p.pn_currbal - COALESCE(t.txn_balance, 0) AS variance
            FROM pname p WITH (NOLOCK)
            LEFT JOIN (
                SELECT pt_account, SUM(pt_trbal) AS txn_balance
                FROM ptran WITH (NOLOCK)
                GROUP BY pt_account
            ) t ON RTRIM(p.pn_account) = RTRIM(t.pt_account)
            WHERE ABS(p.pn_currbal - COALESCE(t.txn_balance, 0)) >= 0.01
            ORDER BY ABS(p.pn_currbal - COALESCE(t.txn_balance, 0)) DESC
        """
        try:
            balance_mismatches = sql_connector.execute_query(balance_mismatch_sql)
            if hasattr(balance_mismatches, 'to_dict'):
                balance_mismatches = balance_mismatches.to_dict('records')
        except Exception:
            balance_mismatches = []

        supplier_balance_issues = []
        for row in balance_mismatches or []:
            supplier_balance_issues.append({
                "account": row['account'].strip() if row['account'] else '',
                "name": row['name'] or '',
                "master_balance": round(float(row['master_balance'] or 0), 2),
                "transaction_balance": round(float(row['transaction_balance'] or 0), 2),
                "variance": round(float(row['variance'] or 0), 2)
            })

        # ========== TRANSFER FILE (pnoml) ==========
        # Check for transactions sitting in the transfer file waiting to post to NL
        # px_done = 'Y' means posted, anything else means pending
        pnoml_pending_sql = """
            SELECT
                px_nacnt AS nominal_account,
                px_type AS type,
                px_date AS date,
                px_value AS value,
                px_tref AS reference,
                px_comment AS comment,
                px_done AS status
            FROM pnoml WITH (NOLOCK)
            WHERE px_done <> 'Y' OR px_done IS NULL
            ORDER BY px_date DESC
        """
        try:
            pnoml_pending = sql_connector.execute_query(pnoml_pending_sql)
            if hasattr(pnoml_pending, 'to_dict'):
                pnoml_pending = pnoml_pending.to_dict('records')
        except Exception:
            pnoml_pending = []

        # Count posted vs pending in transfer file
        pnoml_summary_sql = """
            SELECT
                CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(px_value) AS total
            FROM pnoml WITH (NOLOCK)
            GROUP BY CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            pnoml_summary = sql_connector.execute_query(pnoml_summary_sql)
            if hasattr(pnoml_summary, 'to_dict'):
                pnoml_summary = pnoml_summary.to_dict('records')
        except Exception:
            pnoml_summary = []

        posted_count = 0
        posted_total = 0
        pending_count = 0
        pending_total = 0
        for row in pnoml_summary or []:
            if row['status'] == 'Posted':
                posted_count = int(row['count'] or 0)
                posted_total = float(row['total'] or 0)
            else:
                pending_count = int(row['count'] or 0)
                pending_total = float(row['total'] or 0)

        # Build pending transactions list from transfer file
        pending_transactions = []
        for row in pnoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "type": row['type'].strip() if row['type'] else '',
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["purchase_ledger"] = {
            "source": "ptran (Purchase Ledger Transactions)",
            "total_outstanding": round(pl_total, 2),
            "transaction_count": pl_count,
            "breakdown_by_type": pl_by_type,
            "transfer_file": {
                "source": "pnoml (Purchase to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": posted_count,
                    "total": round(posted_total, 2)
                },
                "pending_transfer": {
                    "count": pending_count,
                    "total": round(pending_total, 2),
                    "transactions": pending_transactions
                }
            },
            "supplier_master_check": {
                "source": "pname (Supplier Master)",
                "total": round(pname_total, 2),
                "supplier_count": pname_count,
                "matches_ptran": abs(pl_total - pname_total) < 0.01,
                "balance_variance": round(pl_total - pname_total, 2),
                "suppliers_with_balance_issues": supplier_balance_issues[:20] if supplier_balance_issues else [],
                "total_suppliers_with_issues": len(supplier_balance_issues)
            }
        }

        # ========== NOMINAL LEDGER (nacnt/ntran) ==========
        # Find Creditors Control account - check various naming conventions
        # Include the dynamically loaded control account from config
        control_account_sql = f"""
            SELECT na_acnt, na_desc, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt WITH (NOLOCK)
            WHERE na_desc LIKE '%Creditor%Control%'
               OR na_desc LIKE '%Trade%Creditor%'
               OR na_acnt = '{creditors_control}'
            ORDER BY na_acnt
        """
        control_result = sql_connector.execute_query(control_account_sql)
        if hasattr(control_result, 'to_dict'):
            control_result = control_result.to_dict('records')

        nl_total = 0
        nl_details = []

        # Get current year from ntran
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        if control_result:
            for acc in control_result:
                acnt = acc['na_acnt'].strip()

                # Get YTD figures from nacnt (these are current year movements)
                ytd_dr = float(acc['na_ytddr'] or 0)
                ytd_cr = float(acc['na_ytdcr'] or 0)
                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                # Prior year B/F (credit balance for creditors)
                bf_balance = pry_cr - pry_dr

                # Get current year transactions from ntran
                ntran_current_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_current = sql_connector.execute_query(ntran_current_sql)
                if hasattr(ntran_current, 'to_dict'):
                    ntran_current = ntran_current.to_dict('records')

                current_year_dr = float(ntran_current[0]['debits'] or 0) if ntran_current else 0
                current_year_cr = float(ntran_current[0]['credits'] or 0) if ntran_current else 0
                current_year_net = float(ntran_current[0]['net'] or 0) if ntran_current else 0

                # For creditors control, NEGATE the ntran value for comparison with ptran
                # Sign conventions are OPPOSITE:
                # - ptran: positive = we owe suppliers, negative = they owe us
                # - ntran: negative = we owe suppliers (credit), positive = they owe us (debit)
                # So we negate ntran to match ptran convention
                current_year_balance = -current_year_net

                # Get all years for reference
                ntran_sql = f"""
                    SELECT
                        nt_year,
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}'
                    GROUP BY nt_year
                    ORDER BY nt_year
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                ntran_by_year = []
                for row in ntran_result or []:
                    ntran_by_year.append({
                        "year": int(row['nt_year']),
                        "debits": round(float(row['debits'] or 0), 2),
                        "credits": round(float(row['credits'] or 0), 2),
                        "net": round(float(row['net'] or 0), 2)
                    })

                nl_details.append({
                    "account": acnt,
                    "description": acc['na_desc'].strip() if acc['na_desc'] else '',
                    "brought_forward": round(bf_balance, 2),
                    "current_year": current_year,
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(current_year_balance, 2),
                    "ntran_by_year": ntran_by_year
                })

                # Use current year balance for reconciliation
                nl_total += current_year_balance

        reconciliation["nominal_ledger"] = {
            "source": f"ntran (Nominal Ledger - {current_year} only)",
            "control_accounts": nl_details,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ========== VARIANCE CALCULATION ==========
        # The total PL outstanding should match the NL control account balance
        # Pending transfers are transactions in the PL that haven't been posted to NL yet
        # but the PL total and NL should still reconcile as the balance is calculated differently

        # Primary variance: Total PL vs NL (this is what matters for reconciliation)
        variance = pl_total - nl_total
        variance_abs = abs(variance)

        # Secondary check: Posted-only variance (to verify NL posting integrity)
        variance_posted = posted_total - nl_total
        variance_posted_abs = abs(variance_posted)

        reconciliation["variance"] = {
            "amount": round(variance, 2),
            "absolute": round(variance_abs, 2),
            "purchase_ledger_total": round(pl_total, 2),
            "transfer_file_posted": round(posted_total, 2),
            "transfer_file_pending": round(pending_total, 2),
            "nominal_ledger_total": round(nl_total, 2),
            "posted_variance": round(variance_posted, 2),
            "posted_variance_abs": round(variance_posted_abs, 2),
            "reconciled": variance_abs < 1.00,
            "has_pending_transfers": pending_count > 0
        }

        # Determine status based on total PL vs NL
        if variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            if pending_count > 0:
                reconciliation["message"] = f"Purchase Ledger reconciles to Nominal Ledger. {pending_count} transactions (£{abs(pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = "Purchase Ledger reconciles to Nominal Ledger Creditors Control"
        else:
            reconciliation["status"] = "UNRECONCILED"
            if variance > 0:
                reconciliation["message"] = f"Purchase Ledger is £{variance_abs:,.2f} MORE than Nominal Ledger Control"
            else:
                reconciliation["message"] = f"Purchase Ledger is £{variance_abs:,.2f} LESS than Nominal Ledger Control"

        # ========== VARIANCE ANALYSIS ==========
        # If there's a variance, try to identify the cause by comparing NL and PL
        variance_items = []
        nl_only_items = []
        pl_only_items = []
        value_diff_items = []
        nl_total_check = 0
        pl_total_check = 0

        if variance_abs >= 0.01:
            # Get creditors control account code(s), fallback to config value
            control_accounts = [acc['account'] for acc in nl_details] if nl_details else [creditors_control]
            control_accounts_str = "','".join(control_accounts)

            # Get NL transactions for current year only
            # Match key is: date + value + reference (composite key)
            # Note: nt_entr is the entry date, nt_cmnt contains the PL reference
            nl_transactions_sql = f"""
                SELECT
                    RTRIM(nt_cmnt) AS reference,
                    nt_value AS nl_value,
                    nt_entr AS date,
                    nt_year AS year,
                    nt_type AS type,
                    RTRIM(nt_ref) AS nl_ref,
                    RTRIM(nt_trnref) AS description
                FROM ntran
                WHERE nt_acnt IN ('{control_accounts_str}')
                  AND nt_year = {current_year}
                ORDER BY nt_entr, nt_cmnt
            """
            try:
                nl_trans = sql_connector.execute_query(nl_transactions_sql)
                if hasattr(nl_trans, 'to_dict'):
                    nl_trans = nl_trans.to_dict('records')
            except Exception as e:
                logger.error(f"NL transactions query failed: {e}")
                nl_trans = []

            # Get supplier names from pname for matching
            # Build lookup: supplier_name -> account and account -> name
            supplier_names_sql = """
                SELECT RTRIM(pn_account) AS account, RTRIM(pn_name) AS name
                FROM pname
            """
            try:
                supplier_result = sql_connector.execute_query(supplier_names_sql)
                if hasattr(supplier_result, 'to_dict'):
                    supplier_result = supplier_result.to_dict('records')
                # Build lookups for matching
                supplier_name_to_account = {}
                supplier_account_to_name = {}
                for row in supplier_result or []:
                    acc = row['account'].strip() if row['account'] else ''
                    name = row['name'].strip().upper() if row['name'] else ''
                    if acc and name:
                        supplier_name_to_account[name] = acc
                        supplier_account_to_name[acc] = name
                active_suppliers = set(supplier_name_to_account.values())
            except Exception as e:
                logger.error(f"Supplier names query failed: {e}")
                supplier_name_to_account = {}
                supplier_account_to_name = {}
                active_suppliers = set()

            # Get PL transactions for current year only, for active suppliers
            pl_transactions_sql = f"""
                SELECT
                    RTRIM(pt_trref) AS reference,
                    pt_trbal AS pl_balance,
                    pt_trvalue AS pl_value,
                    pt_trdate AS date,
                    RTRIM(pt_account) AS supplier,
                    pt_trtype AS type,
                    RTRIM(pt_supref) AS supplier_ref
                FROM ptran
                WHERE pt_trbal <> 0
                  AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname)
                  AND YEAR(pt_trdate) = {current_year}
                ORDER BY pt_trdate, pt_trref
            """
            try:
                pl_trans = sql_connector.execute_query(pl_transactions_sql)
                if hasattr(pl_trans, 'to_dict'):
                    pl_trans = pl_trans.to_dict('records')
            except Exception as e:
                logger.error(f"PL transactions query failed: {e}")
                pl_trans = []

            # Store NL transactions as a list to handle duplicates properly
            # Each entry should be matchable individually, not aggregated
            nl_entries = []

            for txn in nl_trans or []:
                ref = txn['reference'].strip() if txn['reference'] else ''
                nl_value = float(txn['nl_value'] or 0)
                nl_date = txn['date']
                if hasattr(nl_date, 'strftime'):
                    nl_date_str = nl_date.strftime('%Y-%m-%d')
                else:
                    nl_date_str = str(nl_date) if nl_date else ''

                # Extract supplier name from nt_trnref (first 30 chars contain supplier name)
                description = txn['description'].strip() if txn['description'] else ''
                nl_supplier_name = description[:30].strip().upper() if description else ''

                # Try to find the supplier account from the name
                nl_supplier_account = supplier_name_to_account.get(nl_supplier_name, '')

                # If no exact match, try partial match (supplier name might be truncated)
                if not nl_supplier_account and nl_supplier_name:
                    for name, acc in supplier_name_to_account.items():
                        if name.startswith(nl_supplier_name) or nl_supplier_name.startswith(name):
                            nl_supplier_account = acc
                            break

                abs_val = abs(nl_value)

                # Keys for matching
                date_val_key = f"{nl_date_str}|{abs_val:.2f}"
                # Include supplier in the key for more accurate matching
                date_val_supplier_key = f"{nl_date_str}|{abs_val:.2f}|{nl_supplier_account}"

                nl_entry = {
                    'value': nl_value,
                    'date': nl_date_str,
                    'reference': ref,
                    'year': txn['year'],
                    'type': txn['type'],
                    'matched': False,
                    'date_val_key': date_val_key,
                    'date_val_supplier_key': date_val_supplier_key,
                    'abs_val': abs_val,
                    'supplier_name': nl_supplier_name,
                    'supplier_account': nl_supplier_account
                }

                nl_entries.append(nl_entry)
                nl_total_check += nl_value

            # Build PL lookup and try to match against NL
            pl_entries = []
            matched_pl_indices = set()

            for txn in pl_trans or []:
                ref = txn['reference'].strip() if txn['reference'] else ''
                pl_bal = float(txn['pl_balance'] or 0)
                pl_value = float(txn['pl_value'] or 0)
                supplier = txn['supplier'].strip() if txn['supplier'] else ''
                tr_type = txn['type'].strip() if txn['type'] else ''
                sup_ref = txn['supplier_ref'].strip() if txn['supplier_ref'] else ''
                pl_date = txn['date']

                if hasattr(pl_date, 'strftime'):
                    pl_date_str = pl_date.strftime('%Y-%m-%d')
                else:
                    pl_date_str = str(pl_date) if pl_date else ''

                abs_val = abs(pl_value)
                date_val_key = f"{pl_date_str}|{abs_val:.2f}"
                # Include supplier account in key for precise matching
                date_val_supplier_key = f"{pl_date_str}|{abs_val:.2f}|{supplier}"

                pl_entries.append({
                    'balance': pl_bal,
                    'value': pl_value,
                    'date': pl_date_str,
                    'reference': ref,
                    'supplier': supplier,
                    'type': tr_type,
                    'supplier_ref': sup_ref,
                    'date_val_key': date_val_key,
                    'date_val_supplier_key': date_val_supplier_key,
                    'abs_val': abs_val,
                    'matched': False
                })

                pl_total_check += pl_bal

            # Build NL reference lookup for fast matching
            # nt_cmnt (stored as 'reference') should match pt_trref directly
            # Use list to handle multiple entries with same reference
            nl_by_reference = {}
            for nl_entry in nl_entries:
                ref = nl_entry['reference']
                if ref:
                    if ref not in nl_by_reference:
                        nl_by_reference[ref] = []
                    nl_by_reference[ref].append(nl_entry)

            # Match PL entries to NL entries
            # Priority: 1) Reference match, 2) Date+Value+Supplier, 3) Date+Value, 4) Value+Supplier
            for pl_idx, pl_entry in enumerate(pl_entries):
                pl_ref = pl_entry['reference']
                pl_date_val_supplier_key = pl_entry['date_val_supplier_key']
                pl_date_val_key = pl_entry['date_val_key']
                pl_abs = pl_entry['abs_val']
                pl_supplier = pl_entry['supplier']

                nl_data = None
                match_type = None

                # Strategy 1: Match by reference (nt_cmnt = pt_trref) - MOST RELIABLE
                # For generic refs like 'pay', 'rec' - require exact value match to avoid false matches
                # For specific refs (invoice numbers) - allow small tolerance
                GENERIC_REFS = {'rec', 'pay', 'contra', 'refund', 'adjustment', 'adj', 'jnl', 'journal'}
                is_generic_ref = pl_ref.lower() in GENERIC_REFS if pl_ref else False

                if pl_ref and pl_ref in nl_by_reference:
                    for nl_entry in nl_by_reference[pl_ref]:
                        if not nl_entry['matched']:
                            value_diff = abs(nl_entry['abs_val'] - pl_abs)

                            # For generic refs, require exact match (within £0.10 for rounding)
                            # For specific refs, allow 10% or £10 tolerance
                            if is_generic_ref:
                                value_tolerance = 0.10  # Must be exact for generic refs
                            else:
                                value_tolerance = max(10.0, pl_abs * 0.1)

                            if value_diff <= value_tolerance:
                                nl_data = nl_entry
                                match_type = "reference"
                                break

                # Strategy 2: Match by date + value + supplier
                if not nl_data:
                    for nl_entry in nl_entries:
                        if not nl_entry['matched'] and nl_entry['date_val_supplier_key'] == pl_date_val_supplier_key:
                            nl_data = nl_entry
                            match_type = "date_value_supplier"
                            break

                # Strategy 3: Match by date + value only (supplier may not match exactly)
                if not nl_data:
                    for nl_entry in nl_entries:
                        if not nl_entry['matched'] and nl_entry['date_val_key'] == pl_date_val_key:
                            nl_data = nl_entry
                            match_type = "date_value"
                            break

                # Strategy 4: Match by value + supplier (dates may differ)
                if not nl_data:
                    for nl_entry in nl_entries:
                        if nl_entry['matched']:
                            continue
                        if abs(nl_entry['abs_val'] - pl_abs) < 0.02 and nl_entry['supplier_account'] == pl_supplier:
                            nl_data = nl_entry
                            match_type = "value_supplier"
                            break

                if nl_data:
                    nl_data['matched'] = True
                    pl_entry['matched'] = True
                    matched_pl_indices.add(pl_idx)

                    # Check for value differences
                    nl_abs = nl_data['abs_val']
                    actual_diff = round(nl_abs - pl_abs, 2)
                    if abs(actual_diff) >= 0.01:
                        value_diff_items.append({
                            "source": "Value Difference",
                            "date": pl_entry['date'],
                            "reference": pl_entry['reference'],
                            "supplier": pl_entry['supplier'],
                            "type": pl_entry['type'],
                            "value": actual_diff,
                            "nl_value": round(nl_data['value'], 2),
                            "pl_value": round(pl_entry['value'], 2),
                            "pl_balance": round(pl_entry['balance'], 2),
                            "match_type": match_type,
                            "note": f"NL: £{nl_abs:.2f} vs PL: £{pl_abs:.2f} (diff: £{abs(actual_diff):.2f})"
                        })

            # Find unmatched NL entries (in NL but not in PL)
            for nl_entry in nl_entries:
                if not nl_entry['matched'] and abs(nl_entry['value']) >= 0.01:
                    # Include extracted supplier info from nt_trnref
                    supplier_info = nl_entry['supplier_account'] or nl_entry['supplier_name'] or ''
                    note = f"In NL (year {nl_entry['year']}) but no matching PL entry"
                    if nl_entry['supplier_name'] and not nl_entry['supplier_account']:
                        note += f" - Supplier name '{nl_entry['supplier_name']}' not found in pname"
                    nl_only_items.append({
                        "source": "Nominal Ledger Only",
                        "date": nl_entry['date'],
                        "reference": nl_entry['reference'],
                        "supplier": supplier_info,
                        "type": nl_entry['type'] or "NL",
                        "value": round(nl_entry['value'], 2),
                        "note": note
                    })

            # Find unmatched PL entries (in PL but not in NL)
            for pl_idx, pl_entry in enumerate(pl_entries):
                if not pl_entry['matched'] and abs(pl_entry['balance']) >= 0.01:
                    pl_only_items.append({
                        "source": "Purchase Ledger Only",
                        "date": pl_entry['date'],
                        "reference": pl_entry['reference'],
                        "supplier": pl_entry['supplier'],
                        "type": pl_entry['type'],
                        "value": round(pl_entry['balance'], 2),
                        "note": "In PL but no matching NL entry"
                    })

            # Look for items that exactly match the variance or are small balances
            # These are most likely to explain the variance
            exact_match_refs = set()
            small_balance_refs = set()

            for txn in pl_trans or []:
                pl_bal = float(txn['pl_balance'] or 0)
                ref = txn['reference'].strip() if txn['reference'] else ''
                tr_date = txn['date']
                if hasattr(tr_date, 'strftime'):
                    tr_date = tr_date.strftime('%Y-%m-%d')

                # Check for exact match to variance
                if abs(abs(pl_bal) - variance_abs) < 0.02:
                    exact_match_refs.add(ref)
                    variance_items.append({
                        "source": "Exact Match",
                        "date": str(tr_date) if tr_date else '',
                        "reference": ref,
                        "supplier": txn['supplier'].strip() if txn['supplier'] else '',
                        "type": txn['type'].strip() if txn['type'] else '',
                        "value": round(pl_bal, 2),
                        "note": f"Balance £{pl_bal:.2f} matches variance £{variance_abs:.2f}"
                    })
                # Check for small balances under £1 that could be rounding
                elif 0.01 <= abs(pl_bal) < 1.00:
                    small_balance_refs.add(ref)
                    variance_items.append({
                        "source": "Small Balance",
                        "date": str(tr_date) if tr_date else '',
                        "reference": ref,
                        "supplier": txn['supplier'].strip() if txn['supplier'] else '',
                        "type": txn['type'].strip() if txn['type'] else '',
                        "value": round(pl_bal, 2),
                        "note": "Small balance - possible rounding"
                    })

            # Remove small balance/exact match items from pl_only_items
            variance_refs = exact_match_refs | small_balance_refs
            pl_only_items = [item for item in pl_only_items if item['reference'] not in variance_refs]

        # Always show all variance items - don't hide any
        # Separate into clear categories for the report
        display_items = []

        # Add NL only items (in Nominal but not in Purchase Ledger)
        for item in nl_only_items:
            item['category'] = 'NL_ONLY'
            display_items.append(item)

        # Add PL only items (in Purchase Ledger but not in Nominal)
        for item in pl_only_items:
            item['category'] = 'PL_ONLY'
            display_items.append(item)

        # Add value difference items
        for item in value_diff_items:
            item['category'] = 'VALUE_DIFF'
            display_items.append(item)

        # Add exact match/small balance items
        for item in variance_items:
            item['category'] = 'OTHER'
            display_items.append(item)

        # Calculate totals for each category
        nl_only_total = sum(item['value'] for item in nl_only_items)
        pl_only_total = sum(item['value'] for item in pl_only_items)
        value_diff_total = sum(item['value'] for item in value_diff_items)

        reconciliation["variance_analysis"] = {
            "items": display_items,
            "count": len(display_items),
            # Backward compatible fields for frontend
            "value_diff_count": len(value_diff_items),
            "nl_only_count": len(nl_only_items),
            "pl_only_count": len(pl_only_items),
            "small_balance_count": len(variance_items),
            # New detailed summary
            "summary": {
                "nl_only": {
                    "count": len(nl_only_items),
                    "total": round(nl_only_total, 2),
                    "description": "Entries in Nominal Ledger with no matching Purchase Ledger entry"
                },
                "pl_only": {
                    "count": len(pl_only_items),
                    "total": round(pl_only_total, 2),
                    "description": "Entries in Purchase Ledger with no matching Nominal Ledger entry"
                },
                "value_differences": {
                    "count": len(value_diff_items),
                    "total": round(value_diff_total, 2),
                    "description": "Matched entries with different values"
                }
            },
            "nl_total_check": round(nl_total_check, 2),
            "pl_total_check": round(pl_total_check, 2)
        }

        # ========== DETAILED ANALYSIS ==========
        # Get aged breakdown of PL outstanding (only active suppliers)
        aged_sql = """
            SELECT
                CASE
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
                    ELSE 'Over 90 days'
                END AS age_band,
                COUNT(*) AS count,
                SUM(pt_trbal) AS total
            FROM ptran
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname)
            GROUP BY CASE
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
                ELSE 'Over 90 days'
            END
            ORDER BY MIN(DATEDIFF(day, pt_trdate, GETDATE()))
        """
        aged_result = sql_connector.execute_query(aged_sql)
        if hasattr(aged_result, 'to_dict'):
            aged_result = aged_result.to_dict('records')

        aged_analysis = []
        for row in aged_result or []:
            aged_analysis.append({
                "age_band": row['age_band'],
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        reconciliation["aged_analysis"] = aged_analysis

        # Top suppliers with outstanding balances
        top_suppliers_sql = """
            SELECT TOP 10
                RTRIM(p.pn_account) AS account,
                RTRIM(p.pn_name) AS supplier_name,
                COUNT(*) AS invoice_count,
                SUM(pt.pt_trbal) AS outstanding
            FROM ptran pt
            JOIN pname p ON pt.pt_account = p.pn_account
            WHERE pt.pt_trbal <> 0
            GROUP BY p.pn_account, p.pn_name
            ORDER BY SUM(pt.pt_trbal) DESC
        """
        top_suppliers = sql_connector.execute_query(top_suppliers_sql)
        if hasattr(top_suppliers, 'to_dict'):
            top_suppliers = top_suppliers.to_dict('records')

        reconciliation["top_suppliers"] = [
            {
                "account": row['account'],
                "name": row['supplier_name'],
                "invoice_count": int(row['invoice_count'] or 0),
                "outstanding": round(float(row['outstanding'] or 0), 2)
            }
            for row in (top_suppliers or [])
        ]

        return reconciliation

    except Exception as e:
        logger.error(f"Creditors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/reconcile/debtors")
async def reconcile_debtors():
    """
    Reconcile Sales Ledger (stran) to Debtors Control Account (Nominal Ledger).
    Compares outstanding balances in stran with the control account in nacnt/ntran.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get dynamic control accounts from config
        control_accounts_config = _get_control_accounts_for_reconciliation()
        debtors_control = control_accounts_config['debtors']

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "sales_ledger": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": [],
            "control_account_used": debtors_control
        }

        # ========== SALES LEDGER (stran) ==========
        sl_outstanding_sql = """
            SELECT
                COUNT(*) AS transaction_count,
                SUM(st_trbal) AS total_outstanding
            FROM stran WITH (NOLOCK)
            WHERE st_trbal <> 0
        """
        sl_result = sql_connector.execute_query(sl_outstanding_sql)
        if hasattr(sl_result, 'to_dict'):
            sl_result = sl_result.to_dict('records')

        sl_total = float(sl_result[0]['total_outstanding'] or 0) if sl_result else 0
        sl_count = int(sl_result[0]['transaction_count'] or 0) if sl_result else 0

        # Breakdown by type
        sl_breakdown_sql = """
            SELECT
                st_trtype AS type,
                COUNT(*) AS count,
                SUM(st_trbal) AS total
            FROM stran WITH (NOLOCK)
            WHERE st_trbal <> 0
            GROUP BY st_trtype
            ORDER BY st_trtype
        """
        sl_breakdown = sql_connector.execute_query(sl_breakdown_sql)
        if hasattr(sl_breakdown, 'to_dict'):
            sl_breakdown = sl_breakdown.to_dict('records')

        type_names = {'I': 'Invoices', 'C': 'Credit Notes', 'R': 'Receipts', 'B': 'Brought Forward'}
        sl_by_type = []
        for row in sl_breakdown or []:
            sl_by_type.append({
                "type": row['type'].strip() if row['type'] else 'Unknown',
                "description": type_names.get(row['type'].strip(), row['type'].strip()) if row['type'] else 'Unknown',
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        # Verify against customer master (sname)
        sname_sql = """
            SELECT
                COUNT(*) AS customer_count,
                SUM(sn_currbal) AS total_balance
            FROM sname WITH (NOLOCK)
            WHERE sn_currbal <> 0
        """
        sname_result = sql_connector.execute_query(sname_sql)
        if hasattr(sname_result, 'to_dict'):
            sname_result = sname_result.to_dict('records')

        sname_total = float(sname_result[0]['total_balance'] or 0) if sname_result else 0
        sname_count = int(sname_result[0]['customer_count'] or 0) if sname_result else 0

        # Detailed verification: Find customers where sn_currbal doesn't match SUM(st_trbal)
        cust_balance_mismatch_sql = """
            SELECT
                s.sn_account AS account,
                RTRIM(s.sn_name) AS name,
                s.sn_currbal AS master_balance,
                COALESCE(t.txn_balance, 0) AS transaction_balance,
                s.sn_currbal - COALESCE(t.txn_balance, 0) AS variance
            FROM sname s WITH (NOLOCK)
            LEFT JOIN (
                SELECT st_account, SUM(st_trbal) AS txn_balance
                FROM stran WITH (NOLOCK)
                GROUP BY st_account
            ) t ON RTRIM(s.sn_account) = RTRIM(t.st_account)
            WHERE ABS(s.sn_currbal - COALESCE(t.txn_balance, 0)) >= 0.01
            ORDER BY ABS(s.sn_currbal - COALESCE(t.txn_balance, 0)) DESC
        """
        try:
            cust_balance_mismatches = sql_connector.execute_query(cust_balance_mismatch_sql)
            if hasattr(cust_balance_mismatches, 'to_dict'):
                cust_balance_mismatches = cust_balance_mismatches.to_dict('records')
        except Exception:
            cust_balance_mismatches = []

        customer_balance_issues = []
        for row in cust_balance_mismatches or []:
            customer_balance_issues.append({
                "account": row['account'].strip() if row['account'] else '',
                "name": row['name'] or '',
                "master_balance": round(float(row['master_balance'] or 0), 2),
                "transaction_balance": round(float(row['transaction_balance'] or 0), 2),
                "variance": round(float(row['variance'] or 0), 2)
            })

        # ========== TRANSFER FILE (snoml) ==========
        # Check for transactions sitting in the transfer file waiting to post to NL
        # sx_done = 'Y' means posted, anything else means pending
        snoml_pending_sql = """
            SELECT
                sx_nacnt AS nominal_account,
                sx_type AS type,
                sx_date AS date,
                sx_value AS value,
                sx_tref AS reference,
                sx_comment AS comment,
                sx_done AS status
            FROM snoml WITH (NOLOCK)
            WHERE sx_done <> 'Y' OR sx_done IS NULL
            ORDER BY sx_date DESC
        """
        try:
            snoml_pending = sql_connector.execute_query(snoml_pending_sql)
            if hasattr(snoml_pending, 'to_dict'):
                snoml_pending = snoml_pending.to_dict('records')
        except Exception:
            snoml_pending = []

        # Count posted vs pending in transfer file
        snoml_summary_sql = """
            SELECT
                CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(sx_value) AS total
            FROM snoml WITH (NOLOCK)
            GROUP BY CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            snoml_summary = sql_connector.execute_query(snoml_summary_sql)
            if hasattr(snoml_summary, 'to_dict'):
                snoml_summary = snoml_summary.to_dict('records')
        except Exception:
            snoml_summary = []

        sl_posted_count = 0
        sl_posted_total = 0
        sl_pending_count = 0
        sl_pending_total = 0
        for row in snoml_summary or []:
            if row['status'] == 'Posted':
                sl_posted_count = int(row['count'] or 0)
                sl_posted_total = float(row['total'] or 0)
            else:
                sl_pending_count = int(row['count'] or 0)
                sl_pending_total = float(row['total'] or 0)

        # Build pending transactions list from transfer file
        sl_pending_transactions = []
        for row in snoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            sl_pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "type": row['type'].strip() if row['type'] else '',
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["sales_ledger"] = {
            "source": "stran (Sales Ledger Transactions)",
            "total_outstanding": round(sl_total, 2),
            "transaction_count": sl_count,
            "breakdown_by_type": sl_by_type,
            "transfer_file": {
                "source": "snoml (Sales to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": sl_posted_count,
                    "total": round(sl_posted_total, 2)
                },
                "pending_transfer": {
                    "count": sl_pending_count,
                    "total": round(sl_pending_total, 2),
                    "transactions": sl_pending_transactions
                }
            },
            "customer_master_check": {
                "source": "sname (Customer Master)",
                "total": round(sname_total, 2),
                "customer_count": sname_count,
                "matches_stran": abs(sl_total - sname_total) < 0.01,
                "balance_variance": round(sl_total - sname_total, 2),
                "customers_with_balance_issues": customer_balance_issues[:20] if customer_balance_issues else [],
                "total_customers_with_issues": len(customer_balance_issues)
            }
        }

        # ========== NOMINAL LEDGER ==========
        # Find Debtors Control account - check various naming conventions
        # Include the dynamically loaded control account from config
        control_account_sql = f"""
            SELECT na_acnt, na_desc, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt WITH (NOLOCK)
            WHERE na_desc LIKE '%Debtor%Control%'
               OR na_desc LIKE '%Trade%Debtor%'
               OR na_acnt = '{debtors_control}'
            ORDER BY na_acnt
        """
        control_result = sql_connector.execute_query(control_account_sql)
        if hasattr(control_result, 'to_dict'):
            control_result = control_result.to_dict('records')

        nl_total = 0
        nl_details = []

        # Get current year from ntran
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        if control_result:
            for acc in control_result:
                acnt = acc['na_acnt'].strip()

                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                # Prior year B/F (debit balance for debtors)
                bf_balance = pry_dr - pry_cr

                # Get current year transactions from ntran
                ntran_current_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_current = sql_connector.execute_query(ntran_current_sql)
                if hasattr(ntran_current, 'to_dict'):
                    ntran_current = ntran_current.to_dict('records')

                current_year_dr = float(ntran_current[0]['debits'] or 0) if ntran_current else 0
                current_year_cr = float(ntran_current[0]['credits'] or 0) if ntran_current else 0
                current_year_net = float(ntran_current[0]['net'] or 0) if ntran_current else 0

                # For debtors control, keep the actual net value (don't force positive)
                # Positive = customers owe us (normal debit balance)
                # Negative = we owe customers (credit balance - e.g. overpayments, credit notes)
                # This must match stran sign convention for reconciliation
                current_year_balance = current_year_net

                # Get all years for reference
                ntran_sql = f"""
                    SELECT
                        nt_year,
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}'
                    GROUP BY nt_year
                    ORDER BY nt_year
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                ntran_by_year = []
                for row in ntran_result or []:
                    ntran_by_year.append({
                        "year": int(row['nt_year']),
                        "debits": round(float(row['debits'] or 0), 2),
                        "credits": round(float(row['credits'] or 0), 2),
                        "net": round(float(row['net'] or 0), 2)
                    })

                nl_details.append({
                    "account": acnt,
                    "description": acc['na_desc'].strip() if acc['na_desc'] else '',
                    "brought_forward": round(bf_balance, 2),
                    "current_year": current_year,
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(current_year_balance, 2),
                    "ntran_by_year": ntran_by_year
                })

                # Use current year balance for reconciliation
                nl_total += current_year_balance

        reconciliation["nominal_ledger"] = {
            "source": f"ntran (Nominal Ledger - {current_year} only)",
            "control_accounts": nl_details,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ========== VARIANCE ==========
        # The total SL outstanding should match the NL control account balance
        # Pending transfers are transactions in the SL that haven't been posted to NL yet

        # Primary variance: Total SL vs NL (this is what matters for reconciliation)
        variance = sl_total - nl_total
        variance_abs = abs(variance)

        # Secondary check: Posted-only variance (to verify NL posting integrity)
        variance_posted = sl_posted_total - nl_total
        variance_posted_abs = abs(variance_posted)

        reconciliation["variance"] = {
            "amount": round(variance, 2),
            "absolute": round(variance_abs, 2),
            "sales_ledger_total": round(sl_total, 2),
            "transfer_file_posted": round(sl_posted_total, 2),
            "transfer_file_pending": round(sl_pending_total, 2),
            "nominal_ledger_total": round(nl_total, 2),
            "posted_variance": round(variance_posted, 2),
            "posted_variance_abs": round(variance_posted_abs, 2),
            "reconciled": variance_abs < 1.00,
            "has_pending_transfers": sl_pending_count > 0
        }

        # Determine status based on total SL vs NL
        if variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            if sl_pending_count > 0:
                reconciliation["message"] = f"Sales Ledger reconciles to Nominal Ledger. {sl_pending_count} transactions (£{abs(sl_pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = "Sales Ledger reconciles to Nominal Ledger Debtors Control"
        else:
            reconciliation["status"] = "UNRECONCILED"
            if variance > 0:
                reconciliation["message"] = f"Sales Ledger is £{variance_abs:,.2f} MORE than Nominal Ledger Control"
            else:
                reconciliation["message"] = f"Sales Ledger is £{variance_abs:,.2f} LESS than Nominal Ledger Control"

        # ========== VARIANCE ANALYSIS ==========
        # Provide drill-down into transactions even when reconciled
        variance_items = []
        nl_only_items = []
        sl_only_items = []
        value_diff_items = []
        nl_total_check = 0
        sl_total_check = 0

        # Get control account codes, fallback to config value
        control_accounts = [acc['account'] for acc in nl_details] if nl_details else [debtors_control]
        control_accounts_str = "','".join(control_accounts)

        # Get ALL NL transactions for the control account
        # Note: nt_entr is the entry date, nt_cmnt contains the SL reference
        # We need all years to properly match against SL transactions that may be old
        nl_transactions_sql = f"""
            SELECT
                RTRIM(nt_cmnt) AS reference,
                nt_value AS nl_value,
                nt_entr AS date,
                nt_year AS year,
                nt_type AS type,
                RTRIM(nt_ref) AS nl_ref,
                RTRIM(nt_trnref) AS description
            FROM ntran
            WHERE nt_acnt IN ('{control_accounts_str}')
            ORDER BY nt_entr, nt_cmnt
        """
        try:
            nl_trans = sql_connector.execute_query(nl_transactions_sql)
            if hasattr(nl_trans, 'to_dict'):
                nl_trans = nl_trans.to_dict('records')
        except Exception as e:
            logger.error(f"NL transactions query failed: {e}")
            nl_trans = []

        # Get ALL SL transactions with non-zero balance (to match the total calculation)
        # Include all years to properly identify all variance sources
        sl_transactions_sql = """
            SELECT
                RTRIM(st_trref) AS reference,
                st_trbal AS sl_balance,
                st_trvalue AS sl_value,
                st_trdate AS date,
                RTRIM(st_account) AS customer,
                st_trtype AS type,
                RTRIM(st_custref) AS customer_ref
            FROM stran
            WHERE st_trbal <> 0
            ORDER BY st_trdate, st_trref
        """
        try:
            sl_trans = sql_connector.execute_query(sl_transactions_sql)
            if hasattr(sl_trans, 'to_dict'):
                sl_trans = sl_trans.to_dict('records')
        except Exception as e:
            logger.error(f"SL transactions query failed: {e}")
            sl_trans = []

        # Build NL lookups for matching
        # Primary: by reference only (nt_cmnt = st_trref) - most reliable
        # Secondary: by date + abs(value) + reference
        # Tertiary: by reference + abs_value
        # Use lists to handle multiple entries with same reference
        nl_by_ref = {}  # reference -> [nl_entries] (PRIMARY - most reliable)
        nl_by_key = {}  # date|value|ref -> nl_entry
        nl_by_ref_val = {}  # ref|value -> nl_entry

        for txn in nl_trans or []:
            ref = txn['reference'].strip() if txn['reference'] else ''
            nl_value = float(txn['nl_value'] or 0)
            nl_date = txn['date']
            if hasattr(nl_date, 'strftime'):
                nl_date_str = nl_date.strftime('%Y-%m-%d')
            else:
                nl_date_str = str(nl_date) if nl_date else ''

            abs_val = abs(nl_value)
            key = f"{nl_date_str}|{abs_val:.2f}|{ref}"
            ref_val_key = f"{ref}|{abs_val:.2f}"

            nl_entry = {
                'value': nl_value,
                'date': nl_date_str,
                'reference': ref,
                'year': txn['year'],
                'type': txn['type'],
                'matched': False,
                'key': key
            }

            # Primary lookup by reference only (nt_cmnt = st_trref)
            # Store as list to handle multiple entries with same reference
            if ref:
                if ref not in nl_by_ref:
                    nl_by_ref[ref] = []
                nl_by_ref[ref].append(nl_entry)

            if key not in nl_by_key:
                nl_by_key[key] = nl_entry
            else:
                nl_by_key[key]['value'] += nl_value

            if ref_val_key not in nl_by_ref_val and ref:
                nl_by_ref_val[ref_val_key] = nl_entry

            nl_total_check += nl_value

        # Build SL lookup and try to match against NL
        sl_by_key = {}
        matched_sl_keys = set()

        for txn in sl_trans or []:
            ref = txn['reference'].strip() if txn['reference'] else ''
            sl_bal = float(txn['sl_balance'] or 0)
            sl_value = float(txn['sl_value'] or 0)
            customer = txn['customer'].strip() if txn['customer'] else ''
            tr_type = txn['type'].strip() if txn['type'] else ''
            cust_ref = txn['customer_ref'].strip() if txn['customer_ref'] else ''
            sl_date = txn['date']

            if hasattr(sl_date, 'strftime'):
                sl_date_str = sl_date.strftime('%Y-%m-%d')
            else:
                sl_date_str = str(sl_date) if sl_date else ''

            # Create composite key: date|abs_value|reference
            abs_val = abs(sl_value)
            key = f"{sl_date_str}|{abs_val:.2f}|{ref}"
            ref_val_key = f"{ref}|{abs_val:.2f}"

            sl_by_key[key] = {
                'balance': sl_bal,
                'value': sl_value,
                'date': sl_date_str,
                'reference': ref,
                'customer': customer,
                'type': tr_type,
                'customer_ref': cust_ref
            }

            sl_total_check += sl_bal

            # Try to match with NL using multiple strategies
            # Priority: 1) Reference (nt_cmnt = st_trref), 2) Date+Value+Ref, 3) Ref+Value, 4) Fuzzy
            nl_data = None
            match_type = None

            # Strategy 1: Match by reference only (nt_cmnt = st_trref) - MOST RELIABLE
            # For generic refs like 'rec', 'pay' - require exact value match to avoid false matches
            # For specific refs (invoice numbers) - allow small tolerance
            GENERIC_REFS = {'rec', 'pay', 'contra', 'refund', 'adjustment', 'adj', 'jnl', 'journal'}
            is_generic_ref = ref.lower() in GENERIC_REFS if ref else False

            if ref and ref in nl_by_ref:
                for nl_entry in nl_by_ref[ref]:
                    if not nl_entry['matched']:
                        nl_abs = abs(nl_entry['value'])
                        value_diff = abs(nl_abs - abs_val)

                        # For generic refs, require exact match (within £0.10 for rounding)
                        # For specific refs, allow 10% or £10 tolerance
                        if is_generic_ref:
                            value_tolerance = 0.10  # Must be exact for generic refs
                        else:
                            value_tolerance = max(10.0, abs_val * 0.1)

                        if value_diff <= value_tolerance:
                            nl_data = nl_entry
                            match_type = "reference"
                            break

            # Strategy 2: Match by date + value + reference (exact composite key)
            if not nl_data and key in nl_by_key:
                nl_entry = nl_by_key[key]
                if not nl_entry['matched']:
                    nl_data = nl_entry
                    match_type = "exact"

            # Strategy 3: Match by reference + value (for date mismatches)
            if not nl_data and ref and ref_val_key in nl_by_ref_val:
                nl_entry = nl_by_ref_val[ref_val_key]
                if not nl_entry['matched']:
                    nl_data = nl_entry
                    match_type = "ref_val"

            # Strategy 4: Fuzzy value matching with same reference
            if not nl_data and ref:
                for nl_key, nl_entry in nl_by_ref_val.items():
                    if nl_entry['matched']:
                        continue
                    nl_ref = nl_entry['reference']
                    if nl_ref == ref:
                        nl_abs = abs(nl_entry['value'])
                        diff = abs(nl_abs - abs_val)
                        if diff <= 0.10:
                            nl_data = nl_entry
                            match_type = "fuzzy"
                            break

            if nl_data:
                nl_data['matched'] = True
                matched_sl_keys.add(key)

                # Check if absolute values match
                nl_val = nl_data['value']
                nl_abs = abs(nl_val)
                sl_abs = abs(sl_value)

                actual_diff = round(nl_abs - sl_abs, 2)
                if abs(actual_diff) >= 0.01:
                    value_diff_items.append({
                        "source": "Value Difference",
                        "date": sl_date_str,
                        "reference": ref,
                        "customer": customer,
                        "type": tr_type,
                        "value": actual_diff,
                        "nl_value": round(nl_val, 2),
                        "sl_value": round(sl_value, 2),
                        "sl_balance": round(sl_bal, 2),
                        "match_type": match_type,
                        "note": f"NL: £{nl_abs:.2f} vs SL: £{sl_abs:.2f} (diff: £{abs(actual_diff):.2f})"
                    })

        # Find unmatched NL entries
        for key, nl_data in nl_by_key.items():
            if not nl_data['matched'] and abs(nl_data['value']) >= 0.01:
                nl_only_items.append({
                    "source": "Nominal Ledger Only",
                    "date": nl_data['date'],
                    "reference": nl_data['reference'],
                    "customer": "",
                    "type": nl_data['type'] or "NL",
                    "value": round(nl_data['value'], 2),
                    "note": f"In NL (year {nl_data['year']}) but no matching SL entry"
                })

        # Find unmatched SL entries
        for key, sl_data in sl_by_key.items():
            if key not in matched_sl_keys and abs(sl_data['balance']) >= 0.01:
                sl_only_items.append({
                    "source": "Sales Ledger Only",
                    "date": sl_data['date'],
                    "reference": sl_data['reference'],
                    "customer": sl_data['customer'],
                    "type": sl_data['type'],
                    "value": round(sl_data['balance'], 2),
                    "note": f"In SL but no matching NL entry (key: {key})"
                })

        # Look for small balances that could be rounding
        exact_match_refs = set()
        small_balance_refs = set()

        for txn in sl_trans or []:
            sl_bal = float(txn['sl_balance'] or 0)
            ref = txn['reference'].strip() if txn['reference'] else ''
            sl_date = txn['date']
            if hasattr(sl_date, 'strftime'):
                sl_date_str = sl_date.strftime('%Y-%m-%d')
            else:
                sl_date_str = str(sl_date) if sl_date else ''

            # Check for exact match to variance
            if abs(abs(sl_bal) - variance_abs) < 0.02:
                exact_match_refs.add(ref)
                variance_items.append({
                    "source": "Exact Match",
                    "date": sl_date_str,
                    "reference": ref,
                    "customer": txn['customer'].strip() if txn['customer'] else '',
                    "type": txn['type'].strip() if txn['type'] else '',
                    "value": round(sl_bal, 2),
                    "note": f"Balance £{sl_bal:.2f} matches variance £{variance_abs:.2f}"
                })
            elif 0.01 <= abs(sl_bal) < 1.00:
                small_balance_refs.add(ref)
                variance_items.append({
                    "source": "Small Balance",
                    "date": sl_date_str,
                    "reference": ref,
                    "customer": txn['customer'].strip() if txn['customer'] else '',
                    "type": txn['type'].strip() if txn['type'] else '',
                    "value": round(sl_bal, 2),
                    "note": "Small balance - possible rounding"
                })

        # Remove small balance/exact match items from sl_only_items
        variance_refs = exact_match_refs | small_balance_refs
        sl_only_items = [item for item in sl_only_items if item['reference'] not in variance_refs]

        # Calculate totals for unmatched items
        sl_only_total = sum(item['value'] for item in sl_only_items)
        nl_only_total = sum(item['value'] for item in nl_only_items)
        value_diff_total = sum(item['value'] for item in value_diff_items)

        # Theoretical variance from items: SL only - NL only + value diffs
        # (SL only adds to SL total, NL only reduces it, value diffs are SL perspective)
        calculated_variance = value_diff_total

        # For display - show top items sorted by absolute value
        display_items = []

        # Always show value differences
        display_items.extend(sorted(value_diff_items, key=lambda x: abs(x['value']), reverse=True))

        # Show small balance items
        display_items.extend(variance_items)

        # Show top 10 unmatched items by value (if not too many)
        if len(sl_only_items) <= 50:
            top_sl_only = sorted(sl_only_items, key=lambda x: abs(x['value']), reverse=True)[:10]
            display_items.extend(top_sl_only)

        if len(nl_only_items) <= 50:
            top_nl_only = sorted(nl_only_items, key=lambda x: abs(x['value']), reverse=True)[:10]
            display_items.extend(top_nl_only)

        # Analysis note
        if len(sl_only_items) > 50 or len(nl_only_items) > 50:
            analysis_note = f"NL uses batch posting. SL unmatched: {len(sl_only_items)} items (£{sl_only_total:,.2f}), NL unmatched: {len(nl_only_items)} items"
        else:
            analysis_note = None

        reconciliation["variance_analysis"] = {
            "items": display_items,
            "count": len(display_items),
            "value_diff_count": len(value_diff_items),
            "value_diff_total": round(value_diff_total, 2),
            "nl_only_count": len(nl_only_items),
            "nl_only_total": round(nl_only_total, 2),
            "sl_only_count": len(sl_only_items),
            "sl_only_total": round(sl_only_total, 2),
            "small_balance_count": len(variance_items),
            "nl_total_check": round(nl_total_check, 2),
            "sl_total_check": round(sl_total_check, 2),
            "note": analysis_note
        }

        # ========== DETAILED ANALYSIS ==========
        # Get aged breakdown of SL outstanding (only active customers)
        aged_sql = f"""
            SELECT
                CASE
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
                    ELSE 'Over 90 days'
                END AS age_band,
                COUNT(*) AS count,
                SUM(st_trbal) AS total
            FROM stran
            WHERE st_trbal <> 0
              AND RTRIM(st_account) IN (SELECT RTRIM(sn_account) FROM sname)
            GROUP BY CASE
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
                ELSE 'Over 90 days'
            END
            ORDER BY MIN(DATEDIFF(day, st_trdate, GETDATE()))
        """
        aged_result = sql_connector.execute_query(aged_sql)
        if hasattr(aged_result, 'to_dict'):
            aged_result = aged_result.to_dict('records')

        aged_analysis = []
        for row in aged_result or []:
            aged_analysis.append({
                "age_band": row['age_band'],
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        reconciliation["aged_analysis"] = aged_analysis

        # Top customers with outstanding balances
        top_customers_sql = """
            SELECT TOP 10
                RTRIM(s.sn_account) AS account,
                RTRIM(s.sn_name) AS customer_name,
                COUNT(*) AS invoice_count,
                SUM(st.st_trbal) AS outstanding
            FROM stran st
            JOIN sname s ON st.st_account = s.sn_account
            WHERE st.st_trbal <> 0
            GROUP BY s.sn_account, s.sn_name
            ORDER BY SUM(st.st_trbal) DESC
        """
        top_customers = sql_connector.execute_query(top_customers_sql)
        if hasattr(top_customers, 'to_dict'):
            top_customers = top_customers.to_dict('records')

        reconciliation["top_customers"] = [
            {
                "account": row['account'],
                "name": row['customer_name'],
                "invoice_count": int(row['invoice_count'] or 0),
                "outstanding": round(float(row['outstanding'] or 0), 2)
            }
            for row in (top_customers or [])
        ]

        return reconciliation

    except Exception as e:
        logger.error(f"Debtors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/reconcile/trial-balance")
async def reconcile_trial_balance():
    """
    Trial Balance check - verifies the nominal ledger as a whole balances (debits = credits).
    Also shows all nominal accounts with their balances.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "summary": {},
            "accounts": [],
            "status": "UNBALANCED",
            "message": ""
        }

        # Get current year
        cy_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(cy_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # Get all nominal accounts with balances
        accounts_sql = """
            SELECT
                n.na_acnt AS account,
                RTRIM(n.na_desc) AS description,
                n.na_type AS type,
                n.na_prydr AS prior_debits,
                n.na_prycr AS prior_credits,
                n.na_ytddr AS ytd_debits,
                n.na_ytdcr AS ytd_credits,
                COALESCE(t.current_debits, 0) AS current_debits,
                COALESCE(t.current_credits, 0) AS current_credits,
                COALESCE(t.current_net, 0) AS current_net
            FROM nacnt n WITH (NOLOCK)
            LEFT JOIN (
                SELECT
                    nt_acnt,
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS current_debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS current_credits,
                    SUM(nt_value) AS current_net
                FROM ntran WITH (NOLOCK)
                WHERE nt_year = {current_year}
                GROUP BY nt_acnt
            ) t ON n.na_acnt = t.nt_acnt
            WHERE n.na_ytddr <> 0 OR n.na_ytdcr <> 0 OR n.na_prydr <> 0 OR n.na_prycr <> 0
               OR COALESCE(t.current_debits, 0) <> 0 OR COALESCE(t.current_credits, 0) <> 0
            ORDER BY n.na_acnt
        """.format(current_year=current_year)
        accounts_result = sql_connector.execute_query(accounts_sql)
        if hasattr(accounts_result, 'to_dict'):
            accounts_result = accounts_result.to_dict('records')

        # Process accounts and calculate totals
        total_bf_debits = 0
        total_bf_credits = 0
        total_current_debits = 0
        total_current_credits = 0
        total_closing_debits = 0
        total_closing_credits = 0

        type_names = {
            'A': 'Asset',
            'L': 'Liability',
            'E': 'Expense',
            'I': 'Income',
            'C': 'Capital',
            'P': 'P&L',
            'B': 'Balance Sheet'
        }

        accounts = []
        for row in accounts_result or []:
            prior_dr = float(row['prior_debits'] or 0)
            prior_cr = float(row['prior_credits'] or 0)
            current_dr = float(row['current_debits'] or 0)
            current_cr = float(row['current_credits'] or 0)

            # B/F balance (prior year net)
            bf_balance = prior_dr - prior_cr

            # Current year movement
            current_net = current_dr - current_cr

            # Closing balance
            closing_balance = bf_balance + current_net

            # Track totals (debit vs credit balances)
            if bf_balance > 0:
                total_bf_debits += bf_balance
            else:
                total_bf_credits += abs(bf_balance)

            total_current_debits += current_dr
            total_current_credits += current_cr

            if closing_balance > 0:
                total_closing_debits += closing_balance
            else:
                total_closing_credits += abs(closing_balance)

            account_type = row['type'].strip() if row['type'] else ''

            accounts.append({
                "account": row['account'].strip() if row['account'] else '',
                "description": row['description'] or '',
                "type": account_type,
                "type_name": type_names.get(account_type, account_type),
                "bf_balance": round(bf_balance, 2),
                "current_debits": round(current_dr, 2),
                "current_credits": round(current_cr, 2),
                "current_net": round(current_net, 2),
                "closing_balance": round(closing_balance, 2)
            })

        result["accounts"] = accounts
        result["current_year"] = current_year

        # Calculate variance (should be zero for a balanced trial balance)
        bf_variance = abs(total_bf_debits - total_bf_credits)
        current_variance = abs(total_current_debits - total_current_credits)
        closing_variance = abs(total_closing_debits - total_closing_credits)

        result["summary"] = {
            "brought_forward": {
                "debits": round(total_bf_debits, 2),
                "credits": round(total_bf_credits, 2),
                "variance": round(bf_variance, 2),
                "balanced": bf_variance < 1.00
            },
            "current_year": {
                "debits": round(total_current_debits, 2),
                "credits": round(total_current_credits, 2),
                "variance": round(current_variance, 2),
                "balanced": current_variance < 1.00
            },
            "closing": {
                "debits": round(total_closing_debits, 2),
                "credits": round(total_closing_credits, 2),
                "variance": round(closing_variance, 2),
                "balanced": closing_variance < 1.00
            },
            "account_count": len(accounts)
        }

        # Overall status
        all_balanced = (bf_variance < 1.00 and current_variance < 1.00 and closing_variance < 1.00)

        if all_balanced:
            result["status"] = "BALANCED"
            result["message"] = f"Trial Balance is correct. {len(accounts)} accounts with matching debits and credits."
        else:
            result["status"] = "UNBALANCED"
            variances = []
            if bf_variance >= 1.00:
                variances.append(f"B/F: £{bf_variance:,.2f}")
            if current_variance >= 1.00:
                variances.append(f"Current: £{current_variance:,.2f}")
            if closing_variance >= 1.00:
                variances.append(f"Closing: £{closing_variance:,.2f}")
            result["message"] = f"Trial Balance has variance: {', '.join(variances)}"

        return result

    except Exception as e:
        logger.error(f"Trial balance check failed: {e}")
        return {"success": False, "error": str(e)}


def get_vat_quarter_dates(reference_date: datetime = None):
    """
    Calculate VAT quarter start/end dates based on standard UK calendar quarters.
    Returns current quarter dates and previous quarters for reference.
    """
    if reference_date is None:
        reference_date = datetime.now()

    year = reference_date.year
    month = reference_date.month

    # Determine current quarter (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)
    if month <= 3:
        current_q_start = datetime(year, 1, 1)
        current_q_end = datetime(year, 3, 31)
        quarter_name = f"Q1 {year}"
        quarter_num = 1
    elif month <= 6:
        current_q_start = datetime(year, 4, 1)
        current_q_end = datetime(year, 6, 30)
        quarter_name = f"Q2 {year}"
        quarter_num = 2
    elif month <= 9:
        current_q_start = datetime(year, 7, 1)
        current_q_end = datetime(year, 9, 30)
        quarter_name = f"Q3 {year}"
        quarter_num = 3
    else:
        current_q_start = datetime(year, 10, 1)
        current_q_end = datetime(year, 12, 31)
        quarter_name = f"Q4 {year}"
        quarter_num = 4

    # Build list of quarters (current + previous 3)
    quarters = []
    for i in range(4):
        q_num = quarter_num - i
        q_year = year
        while q_num <= 0:
            q_num += 4
            q_year -= 1

        if q_num == 1:
            q_start = datetime(q_year, 1, 1)
            q_end = datetime(q_year, 3, 31)
        elif q_num == 2:
            q_start = datetime(q_year, 4, 1)
            q_end = datetime(q_year, 6, 30)
        elif q_num == 3:
            q_start = datetime(q_year, 7, 1)
            q_end = datetime(q_year, 9, 30)
        else:
            q_start = datetime(q_year, 10, 1)
            q_end = datetime(q_year, 12, 31)

        quarters.append({
            "name": f"Q{q_num} {q_year}",
            "start": q_start.strftime('%Y-%m-%d'),
            "end": q_end.strftime('%Y-%m-%d'),
            "is_current": i == 0
        })

    return {
        "current_quarter": quarter_name,
        "quarter_start": current_q_start.strftime('%Y-%m-%d'),
        "quarter_end": current_q_end.strftime('%Y-%m-%d'),
        "quarters": quarters
    }


@router.get("/api/reconcile/summary")
async def reconcile_summary():
    """
    Quick summary of all reconciliation checks - shows at a glance whether everything balances.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    summary = {
        "success": True,
        "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "checks": [],
        "all_reconciled": True,
        "total_checks": 0,
        "passed_checks": 0,
        "failed_checks": 0
    }

    try:
        # Get control accounts
        control_accounts = _get_control_accounts_for_reconciliation()

        # ========== 1. DEBTORS CHECK ==========
        try:
            # Sales Ledger total
            sl_sql = "SELECT SUM(st_trbal) AS total FROM stran WITH (NOLOCK) WHERE st_trbal <> 0"
            sl_result = sql_connector.execute_query(sl_sql)
            if hasattr(sl_result, 'to_dict'):
                sl_result = sl_result.to_dict('records')
            sl_total = float(sl_result[0]['total'] or 0) if sl_result and sl_result[0]['total'] else 0

            # Customer master total
            sname_sql = "SELECT SUM(sn_currbal) AS total FROM sname WITH (NOLOCK) WHERE sn_currbal <> 0"
            sname_result = sql_connector.execute_query(sname_sql)
            if hasattr(sname_result, 'to_dict'):
                sname_result = sname_result.to_dict('records')
            sname_total = float(sname_result[0]['total'] or 0) if sname_result and sname_result[0]['total'] else 0

            # NL Debtors control
            debtors_control = control_accounts['debtors']
            nl_debtors_sql = f"""
                SELECT SUM(nt_value) AS total
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{debtors_control}'
            """
            nl_debtors_result = sql_connector.execute_query(nl_debtors_sql)
            if hasattr(nl_debtors_result, 'to_dict'):
                nl_debtors_result = nl_debtors_result.to_dict('records')
            nl_debtors_total = float(nl_debtors_result[0]['total'] or 0) if nl_debtors_result and nl_debtors_result[0]['total'] else 0

            # Check stran vs sname
            sl_vs_sname_variance = abs(sl_total - sname_total)
            sl_vs_sname_ok = sl_vs_sname_variance < 1.00

            # Check stran vs NL
            sl_vs_nl_variance = abs(sl_total - nl_debtors_total)
            sl_vs_nl_ok = sl_vs_nl_variance < 1.00

            debtors_ok = sl_vs_sname_ok and sl_vs_nl_ok

            summary["checks"].append({
                "name": "Debtors",
                "icon": "users",
                "reconciled": debtors_ok,
                "details": [
                    {"label": "Sales Ledger (stran)", "value": round(sl_total, 2)},
                    {"label": "Customer Master (sname)", "value": round(sname_total, 2)},
                    {"label": f"Nominal ({debtors_control})", "value": round(nl_debtors_total, 2)},
                ],
                "variances": [
                    {"label": "SL vs Master", "value": round(sl_vs_sname_variance, 2), "ok": sl_vs_sname_ok},
                    {"label": "SL vs NL", "value": round(sl_vs_nl_variance, 2), "ok": sl_vs_nl_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Debtors",
                "icon": "users",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 2. CREDITORS CHECK ==========
        try:
            # Purchase Ledger total
            pl_sql = """
                SELECT SUM(pt_trbal) AS total
                FROM ptran WITH (NOLOCK)
                WHERE pt_trbal <> 0
                  AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
            """
            pl_result = sql_connector.execute_query(pl_sql)
            if hasattr(pl_result, 'to_dict'):
                pl_result = pl_result.to_dict('records')
            pl_total = float(pl_result[0]['total'] or 0) if pl_result and pl_result[0]['total'] else 0

            # Supplier master total
            pname_sql = "SELECT SUM(pn_currbal) AS total FROM pname WITH (NOLOCK) WHERE pn_currbal <> 0"
            pname_result = sql_connector.execute_query(pname_sql)
            if hasattr(pname_result, 'to_dict'):
                pname_result = pname_result.to_dict('records')
            pname_total = float(pname_result[0]['total'] or 0) if pname_result and pname_result[0]['total'] else 0

            # NL Creditors control (negate for comparison - NL is opposite sign)
            creditors_control = control_accounts['creditors']
            nl_creditors_sql = f"""
                SELECT SUM(nt_value) AS total
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{creditors_control}'
            """
            nl_creditors_result = sql_connector.execute_query(nl_creditors_sql)
            if hasattr(nl_creditors_result, 'to_dict'):
                nl_creditors_result = nl_creditors_result.to_dict('records')
            nl_creditors_total = -float(nl_creditors_result[0]['total'] or 0) if nl_creditors_result and nl_creditors_result[0]['total'] else 0

            # Check ptran vs pname
            pl_vs_pname_variance = abs(pl_total - pname_total)
            pl_vs_pname_ok = pl_vs_pname_variance < 1.00

            # Check ptran vs NL
            pl_vs_nl_variance = abs(pl_total - nl_creditors_total)
            pl_vs_nl_ok = pl_vs_nl_variance < 1.00

            creditors_ok = pl_vs_pname_ok and pl_vs_nl_ok

            summary["checks"].append({
                "name": "Creditors",
                "icon": "building",
                "reconciled": creditors_ok,
                "details": [
                    {"label": "Purchase Ledger (ptran)", "value": round(pl_total, 2)},
                    {"label": "Supplier Master (pname)", "value": round(pname_total, 2)},
                    {"label": f"Nominal ({creditors_control})", "value": round(nl_creditors_total, 2)},
                ],
                "variances": [
                    {"label": "PL vs Master", "value": round(pl_vs_pname_variance, 2), "ok": pl_vs_pname_ok},
                    {"label": "PL vs NL", "value": round(pl_vs_nl_variance, 2), "ok": pl_vs_nl_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Creditors",
                "icon": "building",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 3. CASHBOOK CHECK ==========
        try:
            # Get bank accounts - nk_acnt is both the bank code AND the nominal code
            banks_sql = """
                SELECT nk_acnt, nk_curbal
                FROM nbank WITH (NOLOCK)
            """
            banks_result = sql_connector.execute_query(banks_sql)
            if hasattr(banks_result, 'to_dict'):
                banks_result = banks_result.to_dict('records')

            bank_master_total = 0
            nl_bank_total = 0

            for bank in banks_result or []:
                bank_code = bank['nk_acnt'].strip()
                # nk_curbal is in pence
                master_bal = float(bank['nk_curbal'] or 0) / 100.0
                bank_master_total += master_bal

                # In Opera, bank account code IS the nominal code (e.g., BC010)
                nl_sql = f"SELECT SUM(nt_value) AS total FROM ntran WITH (NOLOCK) WHERE nt_acnt = '{bank_code}'"
                nl_result = sql_connector.execute_query(nl_sql)
                if hasattr(nl_result, 'to_dict'):
                    nl_result = nl_result.to_dict('records')
                nl_bal = float(nl_result[0]['total'] or 0) if nl_result and nl_result[0]['total'] else 0
                nl_bank_total += nl_bal

            # Check bank master vs NL
            bank_variance = abs(bank_master_total - nl_bank_total)
            cashbook_ok = bank_variance < 1.00

            summary["checks"].append({
                "name": "Cashbook",
                "icon": "book",
                "reconciled": cashbook_ok,
                "details": [
                    {"label": "Bank Master (nbank)", "value": round(bank_master_total, 2)},
                    {"label": "Nominal Ledger", "value": round(nl_bank_total, 2)},
                ],
                "variances": [
                    {"label": "Bank vs NL", "value": round(bank_variance, 2), "ok": cashbook_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Cashbook",
                "icon": "book",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 4. VAT CHECK ==========
        try:
            # Get VAT nominal accounts from ztax
            ztax_sql = """
                SELECT DISTINCT tx_nominal, tx_trantyp
                FROM ztax WITH (NOLOCK)
                WHERE tx_ctrytyp = 'H' AND tx_nominal IS NOT NULL AND tx_nominal <> ''
            """
            ztax_result = sql_connector.execute_query(ztax_sql)
            if hasattr(ztax_result, 'to_dict'):
                ztax_result = ztax_result.to_dict('records')

            output_nominals = set()
            input_nominals = set()
            for row in ztax_result or []:
                nominal = row['tx_nominal'].strip() if row['tx_nominal'] else ''
                vat_type = row['tx_trantyp'].strip() if row['tx_trantyp'] else ''
                if nominal:
                    if vat_type == 'S':
                        output_nominals.add(nominal)
                    elif vat_type == 'P':
                        input_nominals.add(nominal)

            # Get current year
            cy_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
            cy_result = sql_connector.execute_query(cy_sql)
            if hasattr(cy_result, 'to_dict'):
                cy_result = cy_result.to_dict('records')
            current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

            # nvat totals
            nvat_output_sql = f"SELECT SUM(nv_vatval) AS total FROM nvat WITH (NOLOCK) WHERE nv_vattype = 'S' AND YEAR(nv_date) = {current_year}"
            nvat_output_result = sql_connector.execute_query(nvat_output_sql)
            if hasattr(nvat_output_result, 'to_dict'):
                nvat_output_result = nvat_output_result.to_dict('records')
            nvat_output = float(nvat_output_result[0]['total'] or 0) if nvat_output_result and nvat_output_result[0]['total'] else 0

            nvat_input_sql = f"SELECT SUM(nv_vatval) AS total FROM nvat WITH (NOLOCK) WHERE nv_vattype = 'P' AND YEAR(nv_date) = {current_year}"
            nvat_input_result = sql_connector.execute_query(nvat_input_sql)
            if hasattr(nvat_input_result, 'to_dict'):
                nvat_input_result = nvat_input_result.to_dict('records')
            nvat_input = float(nvat_input_result[0]['total'] or 0) if nvat_input_result and nvat_input_result[0]['total'] else 0

            nvat_net = nvat_output - nvat_input

            # NL VAT totals
            nl_vat_total = 0
            all_vat_nominals = output_nominals.union(input_nominals)
            for acnt in all_vat_nominals:
                nl_sql = f"SELECT SUM(nt_value) AS total FROM ntran WITH (NOLOCK) WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}"
                nl_result = sql_connector.execute_query(nl_sql)
                if hasattr(nl_result, 'to_dict'):
                    nl_result = nl_result.to_dict('records')
                nl_vat_total += -float(nl_result[0]['total'] or 0) if nl_result and nl_result[0]['total'] else 0

            vat_variance = abs(nvat_net - nl_vat_total)
            vat_ok = vat_variance < 1.00

            summary["checks"].append({
                "name": "VAT",
                "icon": "receipt",
                "reconciled": vat_ok,
                "details": [
                    {"label": "Output VAT (nvat)", "value": round(nvat_output, 2)},
                    {"label": "Input VAT (nvat)", "value": round(nvat_input, 2)},
                    {"label": "Net VAT (nvat)", "value": round(nvat_net, 2)},
                    {"label": "Nominal Ledger", "value": round(nl_vat_total, 2)},
                ],
                "variances": [
                    {"label": "nvat vs NL", "value": round(vat_variance, 2), "ok": vat_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "VAT",
                "icon": "receipt",
                "reconciled": False,
                "error": str(e)
            })

        # Calculate overall status
        summary["total_checks"] = len(summary["checks"])
        summary["passed_checks"] = sum(1 for c in summary["checks"] if c.get("reconciled", False))
        summary["failed_checks"] = summary["total_checks"] - summary["passed_checks"]
        summary["all_reconciled"] = summary["failed_checks"] == 0

        return summary

    except Exception as e:
        logger.error(f"Reconciliation summary failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/reconcile/vat/diagnostic")
async def vat_diagnostic():
    """
    Diagnostic endpoint to check VAT table data availability.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {"tables": {}}

        # Check zvtran
        try:
            zvtran_sql = """
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN va_done = 0 THEN 1 ELSE 0 END) AS uncommitted,
                    SUM(CASE WHEN va_done = 1 THEN 1 ELSE 0 END) AS committed,
                    MIN(va_taxdate) AS min_date,
                    MAX(va_taxdate) AS max_date,
                    SUM(va_vatval) AS total_vat
                FROM zvtran WITH (NOLOCK)
            """
            zvtran_result = sql_connector.execute_query(zvtran_sql)
            if hasattr(zvtran_result, 'to_dict'):
                zvtran_result = zvtran_result.to_dict('records')
            result["tables"]["zvtran"] = zvtran_result[0] if zvtran_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["zvtran"] = {"error": str(e)}

        # Check nvat
        try:
            nvat_sql = """
                SELECT
                    COUNT(*) AS total_rows,
                    MIN(nv_date) AS min_date,
                    MAX(nv_date) AS max_date,
                    SUM(nv_vatval) AS total_vat,
                    COUNT(DISTINCT nv_vattype) AS vat_types
                FROM nvat WITH (NOLOCK)
            """
            nvat_result = sql_connector.execute_query(nvat_sql)
            if hasattr(nvat_result, 'to_dict'):
                nvat_result = nvat_result.to_dict('records')
            result["tables"]["nvat"] = nvat_result[0] if nvat_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["nvat"] = {"error": str(e)}

        # Check ztax (VAT codes)
        try:
            ztax_sql = """
                SELECT COUNT(*) AS total_codes
                FROM ztax WITH (NOLOCK)
                WHERE tx_ctrytyp = 'H'
            """
            ztax_result = sql_connector.execute_query(ztax_sql)
            if hasattr(ztax_result, 'to_dict'):
                ztax_result = ztax_result.to_dict('records')
            result["tables"]["ztax"] = ztax_result[0] if ztax_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["ztax"] = {"error": str(e)}

        # Check ntran for current year
        try:
            ntran_sql = """
                SELECT
                    MAX(nt_year) AS current_year,
                    COUNT(*) AS total_rows
                FROM ntran WITH (NOLOCK)
            """
            ntran_result = sql_connector.execute_query(ntran_sql)
            if hasattr(ntran_result, 'to_dict'):
                ntran_result = ntran_result.to_dict('records')
            result["tables"]["ntran"] = ntran_result[0] if ntran_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["ntran"] = {"error": str(e)}

        return result

    except Exception as e:
        return {"error": str(e)}


@router.get("/api/reconcile/vat/variance-drilldown")
async def vat_variance_drilldown():
    """
    Drill-down to identify causes of VAT variance between zvtran and nominal ledger.
    Shows transactions that don't reconcile.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {
            "success": True,
            "analysis": {}
        }

        # Get VAT nominal accounts from ztax
        ztax_sql = """
            SELECT DISTINCT tx_nominal
            FROM ztax WITH (NOLOCK)
            WHERE tx_ctrytyp = 'H'
              AND tx_nominal IS NOT NULL
              AND RTRIM(tx_nominal) != ''
        """
        ztax_result = sql_connector.execute_query(ztax_sql)
        if hasattr(ztax_result, 'to_dict'):
            ztax_result = ztax_result.to_dict('records')

        vat_nominals = [r['tx_nominal'].strip() for r in (ztax_result or []) if r.get('tx_nominal')]
        result["vat_nominal_accounts"] = vat_nominals

        # 1. Get uncommitted VAT totals by year/period
        zvtran_by_period_sql = """
            SELECT
                YEAR(va_taxdate) AS year,
                MONTH(va_taxdate) AS month,
                va_vattype AS vat_type,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_total
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
            GROUP BY YEAR(va_taxdate), MONTH(va_taxdate), va_vattype
            ORDER BY year DESC, month DESC, va_vattype
        """
        zvtran_periods = sql_connector.execute_query(zvtran_by_period_sql)
        if hasattr(zvtran_periods, 'to_dict'):
            zvtran_periods = zvtran_periods.to_dict('records')

        result["analysis"]["uncommitted_by_period"] = [{
            "year": int(r.get('year') or 0),
            "month": int(r.get('month') or 0),
            "type": r.get('vat_type', '').strip(),
            "count": int(r.get('transaction_count') or 0),
            "total": round(float(r.get('vat_total') or 0), 2)
        } for r in (zvtran_periods or [])]

        # 2. Get nominal ledger VAT movements by year/period
        nl_by_period = []
        for acnt in vat_nominals:
            nl_period_sql = f"""
                SELECT
                    nt_year AS year,
                    nt_period AS period,
                    COUNT(*) AS transaction_count,
                    SUM(nt_value) AS total_value
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                GROUP BY nt_year, nt_period
                ORDER BY nt_year DESC, nt_period DESC
            """
            nl_result = sql_connector.execute_query(nl_period_sql)
            if hasattr(nl_result, 'to_dict'):
                nl_result = nl_result.to_dict('records')

            for r in (nl_result or []):
                nl_by_period.append({
                    "account": acnt,
                    "year": int(r.get('year') or 0),
                    "period": int(r.get('period') or 0),
                    "count": int(r.get('transaction_count') or 0),
                    "total": round(float(r.get('total_value') or 0), 2)
                })

        result["analysis"]["nominal_by_period"] = nl_by_period

        # 3. Get largest uncommitted VAT transactions
        largest_sql = """
            SELECT TOP 100
                va_taxdate,
                va_vattype,
                va_vatval,
                va_trvalue,
                va_anvat
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
            ORDER BY ABS(va_vatval) DESC
        """
        largest_result = sql_connector.execute_query(largest_sql)
        if hasattr(largest_result, 'to_dict'):
            largest_result = largest_result.to_dict('records')

        result["analysis"]["largest_uncommitted"] = [{
            "date": str(r.get('va_taxdate') or ''),
            "type": (r.get('va_vattype') or '').strip(),
            "vat_amount": round(float(r.get('va_vatval') or 0), 2),
            "net_amount": round(float(r.get('va_trvalue') or 0), 2),
            "vat_code": (r.get('va_anvat') or '').strip()
        } for r in (largest_result or [])[:50]]

        # 4. Get nominal ledger entries on VAT accounts - largest transactions
        for acnt in vat_nominals[:2]:  # Check first 2 VAT accounts
            nl_entries_sql = f"""
                SELECT TOP 50
                    nt_entr AS post_date,
                    nt_value,
                    nt_trnref,
                    nt_posttyp,
                    nt_year,
                    nt_period
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                ORDER BY ABS(nt_value) DESC
            """
            nl_entries_result = sql_connector.execute_query(nl_entries_sql)
            if hasattr(nl_entries_result, 'to_dict'):
                nl_entries_result = nl_entries_result.to_dict('records')

            result["analysis"][f"largest_nl_entries_{acnt}"] = [{
                "date": str(r.get('post_date') or ''),
                "value": round(float(r.get('nt_value') or 0), 2),
                "reference": (r.get('nt_trnref') or '').strip()[:40],
                "type": (r.get('nt_posttyp') or '').strip(),
                "year": int(r.get('nt_year') or 0),
                "period": int(r.get('nt_period') or 0)
            } for r in (nl_entries_result or [])]

        # 5. Summary comparison
        # Total uncommitted VAT
        total_uncommitted_sql = """
            SELECT
                SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
                SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total,
                COUNT(*) AS total_records
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        total_uncommitted = sql_connector.execute_query(total_uncommitted_sql)
        if hasattr(total_uncommitted, 'to_dict'):
            total_uncommitted = total_uncommitted.to_dict('records')

        uncommitted_output = float(total_uncommitted[0]['output_total'] or 0) if total_uncommitted else 0
        uncommitted_input = float(total_uncommitted[0]['input_total'] or 0) if total_uncommitted else 0
        uncommitted_net = uncommitted_output - uncommitted_input
        uncommitted_count = int(total_uncommitted[0]['total_records'] or 0) if total_uncommitted else 0

        # Total nominal balance on VAT accounts
        nl_total = 0
        nl_count = 0
        for acnt in vat_nominals:
            nl_sum_sql = f"""
                SELECT SUM(nt_value) AS total, COUNT(*) AS cnt
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
            """
            nl_sum = sql_connector.execute_query(nl_sum_sql)
            if hasattr(nl_sum, 'to_dict'):
                nl_sum = nl_sum.to_dict('records')
            if nl_sum and nl_sum[0]:
                nl_total += float(nl_sum[0]['total'] or 0)
                nl_count += int(nl_sum[0]['cnt'] or 0)

        # VAT liability is typically credit, so negate for comparison
        nl_balance = -nl_total

        result["summary"] = {
            "uncommitted_vat": {
                "output": round(uncommitted_output, 2),
                "input": round(uncommitted_input, 2),
                "net": round(uncommitted_net, 2),
                "record_count": uncommitted_count
            },
            "nominal_balance": {
                "total": round(nl_balance, 2),
                "record_count": nl_count
            },
            "variance": round(uncommitted_net - nl_balance, 2),
            "variance_explanation": []
        }

        # Add variance explanations
        variance = uncommitted_net - nl_balance
        if abs(variance) > 1:
            if variance > 0:
                result["summary"]["variance_explanation"].append(
                    f"Uncommitted VAT is £{variance:,.2f} MORE than nominal balance"
                )
                result["summary"]["variance_explanation"].append(
                    "Possible causes: VAT transactions not posted to nominal, or nominal entries reversed"
                )
            else:
                result["summary"]["variance_explanation"].append(
                    f"Uncommitted VAT is £{abs(variance):,.2f} LESS than nominal balance"
                )
                result["summary"]["variance_explanation"].append(
                    "Possible causes: Nominal entries without zvtran records, or VAT returns processed but marked done"
                )

        return result

    except Exception as e:
        logger.error(f"VAT variance drilldown failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@router.get("/api/reconcile/vat")
async def reconcile_vat():
    """
    Reconcile VAT accounts - compare VAT liability in nominal ledger to VAT transactions.
    Enhanced for quarterly VAT tracking with uncommitted transactions from zvtran.
    Shows output VAT (sales), input VAT (purchases), and net liability for current quarter.
    """
    from api.main import sql_connector
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # First, find the most recent uncommitted VAT transaction date to determine the relevant quarter
        # This handles cases where the database has data from a different year than the current calendar date
        recent_vat_date_sql = """
            SELECT MAX(va_taxdate) AS most_recent_date
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        recent_vat_result = sql_connector.execute_query(recent_vat_date_sql)
        if hasattr(recent_vat_result, 'to_dict'):
            recent_vat_result = recent_vat_result.to_dict('records')

        # Use the most recent uncommitted VAT date as reference, or fallback to current date
        reference_date = None
        if recent_vat_result and recent_vat_result[0] and recent_vat_result[0].get('most_recent_date'):
            reference_date = recent_vat_result[0]['most_recent_date']
            if isinstance(reference_date, str):
                reference_date = datetime.strptime(reference_date, '%Y-%m-%d')

        quarter_info = get_vat_quarter_dates(reference_date)

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "quarter_info": quarter_info,
            "vat_codes": [],
            "current_quarter": {
                "output_vat": {},
                "input_vat": {},
                "uncommitted": {},
                "nominal_movements": {}
            },
            "year_to_date": {
                "output_vat": {},
                "input_vat": {},
                "nominal_accounts": []
            },
            "variance": {},
            "status": "UNRECONCILED",
            "message": ""
        }

        # Get current year from ntran - use NOLOCK to avoid locking
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # If no uncommitted VAT found, also check nvat for the most recent date
        if reference_date is None:
            nvat_date_sql = """
                SELECT MAX(nv_date) AS most_recent_date
                FROM nvat WITH (NOLOCK)
            """
            nvat_date_result = sql_connector.execute_query(nvat_date_sql)
            if hasattr(nvat_date_result, 'to_dict'):
                nvat_date_result = nvat_date_result.to_dict('records')
            if nvat_date_result and nvat_date_result[0] and nvat_date_result[0].get('most_recent_date'):
                reference_date = nvat_date_result[0]['most_recent_date']
                if isinstance(reference_date, str):
                    reference_date = datetime.strptime(reference_date, '%Y-%m-%d')
                # Recalculate quarter_info with the nvat date
                quarter_info = get_vat_quarter_dates(reference_date)
                reconciliation["quarter_info"] = quarter_info

        # Get VAT codes from ztax with date-based rate calculation
        ztax_sql = """
            SELECT tx_code, tx_desc, tx_rate1, tx_rate1dy, tx_rate2, tx_rate2dy, tx_trantyp, tx_nominal
            FROM ztax WITH (NOLOCK)
            WHERE tx_ctrytyp = 'H'
            ORDER BY tx_trantyp, tx_code
        """
        ztax_result = sql_connector.execute_query(ztax_sql)
        if hasattr(ztax_result, 'to_dict'):
            ztax_result = ztax_result.to_dict('records')

        vat_codes = []
        output_nominal_accounts = set()
        input_nominal_accounts = set()
        ref_date = datetime.now().date()

        for row in ztax_result or []:
            code = row['tx_code'].strip() if row['tx_code'] else ''
            nominal = row['tx_nominal'].strip() if row['tx_nominal'] else ''
            vat_type = row['tx_trantyp'].strip() if row['tx_trantyp'] else ''

            # Calculate applicable rate based on today's date
            rate1 = float(row['tx_rate1'] or 0)
            rate2 = float(row['tx_rate2'] or 0)
            date1 = row.get('tx_rate1dy')
            date2 = row.get('tx_rate2dy')

            # Convert dates if needed, handle NaT/None/NaN
            try:
                if date1 is not None and date1 == date1:  # NaT/NaN check (NaN != NaN)
                    if hasattr(date1, 'date'):
                        date1 = date1.date()
                else:
                    date1 = None
            except (TypeError, ValueError):
                date1 = None

            try:
                if date2 is not None and date2 == date2:  # NaT/NaN check
                    if hasattr(date2, 'date'):
                        date2 = date2.date()
                else:
                    date2 = None
            except (TypeError, ValueError):
                date2 = None

            # Determine applicable rate (most recent effective date <= today)
            applicable_rate = rate1
            if date1 and date2:
                if date2 <= ref_date and date1 <= ref_date:
                    applicable_rate = rate2 if date2 > date1 else rate1
                elif date2 <= ref_date:
                    applicable_rate = rate2
                elif date1 <= ref_date:
                    applicable_rate = rate1
            elif date2 and date2 <= ref_date:
                applicable_rate = rate2

            vat_codes.append({
                "code": code,
                "description": row['tx_desc'].strip() if row['tx_desc'] else '',
                "rate": applicable_rate,
                "type": vat_type,  # 'S' = Sales/Output, 'P' = Purchase/Input
                "nominal_account": nominal
            })
            if nominal:
                if vat_type == 'S':
                    output_nominal_accounts.add(nominal)
                elif vat_type == 'P':
                    input_nominal_accounts.add(nominal)

        reconciliation["vat_codes"] = vat_codes

        quarter_start = quarter_info['quarter_start']
        quarter_end = quarter_info['quarter_end']

        # ==========================================
        # CURRENT QUARTER - Uncommitted VAT (zvtran)
        # ==========================================

        # Get uncommitted Output VAT from zvtran (va_done = 0, va_vattype = 'S')
        uncommitted_output_sql = f"""
            SELECT
                va_anvat AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_amount,
                SUM(va_trvalue) AS net_amount
            FROM zvtran WITH (NOLOCK)
            WHERE va_vattype = 'S'
              AND va_done = 0
              AND va_taxdate >= '{quarter_start}'
              AND va_taxdate <= '{quarter_end}'
            GROUP BY va_anvat
            ORDER BY va_anvat
        """
        uncommitted_output_result = sql_connector.execute_query(uncommitted_output_sql)
        if hasattr(uncommitted_output_result, 'to_dict'):
            uncommitted_output_result = uncommitted_output_result.to_dict('records')

        uncommitted_output_total = 0
        uncommitted_output_by_code = []
        for row in uncommitted_output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            uncommitted_output_total += vat_amount
            uncommitted_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2),
                "net_amount": round(float(row['net_amount'] or 0), 2)
            })

        # Get uncommitted Input VAT from zvtran (va_done = 0, va_vattype = 'P')
        uncommitted_input_sql = f"""
            SELECT
                va_anvat AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_amount,
                SUM(va_trvalue) AS net_amount
            FROM zvtran WITH (NOLOCK)
            WHERE va_vattype = 'P'
              AND va_done = 0
              AND va_taxdate >= '{quarter_start}'
              AND va_taxdate <= '{quarter_end}'
            GROUP BY va_anvat
            ORDER BY va_anvat
        """
        uncommitted_input_result = sql_connector.execute_query(uncommitted_input_sql)
        if hasattr(uncommitted_input_result, 'to_dict'):
            uncommitted_input_result = uncommitted_input_result.to_dict('records')

        uncommitted_input_total = 0
        uncommitted_input_by_code = []
        for row in uncommitted_input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            uncommitted_input_total += vat_amount
            uncommitted_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2),
                "net_amount": round(float(row['net_amount'] or 0), 2)
            })

        uncommitted_net = uncommitted_output_total - uncommitted_input_total

        reconciliation["current_quarter"]["uncommitted"] = {
            "source": "zvtran (VAT Return Transactions - va_done=0)",
            "quarter": quarter_info['current_quarter'],
            "period_start": quarter_start,
            "period_end": quarter_end,
            "output_vat": {
                "total": round(uncommitted_output_total, 2),
                "by_code": uncommitted_output_by_code
            },
            "input_vat": {
                "total": round(uncommitted_input_total, 2),
                "by_code": uncommitted_input_by_code
            },
            "net_liability": round(uncommitted_net, 2),
            "description": "VAT transactions not yet submitted in a VAT return"
        }

        # ==========================================
        # CURRENT QUARTER - NL Movements
        # ==========================================

        # Get quarter nominal ledger movements for VAT accounts
        all_vat_nominals = output_nominal_accounts.union(input_nominal_accounts)
        quarter_nl_movements = []
        quarter_nl_output_total = 0
        quarter_nl_input_total = 0

        for acnt in all_vat_nominals:
            ntran_quarter_sql = f"""
                SELECT
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                    SUM(nt_value) AS net,
                    COUNT(*) AS transaction_count
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                  AND nt_entr >= '{quarter_start}'
                  AND nt_entr <= '{quarter_end}'
            """
            ntran_quarter_result = sql_connector.execute_query(ntran_quarter_sql)
            if hasattr(ntran_quarter_result, 'to_dict'):
                ntran_quarter_result = ntran_quarter_result.to_dict('records')

            if ntran_quarter_result and ntran_quarter_result[0]:
                row = ntran_quarter_result[0]
                debits = float(row['debits'] or 0)
                credits = float(row['credits'] or 0)
                net = float(row['net'] or 0)
                txn_count = int(row['transaction_count'] or 0)

                is_output = acnt in output_nominal_accounts
                is_input = acnt in input_nominal_accounts

                # Get account description
                nacnt_sql = f"SELECT RTRIM(na_desc) AS description FROM nacnt WITH (NOLOCK) WHERE na_acnt = '{acnt}'"
                nacnt_result = sql_connector.execute_query(nacnt_sql)
                if hasattr(nacnt_result, 'to_dict'):
                    nacnt_result = nacnt_result.to_dict('records')
                description = nacnt_result[0]['description'] if nacnt_result else ''

                if txn_count > 0:
                    quarter_nl_movements.append({
                        "account": acnt,
                        "description": description,
                        "type": "Output" if is_output else ("Input" if is_input else "Mixed"),
                        "debits": round(debits, 2),
                        "credits": round(credits, 2),
                        "net": round(net, 2),
                        "transaction_count": txn_count
                    })

                    # For Output VAT, credits increase liability
                    if is_output:
                        quarter_nl_output_total += credits
                    # For Input VAT, debits represent reclaimable VAT
                    if is_input:
                        quarter_nl_input_total += debits

        reconciliation["current_quarter"]["nominal_movements"] = {
            "source": "ntran (Nominal Ledger)",
            "quarter": quarter_info['current_quarter'],
            "period_start": quarter_start,
            "period_end": quarter_end,
            "accounts": quarter_nl_movements,
            "output_vat_total": round(quarter_nl_output_total, 2),
            "input_vat_total": round(quarter_nl_input_total, 2),
            "net_movement": round(quarter_nl_output_total - quarter_nl_input_total, 2)
        }

        # ==========================================
        # CURRENT QUARTER - nvat transactions
        # ==========================================

        # Get quarter Output VAT from nvat
        quarter_output_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'S'
              AND nv_date >= '{quarter_start}'
              AND nv_date <= '{quarter_end}'
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        quarter_output_result = sql_connector.execute_query(quarter_output_sql)
        if hasattr(quarter_output_result, 'to_dict'):
            quarter_output_result = quarter_output_result.to_dict('records')

        quarter_output_total = 0
        quarter_output_by_code = []
        for row in quarter_output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            quarter_output_total += vat_amount
            quarter_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["current_quarter"]["output_vat"] = {
            "source": "nvat (VAT Transactions - Sales/Output)",
            "total_vat": round(quarter_output_total, 2),
            "by_code": quarter_output_by_code,
            "quarter": quarter_info['current_quarter']
        }

        # Get quarter Input VAT from nvat
        quarter_input_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'P'
              AND nv_date >= '{quarter_start}'
              AND nv_date <= '{quarter_end}'
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        quarter_input_result = sql_connector.execute_query(quarter_input_sql)
        if hasattr(quarter_input_result, 'to_dict'):
            quarter_input_result = quarter_input_result.to_dict('records')

        quarter_input_total = 0
        quarter_input_by_code = []
        for row in quarter_input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            quarter_input_total += vat_amount
            quarter_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["current_quarter"]["input_vat"] = {
            "source": "nvat (VAT Transactions - Purchase/Input)",
            "total_vat": round(quarter_input_total, 2),
            "by_code": quarter_input_by_code,
            "quarter": quarter_info['current_quarter']
        }

        # ==========================================
        # YEAR TO DATE - nvat totals
        # ==========================================

        # Get YTD Output VAT (Sales) from nvat
        output_vat_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'S' AND YEAR(nv_date) = {current_year}
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        output_result = sql_connector.execute_query(output_vat_sql)
        if hasattr(output_result, 'to_dict'):
            output_result = output_result.to_dict('records')

        ytd_output_total = 0
        ytd_output_by_code = []
        for row in output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            ytd_output_total += vat_amount
            ytd_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["year_to_date"]["output_vat"] = {
            "source": "nvat (VAT Transactions - Sales/Output)",
            "total_vat": round(ytd_output_total, 2),
            "by_code": ytd_output_by_code,
            "current_year": current_year
        }

        # Get YTD Input VAT (Purchases) from nvat
        input_vat_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'P' AND YEAR(nv_date) = {current_year}
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        input_result = sql_connector.execute_query(input_vat_sql)
        if hasattr(input_result, 'to_dict'):
            input_result = input_result.to_dict('records')

        ytd_input_total = 0
        ytd_input_by_code = []
        for row in input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            ytd_input_total += vat_amount
            ytd_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["year_to_date"]["input_vat"] = {
            "source": "nvat (VAT Transactions - Purchase/Input)",
            "total_vat": round(ytd_input_total, 2),
            "by_code": ytd_input_by_code,
            "current_year": current_year
        }

        # ==========================================
        # YEAR TO DATE - Nominal accounts
        # ==========================================

        nominal_accounts = []
        nl_total = 0

        for acnt in all_vat_nominals:
            nacnt_sql = f"""
                SELECT na_acnt, RTRIM(na_desc) AS description, na_ytddr, na_ytdcr, na_prydr, na_prycr
                FROM nacnt WITH (NOLOCK)
                WHERE na_acnt = '{acnt}'
            """
            nacnt_result = sql_connector.execute_query(nacnt_sql)
            if hasattr(nacnt_result, 'to_dict'):
                nacnt_result = nacnt_result.to_dict('records')

            if nacnt_result:
                acc = nacnt_result[0]
                ytd_dr = float(acc['na_ytddr'] or 0)
                ytd_cr = float(acc['na_ytdcr'] or 0)
                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                bf_balance = pry_cr - pry_dr

                # Get current year transactions
                ntran_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                current_year_dr = float(ntran_result[0]['debits'] or 0) if ntran_result else 0
                current_year_cr = float(ntran_result[0]['credits'] or 0) if ntran_result else 0
                current_year_net = float(ntran_result[0]['net'] or 0) if ntran_result else 0

                # VAT liability is typically a credit balance (negative in accounting convention)
                closing_balance = -current_year_net

                is_output = acnt in output_nominal_accounts
                is_input = acnt in input_nominal_accounts

                nominal_accounts.append({
                    "account": acnt,
                    "description": acc['description'] or '',
                    "type": "Output" if is_output else ("Input" if is_input else "Mixed"),
                    "brought_forward": round(bf_balance, 2),
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(closing_balance, 2)
                })

                nl_total += closing_balance

        reconciliation["year_to_date"]["nominal_accounts"] = {
            "source": "ntran (Nominal Ledger)",
            "accounts": nominal_accounts,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ==========================================
        # VAT ACCOUNT BALANCE & BANK TRANSACTIONS
        # ==========================================

        # Get total uncommitted VAT (ALL uncommitted, not just current quarter)
        total_uncommitted_sql = """
            SELECT
                SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
                SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        total_uncommitted_result = sql_connector.execute_query(total_uncommitted_sql)
        if hasattr(total_uncommitted_result, 'to_dict'):
            total_uncommitted_result = total_uncommitted_result.to_dict('records')

        total_uncommitted_output = float(total_uncommitted_result[0]['output_total'] or 0) if total_uncommitted_result else 0
        total_uncommitted_input = float(total_uncommitted_result[0]['input_total'] or 0) if total_uncommitted_result else 0
        total_uncommitted_net = total_uncommitted_output - total_uncommitted_input

        # Get current VAT nominal account balance
        vat_account_balance = 0
        for acnt in all_vat_nominals:
            balance_sql = f"""
                SELECT SUM(nt_value) AS balance
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
            """
            balance_result = sql_connector.execute_query(balance_sql)
            if hasattr(balance_result, 'to_dict'):
                balance_result = balance_result.to_dict('records')
            if balance_result and balance_result[0]['balance']:
                # VAT liability is typically credit (negative), so we negate
                vat_account_balance += float(balance_result[0]['balance'] or 0)

        # Check for bank/cashbook transactions on VAT accounts
        # These would be VAT payments to HMRC (reducing liability) or refunds
        vat_bank_transactions = []
        vat_bank_total = 0
        for acnt in all_vat_nominals:
            bank_vat_sql = f"""
                SELECT
                    ax_date AS trans_date,
                    ax_value AS amount,
                    ax_tref AS reference,
                    ax_source AS source
                FROM anoml WITH (NOLOCK)
                WHERE ax_nacnt = '{acnt}'
                  AND ax_source = 'A'
                ORDER BY ax_date DESC
            """
            bank_vat_result = sql_connector.execute_query(bank_vat_sql)
            if hasattr(bank_vat_result, 'to_dict'):
                bank_vat_result = bank_vat_result.to_dict('records')

            for row in bank_vat_result or []:
                amount = float(row['amount'] or 0)
                vat_bank_total += amount
                vat_bank_transactions.append({
                    "date": str(row['trans_date'] or ''),
                    "amount": round(amount, 2),
                    "reference": (row['reference'] or '').strip(),
                    "account": acnt
                })

        # The reconciliation formula:
        # Total Uncommitted VAT should = VAT Account Balance + VAT Bank Payments (if payment made but return not processed)
        # Or: VAT Account Balance should = Total Uncommitted VAT - VAT Bank Payments
        adjusted_balance = -vat_account_balance  # Negate because liability is credit
        reconciliation["vat_balance"] = {
            "total_uncommitted_vat": round(total_uncommitted_net, 2),
            "total_uncommitted_output": round(total_uncommitted_output, 2),
            "total_uncommitted_input": round(total_uncommitted_input, 2),
            "vat_account_balance": round(adjusted_balance, 2),
            "vat_bank_transactions": vat_bank_transactions[:10],  # Last 10 transactions
            "vat_bank_total": round(vat_bank_total, 2),
            "expected_balance": round(total_uncommitted_net - vat_bank_total, 2),
            "balance_variance": round(adjusted_balance - (total_uncommitted_net - vat_bank_total), 2),
            "reconciled": abs(adjusted_balance - (total_uncommitted_net - vat_bank_total)) < 1.00
        }

        # ==========================================
        # VARIANCE CALCULATION
        # ==========================================

        # Primary reconciliation: Uncommitted VAT (zvtran) vs NL quarter movements
        quarter_variance = uncommitted_net - (quarter_nl_output_total - quarter_nl_input_total)
        quarter_variance_abs = abs(quarter_variance)

        # YTD reconciliation: nvat totals vs NL balances
        ytd_net_vat = ytd_output_total - ytd_input_total
        ytd_variance = ytd_net_vat - nl_total
        ytd_variance_abs = abs(ytd_variance)

        reconciliation["variance"] = {
            "quarter": {
                "uncommitted_output": round(uncommitted_output_total, 2),
                "uncommitted_input": round(uncommitted_input_total, 2),
                "uncommitted_net": round(uncommitted_net, 2),
                "nl_output_movement": round(quarter_nl_output_total, 2),
                "nl_input_movement": round(quarter_nl_input_total, 2),
                "nl_net_movement": round(quarter_nl_output_total - quarter_nl_input_total, 2),
                "variance_amount": round(quarter_variance, 2),
                "variance_absolute": round(quarter_variance_abs, 2),
                "reconciled": quarter_variance_abs < 1.00
            },
            "year_to_date": {
                "nvat_output_total": round(ytd_output_total, 2),
                "nvat_input_total": round(ytd_input_total, 2),
                "nvat_net_liability": round(ytd_net_vat, 2),
                "nominal_ledger_balance": round(nl_total, 2),
                "variance_amount": round(ytd_variance, 2),
                "variance_absolute": round(ytd_variance_abs, 2),
                "reconciled": ytd_variance_abs < 1.00
            }
        }

        # Overall status based on quarter reconciliation (primary focus)
        if quarter_variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) reconciles to NL movements"
        else:
            reconciliation["status"] = "VARIANCE"
            if quarter_variance > 0:
                reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) is £{quarter_variance_abs:,.2f} MORE than NL movements"
            else:
                reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) is £{quarter_variance_abs:,.2f} LESS than NL movements"

        return reconciliation

    except Exception as e:
        logger.error(f"VAT reconciliation failed: {e}")
        return {"success": False, "error": str(e)}



# ============================================================
# Opera 3 Balance Check Endpoints
# ============================================================

@router.get("/api/opera3/reconcile/debtors")
async def opera3_reconcile_debtors(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    debtors_control: str = Query("", description="Debtors control account code (looked up from config if empty)")
):
    """
    Reconcile Sales Ledger to Debtors Control Account from Opera 3 FoxPro data.
    Mirrors /api/reconcile/debtors but reads from DBF files.
    """
    try:
        # Look up control account from Opera 3 config if not provided
        if not debtors_control:
            try:
                from sql_rag.opera3_config import Opera3Config
                config = Opera3Config(data_path)
                controls = config.get_control_accounts()
                debtors_control = controls.debtors_control
            except Exception as e:
                logger.warning(f"Could not look up debtors control from Opera 3 config: {e}")
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        reconciliation = provider.get_debtors_reconciliation(debtors_control)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            **reconciliation
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 debtors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/reconcile/creditors")
async def opera3_reconcile_creditors(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    creditors_control: str = Query("", description="Creditors control account code (looked up from config if empty)")
):
    """
    Reconcile Purchase Ledger to Creditors Control Account from Opera 3 FoxPro data.
    Mirrors /api/reconcile/creditors but reads from DBF files.
    """
    try:
        # Look up control account from Opera 3 config if not provided
        if not creditors_control:
            try:
                from sql_rag.opera3_config import Opera3Config
                config = Opera3Config(data_path)
                controls = config.get_control_accounts()
                creditors_control = controls.creditors_control
            except Exception as e:
                logger.warning(f"Could not look up creditors control from Opera 3 config: {e}")
        from api.main import _get_opera3_provider
        provider = _get_opera3_provider(data_path)
        reconciliation = provider.get_creditors_reconciliation(creditors_control)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            **reconciliation
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/reconcile/trial-balance")
async def opera3_reconcile_trial_balance(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Trial Balance check for Opera 3 - verifies the nominal ledger as a whole balances.
    Mirrors /api/reconcile/trial-balance.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        nacnt_records = reader.read_table('nacnt')

        type_names = {
            'A': 'Asset', 'L': 'Liability', 'E': 'Expense',
            'I': 'Income', 'C': 'Capital', 'P': 'P&L', 'B': 'Balance Sheet'
        }

        total_bf_debits = 0
        total_bf_credits = 0
        total_ytd_debits = 0
        total_ytd_credits = 0
        total_closing_debits = 0
        total_closing_credits = 0

        accounts = []
        for row in nacnt_records:
            pry_dr = float(row.get('na_prydr', 0) or 0)
            pry_cr = float(row.get('na_prycr', 0) or 0)
            ytd_dr = float(row.get('na_ytddr', 0) or 0)
            ytd_cr = float(row.get('na_ytdcr', 0) or 0)

            if pry_dr == 0 and pry_cr == 0 and ytd_dr == 0 and ytd_cr == 0:
                continue

            bf_balance = pry_dr - pry_cr
            current_net = ytd_dr - ytd_cr
            closing_balance = bf_balance + current_net

            if bf_balance > 0:
                total_bf_debits += bf_balance
            else:
                total_bf_credits += abs(bf_balance)

            total_ytd_debits += ytd_dr
            total_ytd_credits += ytd_cr

            if closing_balance > 0:
                total_closing_debits += closing_balance
            else:
                total_closing_credits += abs(closing_balance)

            account_type = (row.get('na_type') or '').strip()
            accounts.append({
                "account": (row.get('na_acnt') or '').strip(),
                "description": (row.get('na_desc') or '').strip(),
                "type": account_type,
                "type_name": type_names.get(account_type, account_type),
                "bf_balance": round(bf_balance, 2),
                "current_debits": round(ytd_dr, 2),
                "current_credits": round(ytd_cr, 2),
                "current_net": round(current_net, 2),
                "closing_balance": round(closing_balance, 2)
            })

        bf_variance = abs(total_bf_debits - total_bf_credits)
        current_variance = abs(total_ytd_debits - total_ytd_credits)
        closing_variance = abs(total_closing_debits - total_closing_credits)
        all_balanced = bf_variance < 1.00 and current_variance < 1.00 and closing_variance < 1.00

        result = {
            "success": True,
            "source": "opera3",
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "accounts": accounts,
            "summary": {
                "brought_forward": {
                    "debits": round(total_bf_debits, 2),
                    "credits": round(total_bf_credits, 2),
                    "variance": round(bf_variance, 2),
                    "balanced": bf_variance < 1.00
                },
                "current_year": {
                    "debits": round(total_ytd_debits, 2),
                    "credits": round(total_ytd_credits, 2),
                    "variance": round(current_variance, 2),
                    "balanced": current_variance < 1.00
                },
                "closing": {
                    "debits": round(total_closing_debits, 2),
                    "credits": round(total_closing_credits, 2),
                    "variance": round(closing_variance, 2),
                    "balanced": closing_variance < 1.00
                },
                "account_count": len(accounts)
            },
            "status": "BALANCED" if all_balanced else "UNBALANCED",
            "message": f"Trial Balance is correct. {len(accounts)} accounts with matching debits and credits." if all_balanced else "Trial Balance has variance."
        }

        return result

    except Exception as e:
        logger.error(f"Opera 3 trial balance check failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/reconcile/summary")
async def opera3_reconcile_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Quick summary of all reconciliation checks for Opera 3.
    Mirrors /api/reconcile/summary.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from sql_rag.opera3_config import Opera3Config
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)

        summary = {
            "success": True,
            "source": "opera3",
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "checks": [],
            "all_reconciled": True,
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0
        }

        # Get control accounts
        try:
            config_obj = Opera3Config(data_path)
            controls = config_obj.get_control_accounts()
            debtors_control = controls.debtors_control
            creditors_control = controls.creditors_control
        except Exception:
            debtors_control = ''
            creditors_control = ''

        # ========== 1. DEBTORS CHECK ==========
        try:
            stran_records = reader.read_table('stran')
            sname_records = reader.read_table('sname')
            nacnt_records = reader.read_table('nacnt')

            sl_total = sum(float(r.get('st_trbal', 0) or 0) for r in stran_records if float(r.get('st_trbal', 0) or 0) != 0)
            sname_total = sum(float(r.get('sn_currbal', 0) or 0) for r in sname_records if float(r.get('sn_currbal', 0) or 0) != 0)

            nl_debtors_total = 0
            if debtors_control:
                for acc in nacnt_records:
                    if (acc.get('na_acnt') or '').strip().upper() == debtors_control.upper():
                        nl_debtors_total = float(acc.get('na_ytddr', 0) or 0) - float(acc.get('na_ytdcr', 0) or 0)
                        break

            sl_vs_sname = abs(sl_total - sname_total)
            sl_vs_nl = abs(sl_total - nl_debtors_total)
            debtors_ok = sl_vs_sname < 1.00 and sl_vs_nl < 1.00

            summary["checks"].append({
                "name": "Debtors",
                "icon": "users",
                "reconciled": debtors_ok,
                "details": [
                    {"label": "Sales Ledger (stran)", "value": round(sl_total, 2)},
                    {"label": "Customer Master (sname)", "value": round(sname_total, 2)},
                    {"label": f"Nominal ({debtors_control})", "value": round(nl_debtors_total, 2)},
                ],
                "variances": [
                    {"label": "SL vs Master", "value": round(sl_vs_sname, 2), "ok": sl_vs_sname < 1.00},
                    {"label": "SL vs NL", "value": round(sl_vs_nl, 2), "ok": sl_vs_nl < 1.00},
                ]
            })
        except Exception as e:
            summary["checks"].append({"name": "Debtors", "icon": "users", "reconciled": False, "error": str(e)})

        # ========== 2. CREDITORS CHECK ==========
        try:
            ptran_records = reader.read_table('ptran')
            pname_records = reader.read_table('pname')

            pl_total = sum(float(r.get('pt_trbal', 0) or 0) for r in ptran_records if float(r.get('pt_trbal', 0) or 0) != 0)
            pname_total = sum(float(r.get('pn_currbal', 0) or 0) for r in pname_records if float(r.get('pn_currbal', 0) or 0) != 0)

            nl_creditors_total = 0
            if creditors_control:
                for acc in nacnt_records:
                    if (acc.get('na_acnt') or '').strip().upper() == creditors_control.upper():
                        nl_creditors_total = -(float(acc.get('na_ytddr', 0) or 0) - float(acc.get('na_ytdcr', 0) or 0))
                        break

            pl_vs_pname = abs(pl_total - pname_total)
            pl_vs_nl = abs(pl_total - nl_creditors_total)
            creditors_ok = pl_vs_pname < 1.00 and pl_vs_nl < 1.00

            summary["checks"].append({
                "name": "Creditors",
                "icon": "building",
                "reconciled": creditors_ok,
                "details": [
                    {"label": "Purchase Ledger (ptran)", "value": round(pl_total, 2)},
                    {"label": "Supplier Master (pname)", "value": round(pname_total, 2)},
                    {"label": f"Nominal ({creditors_control})", "value": round(nl_creditors_total, 2)},
                ],
                "variances": [
                    {"label": "PL vs Master", "value": round(pl_vs_pname, 2), "ok": pl_vs_pname < 1.00},
                    {"label": "PL vs NL", "value": round(pl_vs_nl, 2), "ok": pl_vs_nl < 1.00},
                ]
            })
        except Exception as e:
            summary["checks"].append({"name": "Creditors", "icon": "building", "reconciled": False, "error": str(e)})

        # ========== 3. CASHBOOK CHECK ==========
        try:
            nbank_records = reader.read_table('nbank')

            bank_master_total = 0
            nl_bank_total = 0
            for bank in nbank_records:
                bank_code = (bank.get('nk_acnt') or '').strip()
                master_bal = float(bank.get('nk_curbal', 0) or 0) / 100.0
                bank_master_total += master_bal

                for acc in nacnt_records:
                    if (acc.get('na_acnt') or '').strip().upper() == bank_code.upper():
                        nl_bal = float(acc.get('na_ytddr', 0) or 0) - float(acc.get('na_ytdcr', 0) or 0)
                        nl_bank_total += nl_bal
                        break

            bank_variance = abs(bank_master_total - nl_bank_total)
            cashbook_ok = bank_variance < 1.00

            summary["checks"].append({
                "name": "Cashbook",
                "icon": "book",
                "reconciled": cashbook_ok,
                "details": [
                    {"label": "Bank Master (nbank)", "value": round(bank_master_total, 2)},
                    {"label": "Nominal Ledger", "value": round(nl_bank_total, 2)},
                ],
                "variances": [
                    {"label": "Bank vs NL", "value": round(bank_variance, 2), "ok": cashbook_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({"name": "Cashbook", "icon": "book", "reconciled": False, "error": str(e)})

        # Calculate overall status
        summary["total_checks"] = len(summary["checks"])
        summary["passed_checks"] = sum(1 for c in summary["checks"] if c.get("reconciled", False))
        summary["failed_checks"] = summary["total_checks"] - summary["passed_checks"]
        summary["all_reconciled"] = summary["failed_checks"] == 0

        return summary

    except Exception as e:
        logger.error(f"Opera 3 reconciliation summary failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/reconcile/vat")
async def opera3_reconcile_vat(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    VAT reconciliation for Opera 3.
    Mirrors /api/reconcile/vat.
    Reads zvtran and ztax tables from FoxPro to analyse VAT positions.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)

        # Read VAT-related tables
        try:
            zvtran_records = reader.read_table('zvtran')
        except Exception:
            zvtran_records = []
        try:
            ztax_records = reader.read_table('ztax')
        except Exception:
            ztax_records = []
        try:
            nvat_records = reader.read_table('nvat')
        except Exception:
            nvat_records = []

        # Summarise uncommitted VAT from zvtran
        output_total = 0.0
        input_total = 0.0
        output_count = 0
        input_count = 0

        for row in zvtran_records:
            done = (row.get('va_done', 0) or 0)
            if done == 1:
                continue
            vat_type = (row.get('va_vattype') or '').strip()
            vat_val = float(row.get('va_vatval', 0) or 0)

            if vat_type == 'S':
                output_total += vat_val
                output_count += 1
            elif vat_type == 'P':
                input_total += vat_val
                input_count += 1

        net_liability = output_total - input_total

        # VAT codes from ztax
        vat_codes = []
        for row in ztax_records:
            ctry = (row.get('tx_ctrytyp') or '').strip()
            if ctry != 'H':
                continue
            vat_codes.append({
                "code": (row.get('tx_code') or '').strip(),
                "description": (row.get('tx_desc') or '').strip(),
                "rate": float(row.get('tx_rate1', 0) or 0),
                "type": (row.get('tx_trantyp') or '').strip(),
                "nominal_account": (row.get('tx_nominal') or '').strip()
            })

        return {
            "success": True,
            "source": "opera3",
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "vat_codes": vat_codes,
            "current_quarter": {
                "uncommitted": {
                    "source": "zvtran (VAT Return Transactions - va_done=0)",
                    "output_vat": {
                        "total": round(output_total, 2),
                        "transaction_count": output_count
                    },
                    "input_vat": {
                        "total": round(input_total, 2),
                        "transaction_count": input_count
                    },
                    "net_liability": round(net_liability, 2)
                }
            },
            "status": "RECONCILED" if abs(net_liability) < 1.00 else "DATA_AVAILABLE",
            "message": f"Uncommitted VAT: Output £{output_total:,.2f}, Input £{input_total:,.2f}, Net £{net_liability:,.2f}"
        }

    except Exception as e:
        logger.error(f"Opera 3 VAT reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/opera3/reconcile/vat/diagnostic")
async def opera3_vat_diagnostic(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    VAT diagnostic for Opera 3.
    Mirrors /api/reconcile/vat/diagnostic.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        result = {"tables": {}}

        try:
            zvtran_records = reader.read_table('zvtran')
            total = len(zvtran_records)
            uncommitted = sum(1 for r in zvtran_records if (r.get('va_done', 0) or 0) == 0)
            committed = total - uncommitted
            total_vat = sum(float(r.get('va_vatval', 0) or 0) for r in zvtran_records)
            result["tables"]["zvtran"] = {
                "total_rows": total,
                "uncommitted": uncommitted,
                "committed": committed,
                "total_vat": round(total_vat, 2)
            }
        except Exception as e:
            result["tables"]["zvtran"] = {"error": str(e)}

        try:
            nvat_records = reader.read_table('nvat')
            total_vat = sum(float(r.get('nv_vatval', 0) or 0) for r in nvat_records)
            result["tables"]["nvat"] = {
                "total_rows": len(nvat_records),
                "total_vat": round(total_vat, 2)
            }
        except Exception as e:
            result["tables"]["nvat"] = {"error": str(e)}

        try:
            ztax_records = reader.read_table('ztax')
            home_codes = sum(1 for r in ztax_records if (r.get('tx_ctrytyp') or '').strip() == 'H')
            result["tables"]["ztax"] = {"total_codes": home_codes}
        except Exception as e:
            result["tables"]["ztax"] = {"error": str(e)}

        return result

    except Exception as e:
        return {"error": str(e)}


@router.get("/api/opera3/reconcile/vat/variance-drilldown")
async def opera3_vat_variance_drilldown(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    VAT variance drilldown for Opera 3.
    Mirrors /api/reconcile/vat/variance-drilldown.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)

        try:
            zvtran_records = reader.read_table('zvtran')
        except Exception:
            zvtran_records = []

        # Uncommitted VAT by type
        output_total = 0.0
        input_total = 0.0
        uncommitted_count = 0

        largest_uncommitted = []
        for row in zvtran_records:
            done = (row.get('va_done', 0) or 0)
            if done == 1:
                continue
            uncommitted_count += 1
            vat_type = (row.get('va_vattype') or '').strip()
            vat_val = float(row.get('va_vatval', 0) or 0)

            if vat_type == 'S':
                output_total += vat_val
            elif vat_type == 'P':
                input_total += vat_val

            largest_uncommitted.append({
                "date": str(row.get('va_taxdate') or ''),
                "type": vat_type,
                "vat_amount": round(vat_val, 2),
                "net_amount": round(float(row.get('va_trvalue', 0) or 0), 2),
                "vat_code": (row.get('va_anvat') or '').strip()
            })

        # Sort by absolute value descending
        largest_uncommitted.sort(key=lambda x: abs(x['vat_amount']), reverse=True)

        # Get VAT nominal accounts from ztax
        try:
            ztax_records = reader.read_table('ztax')
        except Exception:
            ztax_records = []

        vat_nominals = set()
        for row in ztax_records:
            if (row.get('tx_ctrytyp') or '').strip() == 'H':
                nominal = (row.get('tx_nominal') or '').strip()
                if nominal:
                    vat_nominals.add(nominal)

        # Get NL balance on VAT accounts
        try:
            nacnt_records = reader.read_table('nacnt')
        except Exception:
            nacnt_records = []

        nl_total = 0.0
        nl_count = 0
        for acc in nacnt_records:
            acnt = (acc.get('na_acnt') or '').strip()
            if acnt in vat_nominals:
                ytd_dr = float(acc.get('na_ytddr', 0) or 0)
                ytd_cr = float(acc.get('na_ytdcr', 0) or 0)
                nl_total += -(ytd_dr - ytd_cr)
                nl_count += 1

        uncommitted_net = output_total - input_total
        variance = uncommitted_net - nl_total

        return {
            "success": True,
            "source": "opera3",
            "vat_nominal_accounts": list(vat_nominals),
            "analysis": {
                "largest_uncommitted": largest_uncommitted[:50]
            },
            "summary": {
                "uncommitted_vat": {
                    "output": round(output_total, 2),
                    "input": round(input_total, 2),
                    "net": round(uncommitted_net, 2),
                    "record_count": uncommitted_count
                },
                "nominal_balance": {
                    "total": round(nl_total, 2),
                    "record_count": nl_count
                },
                "variance": round(variance, 2),
                "variance_explanation": []
            }
        }

    except Exception as e:
        logger.error(f"Opera 3 VAT variance drilldown failed: {e}")
        return {"success": False, "error": str(e)}

