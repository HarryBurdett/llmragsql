"""Pin the F9 audit fix: balance-check handlers share extracted helpers.

Audit cross-cutting F9: the reconcile_creditors (836 lines) and
reconcile_debtors (768 lines) handlers had near-identical sub-ledger
fetch phases. The shared shape is now in
apps.balance_check.logic.sub_ledger_reconcile so both handlers
delegate to the same code.

These tests:
  1. Pin the helper API surface (specs + functions).
  2. Use a fake connector to verify the helpers compose the expected
     SQL and return well-shaped dicts (behaviour preservation).
  3. Source-inspect the handlers to confirm they actually call the
     helpers (so a future edit can't quietly inline the SQL again).
"""
import inspect
import pytest


# -- Helper API -------------------------------------------------------


def test_specs_present_for_both_ledgers():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, DEBTORS,
    )
    # Field names must match Opera schema
    assert CREDITORS.txn_table == 'ptran'
    assert CREDITORS.master_table == 'pname'
    assert CREDITORS.transfer_table == 'pnoml'
    assert DEBTORS.txn_table == 'stran'
    assert DEBTORS.master_table == 'sname'
    assert DEBTORS.transfer_table == 'snoml'


def test_helpers_exist_with_expected_signatures():
    from apps.balance_check.logic import sub_ledger_reconcile as m
    expected = {
        'fetch_outstanding': ['connector', 'spec'],
        'fetch_breakdown_by_type': ['connector', 'spec', 'type_descriptions'],
        'fetch_master_totals': ['connector', 'spec'],
        'fetch_master_txn_variance': ['connector', 'spec'],
        'fetch_transfer_file_pending': ['connector', 'spec'],
        'fetch_transfer_file_summary': ['connector', 'spec'],
    }
    for name, args in expected.items():
        fn = getattr(m, name)
        sig = inspect.signature(fn)
        assert list(sig.parameters.keys()) == args, (
            f"{name} signature changed — broke the wedge contract"
        )


# -- Behaviour-preservation via fake connector ------------------------


class _FakeConnector:
    """Records each SQL query and returns canned rows."""
    def __init__(self, canned):
        self.canned = canned  # list of [{...}] in order of call
        self.queries = []

    def execute_query(self, sql):
        self.queries.append(sql)
        return self.canned.pop(0) if self.canned else []


def test_fetch_outstanding_creditors_shape():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_outstanding,
    )
    conn = _FakeConnector([[
        {'transaction_count': 5, 'total_outstanding': 1234.56},
    ]])
    out = fetch_outstanding(conn, CREDITORS)
    assert out == {'transaction_count': 5, 'total_outstanding': 1234.56}
    # SQL hits the right tables
    assert 'FROM ptran WITH (NOLOCK)' in conn.queries[0]
    assert 'pn_account' in conn.queries[0]
    assert 'pt_trbal' in conn.queries[0]


def test_fetch_outstanding_debtors_shape():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        DEBTORS, fetch_outstanding,
    )
    conn = _FakeConnector([[
        {'transaction_count': 12, 'total_outstanding': -200.0},
    ]])
    out = fetch_outstanding(conn, DEBTORS)
    assert out == {'transaction_count': 12, 'total_outstanding': -200.0}
    assert 'FROM stran WITH (NOLOCK)' in conn.queries[0]
    assert 'sn_account' in conn.queries[0]
    assert 'st_trbal' in conn.queries[0]


def test_fetch_outstanding_handles_empty():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_outstanding,
    )
    conn = _FakeConnector([[]])
    out = fetch_outstanding(conn, CREDITORS)
    assert out == {'transaction_count': 0, 'total_outstanding': 0.0}


def test_fetch_breakdown_decorates_with_descriptions():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_breakdown_by_type,
    )
    conn = _FakeConnector([[
        {'type': 'I', 'count': 3, 'total': 100.0},
        {'type': 'C', 'count': 1, 'total': -50.0},
    ]])
    out = fetch_breakdown_by_type(conn, CREDITORS, {'I': 'Invoices', 'C': 'Credit Notes'})
    assert out == [
        {'type': 'I', 'description': 'Invoices', 'count': 3, 'total': 100.0},
        {'type': 'C', 'description': 'Credit Notes', 'count': 1, 'total': -50.0},
    ]


def test_fetch_master_totals_uses_correct_field():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, DEBTORS, fetch_master_totals,
    )
    conn = _FakeConnector([[{'supplier_count': 4, 'total_balance': 999.0}]])
    fetch_master_totals(conn, CREDITORS)
    assert 'pn_currbal' in conn.queries[0]
    assert 'FROM pname WITH (NOLOCK)' in conn.queries[0]

    conn2 = _FakeConnector([[{'customer_count': 4, 'total_balance': 999.0}]])
    fetch_master_totals(conn2, DEBTORS)
    assert 'sn_currbal' in conn2.queries[0]
    assert 'FROM sname WITH (NOLOCK)' in conn2.queries[0]


def test_fetch_master_txn_variance_returns_per_account_rows():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_master_txn_variance,
    )
    conn = _FakeConnector([[
        {'account': 'ABC001 ', 'name': 'Acme Co', 'master_balance': 100.0,
         'transaction_balance': 95.0, 'variance': 5.0},
    ]])
    out = fetch_master_txn_variance(conn, CREDITORS)
    assert out == [{
        'account': 'ABC001', 'name': 'Acme Co',
        'master_balance': 100.0, 'transaction_balance': 95.0, 'variance': 5.0,
    }]


def test_fetch_transfer_file_pending_creditors_uses_pnoml():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_transfer_file_pending,
    )
    conn = _FakeConnector([[]])
    fetch_transfer_file_pending(conn, CREDITORS)
    assert 'FROM pnoml WITH (NOLOCK)' in conn.queries[0]
    assert 'px_done' in conn.queries[0]


def test_fetch_transfer_file_pending_debtors_uses_snoml():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        DEBTORS, fetch_transfer_file_pending,
    )
    conn = _FakeConnector([[]])
    fetch_transfer_file_pending(conn, DEBTORS)
    assert 'FROM snoml WITH (NOLOCK)' in conn.queries[0]
    assert 'sx_done' in conn.queries[0]


def test_fetch_transfer_file_summary_returns_posted_pending_pair():
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_transfer_file_summary,
    )
    conn = _FakeConnector([[
        {'status': 'Posted', 'count': 10, 'total': 1000.0},
        {'status': 'Pending', 'count': 2, 'total': 200.0},
    ]])
    out = fetch_transfer_file_summary(conn, CREDITORS)
    assert out == {
        'posted': {'count': 10, 'total': 1000.0},
        'pending': {'count': 2, 'total': 200.0},
    }


def test_fetch_transfer_file_summary_defaults_zero_when_status_missing():
    """If a status row is missing (e.g. empty pnoml), the result must
    still have both keys with zeros — the handler unpacks them
    unconditionally."""
    from apps.balance_check.logic.sub_ledger_reconcile import (
        CREDITORS, fetch_transfer_file_summary,
    )
    conn = _FakeConnector([[]])
    out = fetch_transfer_file_summary(conn, CREDITORS)
    assert out == {
        'posted': {'count': 0, 'total': 0.0},
        'pending': {'count': 0, 'total': 0.0},
    }


# -- Pin handlers actually call the helpers ---------------------------


def test_creditors_handler_uses_helpers():
    from apps.balance_check.api import routes
    src = inspect.getsource(routes.reconcile_creditors)
    assert 'fetch_outstanding(sql_connector, CREDITORS)' in src
    assert 'fetch_breakdown_by_type(sql_connector, CREDITORS' in src
    assert 'fetch_master_totals(sql_connector, CREDITORS)' in src
    assert 'fetch_master_txn_variance(sql_connector, CREDITORS)' in src
    assert 'fetch_transfer_file_pending(sql_connector, CREDITORS)' in src
    assert 'fetch_transfer_file_summary(sql_connector, CREDITORS)' in src


def test_debtors_handler_uses_helpers():
    from apps.balance_check.api import routes
    src = inspect.getsource(routes.reconcile_debtors)
    assert 'fetch_outstanding(sql_connector, DEBTORS)' in src
    assert 'fetch_breakdown_by_type(sql_connector, DEBTORS' in src
    assert 'fetch_master_totals(sql_connector, DEBTORS)' in src
    assert 'fetch_master_txn_variance(sql_connector, DEBTORS)' in src
    assert 'fetch_transfer_file_pending(sql_connector, DEBTORS)' in src
    assert 'fetch_transfer_file_summary(sql_connector, DEBTORS)' in src


def test_handler_line_counts_reduced():
    """Pin that the wedge actually shrank the handlers — not just
    moved code with the same overall size.

    Pre-wedge: reconcile_creditors=836 lines, reconcile_debtors=768
    lines. Post-wedge: ~713 / ~639 lines (control-account/NL phase
    still lives inline — that's the next natural seam, deferred so
    this commit stays a small wedge per the user's safety
    constraint).
    """
    from apps.balance_check.api import routes
    cred_src = inspect.getsource(routes.reconcile_creditors)
    debt_src = inspect.getsource(routes.reconcile_debtors)
    cred_lines = cred_src.count('\n')
    debt_lines = debt_src.count('\n')
    assert cred_lines < 800, f"reconcile_creditors still {cred_lines} lines (pre-wedge was 836)"
    assert debt_lines < 700, f"reconcile_debtors still {debt_lines} lines (pre-wedge was 768)"
