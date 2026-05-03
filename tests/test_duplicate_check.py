"""Unit tests for sql_rag.duplicate_check — single-source-of-truth
duplicate detection for bank reconcile."""
from __future__ import annotations

from sql_rag.duplicate_check import (
    DuplicateCheckResult,
    DuplicateMatchKind,
    ACTION_TYPE_MAP,
    DataSource,
)


def test_match_kind_enum_has_required_values():
    expected = {'NONE', 'CASHBOOK_DUPLICATE', 'LEDGER_ALLOCATION_TARGET'}
    actual = {k.name for k in DuplicateMatchKind}
    assert actual == expected


def test_match_kind_value_strings_are_stable():
    """Lock the .value strings — they appear in logs and audit fields."""
    assert DuplicateMatchKind.NONE.value == "none"
    assert DuplicateMatchKind.CASHBOOK_DUPLICATE.value == "cashbook_duplicate"
    assert DuplicateMatchKind.LEDGER_ALLOCATION_TARGET.value == "ledger_allocation_target"


def test_result_dataclass_fields():
    r = DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason="no match",
    )
    assert r.kind is DuplicateMatchKind.NONE
    assert r.matched_table is None
    assert r.matched_entry is None
    assert r.reason == "no match"


def test_action_type_map_covers_all_actions():
    """Every action the bank-import flow can produce must have a mapping."""
    required_actions = {
        'sales_receipt', 'sales_refund',
        'purchase_payment', 'purchase_refund',
        'nominal_payment', 'nominal_receipt',
        'bank_transfer',
    }
    assert required_actions.issubset(set(ACTION_TYPE_MAP.keys()))


def test_action_type_map_has_correct_at_types():
    """The at_type values must match Opera's cashbook conventions
    (CLAUDE.md / opera_knowledge_base.md):
      1=Nominal Pmt, 2=Nominal Rcpt, 3=Sales Refund,
      4=Sales Receipt, 5=Purchase Pmt, 6=Purchase Refund, 8=Bank Transfer
    """
    assert ACTION_TYPE_MAP['nominal_payment']['at_type'] == 1
    assert ACTION_TYPE_MAP['nominal_receipt']['at_type'] == 2
    assert ACTION_TYPE_MAP['sales_refund']['at_type'] == 3
    assert ACTION_TYPE_MAP['sales_receipt']['at_type'] == 4
    assert ACTION_TYPE_MAP['purchase_payment']['at_type'] == 5
    assert ACTION_TYPE_MAP['purchase_refund']['at_type'] == 6
    assert ACTION_TYPE_MAP['bank_transfer']['at_type'] == 8


def test_action_type_map_has_correct_ledger_types():
    """Ledger types per central KB:
      sales_receipt   → stran 'R'
      sales_refund    → stran 'F'
      purchase_payment → ptran 'P'
      purchase_refund → ptran 'F'
      nominal_*, bank_transfer → no ledger row
    """
    assert ACTION_TYPE_MAP['sales_receipt']['st_trtype'] == 'R'
    assert ACTION_TYPE_MAP['sales_refund']['st_trtype'] == 'F'
    assert ACTION_TYPE_MAP['purchase_payment']['pt_trtype'] == 'P'
    assert ACTION_TYPE_MAP['purchase_refund']['pt_trtype'] == 'F'
    # Nominal and bank-transfer actions have no ledger type
    assert ACTION_TYPE_MAP['nominal_payment'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['nominal_payment'].get('pt_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('pt_trtype') is None


def test_datasource_protocol_signatures_pinned():
    import inspect
    sig = inspect.signature(DataSource.find_aentry_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'bank_code', 'date_from', 'date_to',
        'signed_pence', 'expected_at_type', 'exclude_entry_numbers',
    ], f"find_aentry_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_stran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'st_trtype',
    ], f"find_stran_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_ptran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'pt_trtype',
    ], f"find_ptran_by_signed_value signature drifted: {params}"

    class _Good:
        def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                         signed_pence, expected_at_type,
                                         exclude_entry_numbers): return []
        def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, st_trtype): return []
        def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, pt_trtype): return []
    class _Bad:
        def find_aentry_by_signed_value(self, *a, **kw): return []
        # missing the other two
    assert isinstance(_Good(), DataSource)
    assert not isinstance(_Bad(), DataSource)


from datetime import date


class _FakeDataSource:
    """In-memory DataSource for unit tests."""
    def __init__(
        self,
        aentry_results: list[dict] | None = None,
        stran_results: list[dict] | None = None,
        ptran_results: list[dict] | None = None,
    ):
        self._aentry = aentry_results or []
        self._stran = stran_results or []
        self._ptran = ptran_results or []

    def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                     signed_pence, expected_at_type,
                                     exclude_entry_numbers):
        excluded = set(exclude_entry_numbers or [])
        return [
            r for r in self._aentry
            if r.get('ae_entry') not in excluded
        ]

    def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, st_trtype):
        return list(self._stran)

    def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, pt_trtype):
        return list(self._ptran)


def test_no_duplicate_when_nothing_matches():
    """Empty cashbook AND empty ledgers → NONE."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource()
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.NONE
    assert result.matched_entry is None


def test_cashbook_duplicate_when_aentry_of_correct_at_type_exists():
    """A sales_refund (-£198) finds a matching atran with at_type=3."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[{'ae_entry': 'P100000755', 'at_type': 3,
                         'ae_value': -19800}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE
    assert result.matched_table == 'aentry'
    assert result.matched_entry == 'P100000755'


def test_cashbook_duplicate_excludes_consumed_entries():
    """Multi-occurrence: if the matching aentry is in the exclude set,
    it should NOT be returned as a duplicate (the second identical
    bank line should post).
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],  # exclude_entry_numbers makes the lookup empty
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime card purchase",
        reference="",
        exclude_entry_numbers=['P100008190'],
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_cashbook_match_requires_correct_at_type():
    """Sign-blind ABS regression: a -£198 sales_refund must NOT match
    a +£198 sales_receipt even though magnitudes are equal. The
    expected_at_type filter ensures this.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind

    class _StubDS:
        def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                         signed_pence, expected_at_type,
                                         exclude_entry_numbers):
            # The DataSource implementation is responsible for filtering
            # by at_type. If a caller passed at_type=3 (sales_refund),
            # we'd return only at_type=3 rows. Simulate the correct
            # filter behaviour: we received at_type=3 for the search,
            # only the at_type=3 rows are returned.
            assert expected_at_type == 3
            return []  # nothing of at_type=3
        def find_stran_by_signed_value(self, *a, **kw): return []
        def find_ptran_by_signed_value(self, *a, **kw): return []

    result = check_for_duplicate(
        data_source=_StubDS(),
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_unknown_action_raises_value_error():
    """An action not in ACTION_TYPE_MAP must raise — never silently match."""
    from sql_rag.duplicate_check import check_for_duplicate
    import pytest as _pt
    ds = _FakeDataSource()
    with _pt.raises(ValueError, match="not in ACTION_TYPE_MAP"):
        check_for_duplicate(
            data_source=ds,
            bank_code="BB005",
            transaction_date=date(2026, 4, 16),
            signed_amount_pounds=0.0,
            action="totally_made_up_action",
            account_code="X",
            description="",
            reference="",
        )


def test_ledger_allocation_target_for_sales_refund():
    """No cashbook entry, but stran has a type='F' or 'C' row matching the
    refund amount → LEDGER_ALLOCATION_TARGET. The caller should post the
    refund payment, not refuse it.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'CN0001', 'st_trvalue': -198.00,
                        'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_table == 'stran'
    assert result.matched_entry == 'CN0001'


def test_ledger_allocation_target_for_purchase_refund():
    """ptran credit-note-type row matches purchase_refund amount."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        ptran_results=[{'pt_trref': 'CN9999', 'pt_trvalue': 100.00,
                        'pt_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=100.00,
        action="purchase_refund",
        account_code="SUPP1",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_table == 'ptran'
    assert result.matched_entry == 'CN9999'


def test_ledger_advisory_skipped_for_non_refund_actions():
    """sales_receipt, purchase_payment, nominal_*, bank_transfer don't
    consult the ledger — they're authoritatively decided by cashbook.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'X', 'st_trvalue': -50.00, 'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=50.00,
        action="sales_receipt",  # NOT a refund
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_ledger_advisory_requires_account_code():
    """Without account_code we can't query the ledger; result is NONE."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'X', 'st_trvalue': -50.00, 'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-50.00,
        action="sales_refund",
        account_code=None,
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_se_datasource_construction_and_protocol():
    """OperaSEDataSource exists, takes a SQLConnector, satisfies protocol."""
    from sql_rag.duplicate_check_se import OperaSEDataSource
    from sql_rag.duplicate_check import DataSource

    class _StubConn:
        def execute_query(self, q):
            raise NotImplementedError
    ds = OperaSEDataSource(_StubConn())
    assert isinstance(ds, DataSource)


def test_se_datasource_uses_signed_comparison_and_at_type():
    """Smoke test — verify the SQL the SE DataSource emits uses signed
    comparison (`a.at_value - signed_pence`) and a type filter
    (`a.at_type = expected_at_type`). Catches regressions back to
    sign-blind ABS-on-ABS.
    """
    captured_queries: list[str] = []

    class _SpyConn:
        def execute_query(self, q):
            captured_queries.append(q)
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.duplicate_check_se import OperaSEDataSource
    from datetime import date as _date

    ds = OperaSEDataSource(_SpyConn())
    ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=[],
    )
    assert any("ABS(a.at_value - -19800)" in q for q in captured_queries), \
        f"signed comparison not found in queries: {captured_queries}"
    assert any("a.at_type = 3" in q for q in captured_queries), \
        f"at_type filter not found in queries: {captured_queries}"
    # Critical: NO ABS(ABS(...)) — that's the sign-blind regression
    assert not any("ABS(ABS(" in q for q in captured_queries), \
        "sign-blind ABS-on-ABS regression: " + str(captured_queries)


def test_o3_datasource_construction_and_protocol():
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    from sql_rag.duplicate_check import DataSource
    class _Stub:
        def read_table(self, name): return []
    assert isinstance(Opera3DataSource(_Stub()), DataSource)


def test_o3_datasource_filters_aentry_by_bank_at_type_and_signed_value():
    """Verify Opera3DataSource filters aentry rows correctly:
    bank, signed pence (within tolerance), at_type, exclude list, date.
    """
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    from datetime import date as _date

    rows_by_table = {
        'atran': [
            {'at_acnt': 'BB005', 'at_entry': 'P100000755',
             'at_value': -19800, 'at_type': 3,
             'at_pstdate': _date(2026, 4, 16)},
            {'at_acnt': 'BB005', 'at_entry': 'R100000407',
             'at_value': 19800, 'at_type': 4,
             'at_pstdate': _date(2026, 4, 16)},
            {'at_acnt': 'BB005', 'at_entry': 'P100000900',
             'at_value': -19800, 'at_type': 3,
             'at_pstdate': _date(2026, 5, 5)},  # outside window
        ],
        'aentry': [],
    }

    class _Reader:
        def read_table(self, name): return rows_by_table.get(name, [])

    ds = Opera3DataSource(_Reader())
    rows = ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=[],
    )
    assert len(rows) == 1
    assert rows[0]['ae_entry'] == 'P100000755'

    # Now exclude the matching entry — should return empty
    rows = ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=['P100000755'],
    )
    assert rows == []
