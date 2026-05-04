"""Regression test for the Cloudsis BB005 P Flannery £198 incident
(2026-05-04). The reversing entry P100000755 has ae_remove=True
(operator matched it as a correction pair in Opera). The matcher
must NOT flag the corresponding statement line as 'in Opera'.
"""

from datetime import date
from unittest.mock import MagicMock


def _row(at_entry, at_value, at_type, ae_reclnum=0, ae_remove=False):
    return {
        'at_acnt': 'BB005', 'at_entry': at_entry,
        'at_cbtype': 'P1', 'at_value': at_value, 'at_type': at_type,
        'at_pstdate': date(2026, 4, 16),
        'ae_acnt': 'BB005', 'ae_entry': at_entry,
        'ae_reclnum': ae_reclnum, 'ae_remove': ae_remove,
    }


def test_flannery_198_with_ae_remove_true_is_not_a_candidate():
    """P100000755 has ae_remove=True → must NOT match the statement line."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    atran_rows = [_row('P100000755', -19800, 3)]
    aentry_rows = [{'ae_acnt': 'BB005', 'ae_entry': 'P100000755',
                    'ae_reclnum': 0, 'ae_remove': True}]
    reader = MagicMock()

    def _reader(t):
        if t == 'atran':
            return iter(atran_rows)
        if t == 'aentry':
            return iter(aentry_rows)
        return iter([])

    reader.read_table.side_effect = _reader
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-19800,
        expected_at_type=3,
        exclude_entry_numbers=None,
    )
    assert out == [], (
        "Cloudsis BB005 P100000755 (£-198 sales refund, matched in Opera "
        "as a correction pair → ae_remove=True) must NOT be returned as "
        "a candidate. If this fails, the Flannery incident has regressed."
    )


def test_flannery_198_with_ae_remove_false_IS_a_candidate():
    """Sanity: with ae_remove=False the same row IS a candidate."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    atran_rows = [_row('P100000755', -19800, 3)]
    aentry_rows = [{'ae_acnt': 'BB005', 'ae_entry': 'P100000755',
                    'ae_reclnum': 0, 'ae_remove': False}]
    reader = MagicMock()

    def _reader(t):
        if t == 'atran':
            return iter(atran_rows)
        if t == 'aentry':
            return iter(aentry_rows)
        return iter([])

    reader.read_table.side_effect = _reader
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-19800,
        expected_at_type=3,
        exclude_entry_numbers=None,
    )
    assert len(out) == 1
