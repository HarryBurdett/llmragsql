"""Opera3DataSource must filter atran candidates by aentry's open-items rule.

Opera 3 is DBF-based — there's no SQL JOIN. The data source iterates
atran rows and must look up the parent aentry header to apply
ae_reclnum=0 AND ae_remove=0.
"""

from datetime import date
from unittest.mock import MagicMock

from sql_rag.duplicate_check_o3 import Opera3DataSource


def _make_atran_row(entry, value, type_, pstdate, acnt='BB005'):
    return {
        'at_acnt': acnt, 'at_entry': entry, 'at_value': value,
        'at_type': type_, 'at_pstdate': pstdate,
    }


def _make_aentry_row(entry, reclnum=0, remove=False, acnt='BB005'):
    return {
        'ae_acnt': acnt, 'ae_entry': entry,
        'ae_reclnum': reclnum, 'ae_remove': remove,
    }


def _build_reader(atran_rows, aentry_rows):
    """Stub the Opera 3 reader: read_table('atran')/read_table('aentry')."""
    reader = MagicMock()

    def _reader(table_name):
        if table_name == 'atran':
            return iter(atran_rows)
        if table_name == 'aentry':
            return iter(aentry_rows)
        return iter([])

    reader.read_table.side_effect = _reader
    return reader


def test_o3_excludes_removed_aentry():
    """An atran whose parent aentry has ae_remove=True is NOT a candidate."""
    atran_rows = [_make_atran_row('P100000755', -19800, 3, date(2026, 4, 16))]
    aentry_rows = [_make_aentry_row('P100000755', reclnum=0, remove=True)]
    reader = _build_reader(atran_rows, aentry_rows)
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-19800,
        expected_at_type=3,
        exclude_entry_numbers=None,
    )
    assert out == [], "ae_remove=True must exclude the entry from candidates"


def test_o3_excludes_reconciled_aentry():
    atran_rows = [_make_atran_row('P100000755', -19800, 3, date(2026, 4, 16))]
    aentry_rows = [_make_aentry_row('P100000755', reclnum=5, remove=False)]
    reader = _build_reader(atran_rows, aentry_rows)
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-19800,
        expected_at_type=3,
        exclude_entry_numbers=None,
    )
    assert out == [], "ae_reclnum>0 must exclude the entry from candidates"


def test_o3_includes_open_aentry():
    """Open item (reclnum=0, remove=False) IS returned as a candidate."""
    atran_rows = [_make_atran_row('P100000754', -3266, 1, date(2026, 4, 1))]
    aentry_rows = [_make_aentry_row('P100000754', reclnum=0, remove=False)]
    reader = _build_reader(atran_rows, aentry_rows)
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-3266,
        expected_at_type=1,
        exclude_entry_numbers=None,
    )
    assert len(out) == 1
    assert out[0]['ae_entry'] == 'P100000754'


def test_o3_includes_when_aentry_header_missing():
    """If the atran has no parent aentry row in the snapshot (orphan),
    the safest behaviour is to EXCLUDE it (treat as not-an-open-item).
    """
    atran_rows = [_make_atran_row('P_ORPHAN', -10000, 1, date(2026, 4, 16))]
    aentry_rows = []  # no header
    reader = _build_reader(atran_rows, aentry_rows)
    ds = Opera3DataSource(reader)

    out = ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-10000,
        expected_at_type=1,
        exclude_entry_numbers=None,
    )
    assert out == [], "Orphan atran (no aentry header) must be excluded"
