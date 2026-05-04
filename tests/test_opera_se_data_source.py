"""OperaSEDataSource must filter to open items only."""

from datetime import date
from unittest.mock import MagicMock

from sql_rag.duplicate_check_se import OperaSEDataSource


def test_find_aentry_query_contains_open_items_filter():
    """The candidate query MUST filter by ae_reclnum=0 AND ae_remove=0."""
    fake = MagicMock()
    empty_df = MagicMock()
    empty_df.empty = True
    fake.execute_query.return_value = empty_df

    ds = OperaSEDataSource(fake)
    ds.find_aentry_by_signed_value(
        bank_code='BB005',
        date_from=date(2026, 4, 1),
        date_to=date(2026, 4, 30),
        signed_pence=-19800,
        expected_at_type=3,
        exclude_entry_numbers=None,
    )
    sql = fake.execute_query.call_args[0][0]
    assert 'ae_reclnum = 0' in sql
    assert 'ae_remove = 0' in sql
