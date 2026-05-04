"""match_statement_to_cashbook must filter by the open-items rule."""

from unittest.mock import MagicMock

from sql_rag.opera_sql_import import OperaSQLImport


def test_candidate_query_contains_open_items_filter():
    """The candidate-fetch SQL MUST include ae_reclnum=0 AND ae_remove=0."""
    fake = MagicMock()
    fake.execute_query.return_value = None  # short-circuit

    importer = OperaSQLImport.__new__(OperaSQLImport)
    importer.sql = fake

    importer.match_statement_to_cashbook(
        bank_account='BB005',
        statement_transactions=[],
    )
    # Several queries may have been issued — collect them all
    all_sql = ' '.join(c[0][0] for c in fake.execute_query.call_args_list)
    assert 'ae_reclnum = 0' in all_sql
    assert 'ae_remove = 0' in all_sql, (
        "match_statement_to_cashbook MUST filter by ae_remove=0 to exclude "
        "correction-pair-matched entries from the candidate pool"
    )
