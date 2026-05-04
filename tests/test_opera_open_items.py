"""Unit tests for sql_rag/opera_open_items.py — the single source of
truth for 'is this aentry an open item for bank-rec matching?'."""

import pytest

from sql_rag.opera_open_items import OPEN_FOR_REC_SQL, is_open_for_rec


def test_sql_fragment_value():
    """The SQL fragment is exactly the rule, nothing more, nothing less."""
    assert OPEN_FOR_REC_SQL == "ae_reclnum = 0 AND ae_remove = 0"


def test_open_when_unreconciled_and_not_removed():
    assert is_open_for_rec({'ae_reclnum': 0, 'ae_remove': False}) is True


def test_open_when_unreconciled_and_remove_is_none():
    """NULL on ae_remove is treated as False (= not removed)."""
    assert is_open_for_rec({'ae_reclnum': 0, 'ae_remove': None}) is True


def test_closed_when_reconciled():
    assert is_open_for_rec({'ae_reclnum': 5, 'ae_remove': False}) is False


def test_closed_when_removed():
    assert is_open_for_rec({'ae_reclnum': 0, 'ae_remove': True}) is False


def test_closed_when_both_set():
    assert is_open_for_rec({'ae_reclnum': 5, 'ae_remove': True}) is False


def test_missing_ae_reclnum_treated_as_zero():
    """A row without ae_reclnum is treated as unreconciled (=0)."""
    assert is_open_for_rec({'ae_remove': False}) is True


def test_missing_ae_remove_treated_as_false():
    """A row without ae_remove is treated as not-removed (=False)."""
    assert is_open_for_rec({'ae_reclnum': 0}) is True


def test_decimal_reclnum_handled():
    """pyodbc/pandas often deliver Decimal — must coerce."""
    from decimal import Decimal
    assert is_open_for_rec({'ae_reclnum': Decimal('0'), 'ae_remove': False}) is True
    assert is_open_for_rec({'ae_reclnum': Decimal('5'), 'ae_remove': False}) is False


def test_ae_remove_truthy_string_treated_as_true():
    """Some FoxPro readers return 'T'/'F' strings for booleans."""
    assert is_open_for_rec({'ae_reclnum': 0, 'ae_remove': 'T'}) is False
    assert is_open_for_rec({'ae_reclnum': 0, 'ae_remove': 'F'}) is True
