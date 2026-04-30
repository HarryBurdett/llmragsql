"""Tests for bank-method suffix stripping in extract_payee_name_full().

Strips trailing parenthesised bank-method suffixes like `(Faster Payments)`,
`(Direct Debit)`, `(Standing Order)`, `(BACS)`, `(CHAPS)`, `(Card Payment)`,
`(Cheque)`, `(Cash)`, `(Online Payment)`, `(Transfer)` — including truncated
forms like `(Faster Pay...)` or `(Faster Pay…)`. Embedded parens that are
part of legal company names (e.g. `(Bristol)`, `(oval)`) are preserved.
"""

import pytest

from sql_rag.bank_import import extract_payee_name_full


@pytest.mark.parametrize("input_text,expected", [
    # Bank-method suffix stripping
    ("Diskel (Faster Payments)", "Diskel"),
    ("Diskel (Faster Payment)", "Diskel"),
    ("Diskel (Faster Pay...)", "Diskel"),
    ("Diskel (Faster Pay…)", "Diskel"),
    ("Customer (Direct Debit)", "Customer"),
    ("Customer (Standing Order)", "Customer"),
    ("Customer (BACS)", "Customer"),
    ("Customer (CHAPS)", "Customer"),
    ("Customer (Card Payment)", "Customer"),
    ("Customer (Cheque)", "Customer"),
    ("Customer (Cash)", "Customer"),
    ("Customer (Online Payment)", "Customer"),
    ("Customer (Transfer)", "Customer"),
    # Embedded parens preserved when there's also a trailing bank-method suffix
    ("P Flannery Plant Hire(oval) Limited (Faster Pay...)",
     "P Flannery Plant Hire(oval) Limited"),
    # Embedded parens preserved when there's NO trailing bank-method suffix
    ("Acme (Bristol) Ltd", "Acme (Bristol) Ltd"),
    ("P Flannery Plant Hire(oval) Limited", "P Flannery Plant Hire(oval) Limited"),
    # Non-matching parens preserved
    ("Customer (Old Name) (Faster Payments)", "Customer (Old Name)"),
    # Empty input
    ("", ""),
])
def test_extract_payee_name_full_strips_bank_method_suffix(input_text, expected):
    assert extract_payee_name_full(input_text) == expected


from sql_rag.bank_import_opera3 import extract_payee_name_full as extract_payee_name_full_o3


def test_opera3_mirror_strips_bank_method_suffix():
    """Opera 3 mirror must produce the same result as SE for the same input."""
    assert extract_payee_name_full_o3("Diskel (Faster Payments)") == "Diskel"
    assert extract_payee_name_full_o3(
        "P Flannery Plant Hire(oval) Limited (Faster Pay...)"
    ) == "P Flannery Plant Hire(oval) Limited"
    assert extract_payee_name_full_o3("Acme (Bristol) Ltd") == "Acme (Bristol) Ltd"
