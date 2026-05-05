"""Pin the SQL-input validators against actual injection payloads.

Audit 2026-05-05 cross-cutting F5: ~250 f-string SQL sites in the
route handlers. The validators in sql_rag/sql_input_validator.py are
the boundary-level mitigation that ensures no malicious payload can
ever reach those f-string builders.
"""
import pytest
from fastapi import HTTPException


def test_bank_code_accepts_normal_codes():
    from sql_rag.sql_input_validator import validate_bank_code
    assert validate_bank_code('BC010') == 'BC010'
    assert validate_bank_code('bb_005') == 'bb_005'
    assert validate_bank_code('A-1') == 'A-1'


def test_bank_code_rejects_quote_injection():
    from sql_rag.sql_input_validator import validate_bank_code
    with pytest.raises(HTTPException):
        validate_bank_code("BC010' OR '1'='1")


def test_bank_code_rejects_drop_table_injection():
    from sql_rag.sql_input_validator import validate_bank_code
    with pytest.raises(HTTPException):
        validate_bank_code("BC010'; DROP TABLE atran--")


def test_bank_code_rejects_comment_injection():
    from sql_rag.sql_input_validator import validate_bank_code
    with pytest.raises(HTTPException):
        validate_bank_code("BC010 /* comment */")


def test_bank_code_rejects_too_long():
    from sql_rag.sql_input_validator import validate_bank_code
    with pytest.raises(HTTPException):
        validate_bank_code('A' * 13)


def test_bank_code_rejects_empty():
    from sql_rag.sql_input_validator import validate_bank_code
    with pytest.raises(HTTPException):
        validate_bank_code('')
    with pytest.raises(HTTPException):
        validate_bank_code(None)


def test_account_code_accepts_dotted_and_slashed():
    """Some Opera installations use codes like ABC/123 or ABC.SUB."""
    from sql_rag.sql_input_validator import validate_account_code
    assert validate_account_code('ABC.SUB') == 'ABC.SUB'
    assert validate_account_code('A001') == 'A001'


def test_account_code_rejects_injection():
    from sql_rag.sql_input_validator import validate_account_code
    with pytest.raises(HTTPException):
        validate_account_code("A001' UNION SELECT")


def test_entry_number_accepts_typical_opera_format():
    from sql_rag.sql_input_validator import validate_entry_number
    assert validate_entry_number('P100000754') == 'P100000754'
    assert validate_entry_number('R000000001') == 'R000000001'


def test_entry_number_rejects_injection():
    from sql_rag.sql_input_validator import validate_entry_number
    with pytest.raises(HTTPException):
        validate_entry_number("P100000754'; DROP")


def test_cbtype_accepts_short_codes():
    from sql_rag.sql_input_validator import validate_cbtype
    assert validate_cbtype('P1') == 'P1'
    assert validate_cbtype('SR') == 'SR'
    assert validate_cbtype('') == ''


def test_cbtype_rejects_long_or_injection():
    from sql_rag.sql_input_validator import validate_cbtype
    with pytest.raises(HTTPException):
        validate_cbtype('TOOLONG')
    with pytest.raises(HTTPException):
        validate_cbtype("'OR")


def test_payment_ref_accepts_typical_refs():
    from sql_rag.sql_input_validator import validate_payment_ref
    assert validate_payment_ref('CHQ1234') == 'CHQ1234'
    assert validate_payment_ref('BACS-2026/05') == 'BACS-2026/05'


def test_payment_ref_rejects_injection():
    from sql_rag.sql_input_validator import validate_payment_ref
    with pytest.raises(HTTPException):
        validate_payment_ref("CHQ1234'; --")


def test_batch_number_returns_int():
    from sql_rag.sql_input_validator import validate_batch_number
    assert validate_batch_number(2682) == 2682
    assert validate_batch_number('2682') == 2682


def test_batch_number_rejects_non_digit():
    from sql_rag.sql_input_validator import validate_batch_number
    with pytest.raises(HTTPException):
        validate_batch_number('2682; DROP')
    with pytest.raises(HTTPException):
        validate_batch_number('not-a-number')


def test_reference_accepts_empty():
    """Empty reference is allowed — many endpoints use that."""
    from sql_rag.sql_input_validator import validate_reference
    assert validate_reference('') == ''
    assert validate_reference(None) == ''


def test_reference_rejects_injection():
    from sql_rag.sql_input_validator import validate_reference
    with pytest.raises(HTTPException):
        validate_reference("anything'; --")
    with pytest.raises(HTTPException):
        validate_reference("name with ; semicolon")
