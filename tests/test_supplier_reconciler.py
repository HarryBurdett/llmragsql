"""
Tests for sql_rag/supplier_reconciler.py

Each test verifies:
  1. The correct categorisation of items (agreed / theirs_only / ours_only)
  2. That math_checks_out is True (the mathematical guarantee holds)
"""

import pytest
from sql_rag.supplier_reconciler import (
    TheirItem,
    OurItem,
    reconcile,
    clean_reference,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _assert_math(result):
    """Convenience assertion reused in every test."""
    assert result.math_checks_out, (
        f"Math failed: difference={result.difference}, "
        f"theirs_only_net={result.theirs_only_net}, "
        f"ours_only_net={result.ours_only_net}, "
        f"amount_diffs_net={result.amount_diffs_net}"
    )


# ---------------------------------------------------------------------------
# Test 1: All items match — balances agree, math checks out
# ---------------------------------------------------------------------------

def test_all_matched_balances_agree():
    theirs = [
        TheirItem(reference="INV001", debit=100.00, credit=0.00),
        TheirItem(reference="INV002", debit=200.00, credit=0.00),
    ]
    ours = [
        OurItem(reference="INV001", balance=100.00),
        OurItem(reference="INV002", balance=200.00),
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 2
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0
    assert result.difference == pytest.approx(0.00)
    assert result.their_balance == pytest.approx(300.00)
    assert result.our_balance == pytest.approx(300.00)
    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 2: Unmatched items on both sides — difference fully explained
# ---------------------------------------------------------------------------

def test_unmatched_both_sides():
    theirs = [
        TheirItem(reference="INV001", debit=100.00, credit=0.00),   # matched
        TheirItem(reference="INV003", debit=50.00,  credit=0.00),   # theirs only
    ]
    ours = [
        OurItem(reference="INV001", balance=100.00),                # matched
        OurItem(reference="INV004", balance=75.00),                 # ours only
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 1
    assert result.agreed[0].reference == "INV001"

    assert len(result.theirs_only) == 1
    assert result.theirs_only[0].reference == "INV003"
    assert result.theirs_only[0].amount == pytest.approx(50.00)

    assert len(result.ours_only) == 1
    assert result.ours_only[0].reference == "INV004"
    assert result.ours_only[0].amount == pytest.approx(75.00)

    # difference = 150 - 175 = -25
    assert result.difference == pytest.approx(-25.00)
    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 3: Case-insensitive matching with *OVERDUE* cleanup
# ---------------------------------------------------------------------------

def test_case_insensitive_and_overdue_cleanup():
    theirs = [
        TheirItem(reference="*OVERDUE*INV005", debit=300.00, credit=0.00),
        TheirItem(reference="inv006", debit=120.00, credit=0.00),
    ]
    ours = [
        OurItem(reference="INV005", balance=300.00),
        OurItem(reference="INV006", balance=120.00),
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 2
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0
    assert result.difference == pytest.approx(0.00)
    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 4: Amount difference on matched reference
# ---------------------------------------------------------------------------

def test_amount_difference_on_matched_reference():
    theirs = [
        TheirItem(reference="INV010", debit=500.00, credit=0.00),
    ]
    ours = [
        OurItem(reference="INV010", balance=490.00),   # £10 less than their figure
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 1
    item = result.agreed[0]
    assert item.their_amount == pytest.approx(500.00)
    assert item.our_amount == pytest.approx(490.00)
    assert item.amount_difference == pytest.approx(10.00)

    assert result.difference == pytest.approx(10.00)
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0
    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 5: Empty references always unmatched
# ---------------------------------------------------------------------------

def test_empty_references_always_unmatched():
    theirs = [
        TheirItem(reference="",      debit=80.00, credit=0.00),   # empty → theirs only
        TheirItem(reference="INV020", debit=60.00, credit=0.00),  # matched
    ]
    ours = [
        OurItem(reference="INV020", balance=60.00),               # matched
        OurItem(reference="",       balance=40.00),               # empty → ours only
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 1
    assert result.agreed[0].reference == "INV020"

    # Empty-ref their item goes to theirs_only
    assert len(result.theirs_only) == 1
    assert result.theirs_only[0].reference == ""
    assert result.theirs_only[0].amount == pytest.approx(80.00)

    # Empty-ref our item goes to ours_only
    assert len(result.ours_only) == 1
    assert result.ours_only[0].reference == ""
    assert result.ours_only[0].amount == pytest.approx(40.00)

    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 6: Duplicate Opera references grouped and pt_trbal summed
# ---------------------------------------------------------------------------

def test_duplicate_opera_references_grouped():
    theirs = [
        TheirItem(reference="INV030", debit=150.00, credit=0.00),
    ]
    # Two Opera rows with the same reference — balances must be summed
    ours = [
        OurItem(reference="INV030", balance=100.00, detail="line 1"),
        OurItem(reference="INV030", balance=50.00,  detail="line 2"),
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 1
    item = result.agreed[0]
    assert item.our_amount == pytest.approx(150.00)
    assert item.amount_difference == pytest.approx(0.00)
    assert len(item.our_details) == 2

    assert result.difference == pytest.approx(0.00)
    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 7: Mixed scenario — math still checks out
# ---------------------------------------------------------------------------

def test_mixed_scenario_math_checks_out():
    theirs = [
        TheirItem(reference="INV100", debit=1000.00, credit=0.00),  # agreed, exact
        TheirItem(reference="INV101", debit=200.00,  credit=0.00),  # agreed, diff
        TheirItem(reference="INV102", debit=300.00,  credit=0.00),  # theirs only
    ]
    ours = [
        OurItem(reference="INV100", balance=1000.00),    # agreed, exact
        OurItem(reference="INV101", balance=210.00),     # agreed, our side higher by £10
        OurItem(reference="INV103", balance=400.00),     # ours only
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 2
    assert len(result.theirs_only) == 1
    assert result.theirs_only[0].reference == "INV102"
    assert len(result.ours_only) == 1
    assert result.ours_only[0].reference == "INV103"

    # their_balance = 1500, our_balance = 1610, difference = -110
    assert result.their_balance == pytest.approx(1500.00)
    assert result.our_balance == pytest.approx(1610.00)
    assert result.difference == pytest.approx(-110.00)

    # INV101 amount_difference = 200 - 210 = -10
    inv101 = next(a for a in result.agreed if "INV101" in a.reference)
    assert inv101.amount_difference == pytest.approx(-10.00)

    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 8: Negative balances (credits) handled correctly
# ---------------------------------------------------------------------------

def test_credits_handled_correctly():
    # Supplier has issued a credit note; also has an outstanding invoice
    theirs = [
        TheirItem(reference="INV200", debit=500.00, credit=0.00),
        TheirItem(reference="CN001",  debit=0.00,   credit=100.00),  # credit note
    ]
    ours = [
        OurItem(reference="INV200", balance=500.00),   # positive = we owe
        OurItem(reference="CN001",  balance=-100.00),  # negative = credit in ptran
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 2
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0

    # their_balance = 500 - 100 = 400
    # our_balance   = 500 + (-100) = 400
    assert result.their_balance == pytest.approx(400.00)
    assert result.our_balance == pytest.approx(400.00)
    assert result.difference == pytest.approx(0.00)

    # Credit note: their net = 0 - 100 = -100; our amount = -100; diff = 0
    cn = next(a for a in result.agreed if "CN001" in a.reference)
    assert cn.their_amount == pytest.approx(-100.00)
    assert cn.our_amount == pytest.approx(-100.00)
    assert cn.amount_difference == pytest.approx(0.00)

    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 9: Empty lists (no items on either side)
# ---------------------------------------------------------------------------

def test_empty_lists():
    result = reconcile([], [])

    assert result.their_balance == pytest.approx(0.00)
    assert result.our_balance == pytest.approx(0.00)
    assert result.difference == pytest.approx(0.00)
    assert result.agreed == []
    assert result.theirs_only == []
    assert result.ours_only == []
    assert result.math_checks_out is True


# ---------------------------------------------------------------------------
# Test 10: clean_reference helper
# ---------------------------------------------------------------------------

def test_clean_reference():
    assert clean_reference("*OVERDUE*INV001") == "inv001"
    assert clean_reference("  INV002  ")      == "inv002"
    assert clean_reference("*OVERDUE* INV003 ") == "inv003"
    assert clean_reference("inv004")           == "inv004"
    assert clean_reference("")                 == ""
    assert clean_reference("*")               == ""
    assert clean_reference("INV005*")         == "inv005"
    # Mixed case
    assert clean_reference("Inv006")          == "inv006"


# ---------------------------------------------------------------------------
# Test 11: Duplicate their-side references — grouped and summed correctly
# ---------------------------------------------------------------------------

def test_duplicate_their_side_references_grouped():
    """
    Two supplier statement lines share the same reference (e.g., a part-payment
    split across two statement rows).  They must be merged into a single
    AgreedItem rather than producing two entries that double-count our_amount.
    """
    # Statement has INV040 split into two lines: £60 + £40 = £100 total
    theirs = [
        TheirItem(reference="INV040", debit=60.00, credit=0.00, detail="line A"),
        TheirItem(reference="INV040", debit=40.00, credit=0.00, detail="line B"),
    ]
    # Opera has a single row for INV040 at £100
    ours = [
        OurItem(reference="INV040", balance=100.00, detail="opera row"),
    ]
    result = reconcile(theirs, ours)

    # Should produce exactly ONE agreed item, not two
    assert len(result.agreed) == 1, (
        f"Expected 1 agreed item, got {len(result.agreed)} — "
        "duplicate their-side refs were not grouped"
    )
    item = result.agreed[0]

    # their_amount must be the summed total (60 + 40 = 100), not 60 or 40 alone
    assert item.their_amount == pytest.approx(100.00)
    assert item.our_amount == pytest.approx(100.00)
    assert item.amount_difference == pytest.approx(0.00)

    # Balances and difference
    assert result.their_balance == pytest.approx(100.00)
    assert result.our_balance == pytest.approx(100.00)
    assert result.difference == pytest.approx(0.00)

    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0

    _assert_math(result)


# ---------------------------------------------------------------------------
# Test 12: Tolerance edge case — floating-point penny rounding
# ---------------------------------------------------------------------------

def test_tolerance_edge_case_floating_point():
    """
    Amounts chosen so that Python floating-point arithmetic produces a
    difference that is representable as exactly 0.01 (or indistinguishably
    close to it).  The math check must pass because <= is used, not <.

    0.1 + 0.2 == 0.30000000000000004 in IEEE 754, so using those as the
    building blocks produces a real-world fp rounding scenario.
    """
    # their net = 0.30 (debit 0.30, credit 0.00)
    # our balance = 0.20
    # amount_difference = 0.10; difference = 0.10; math trivially checks out.
    # For the edge case we want the *computed* check itself to land exactly on
    # the boundary.  Construct amounts where the rounded difference is exactly
    # 0.01 due to cumulative floating-point arithmetic.
    theirs = [
        TheirItem(reference="INV050", debit=1.10, credit=0.00),   # net = 1.10
        TheirItem(reference="INV051", debit=2.20, credit=0.00),   # net = 2.20
    ]
    ours = [
        OurItem(reference="INV050", balance=1.10),
        OurItem(reference="INV051", balance=2.19),   # 1p less → amount_diff = 0.01
    ]
    result = reconcile(theirs, ours)

    assert len(result.agreed) == 2

    inv051 = next(a for a in result.agreed if "INV051" in a.reference)
    assert inv051.amount_difference == pytest.approx(0.01, abs=1e-9)

    # The overall difference is 0.01 — math must check out with <= not <
    assert result.difference == pytest.approx(0.01)
    assert result.math_checks_out is True, (
        "math_checks_out failed on a penny-rounding edge case — "
        "tolerance check must use <= not <"
    )
    _assert_math(result)
