"""Tests for sql_rag/gemini_throttle.py"""

import pytest

from sql_rag.gemini_throttle import is_rate_limit_error


def test_is_rate_limit_error_429_string():
    exc = Exception("429 Resource exhausted. Please try again later.")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_resource_exhausted():
    exc = Exception("RESOURCE_EXHAUSTED: quota exceeded")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_lowercase_quota():
    exc = Exception("Daily quota exceeded for model X")
    assert is_rate_limit_error(exc) is True


def test_is_rate_limit_error_unrelated_message():
    exc = Exception("Could not parse JSON response")
    assert is_rate_limit_error(exc) is False


def test_is_rate_limit_error_empty_message():
    exc = Exception("")
    assert is_rate_limit_error(exc) is False


import threading
from unittest.mock import MagicMock

from sql_rag.gemini_throttle import (
    RateLimitExhaustedError,
    ExtractionFailedError,
    call_gemini_with_throttle,
    _reset_throttle_state_for_testing,
)


@pytest.fixture(autouse=True)
def _reset_throttle():
    _reset_throttle_state_for_testing()
    yield


def test_call_returns_response_on_success():
    model = MagicMock()
    expected = MagicMock(name="response")
    model.generate_content.return_value = expected

    result = call_gemini_with_throttle(model, ["prompt"], filename="f.pdf")

    assert result is expected
    model.generate_content.assert_called_once_with(["prompt"])


def test_call_retries_on_429_then_succeeds(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.sleep", lambda s: sleeps.append(s)
    )

    model = MagicMock()
    success_response = MagicMock(name="response")
    model.generate_content.side_effect = [
        Exception("429 Resource exhausted"),
        success_response,
    ]

    result = call_gemini_with_throttle(model, ["prompt"], filename="f.pdf")

    assert result is success_response
    assert model.generate_content.call_count == 2
    # First retry sleeps 5s for backoff (throttle interval may add too)
    assert any(abs(s - 5.0) < 0.01 for s in sleeps), f"expected a 5s backoff sleep, got {sleeps}"


def test_call_raises_rate_limit_after_three_retries(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)

    model = MagicMock()
    model.generate_content.side_effect = Exception("429 Resource exhausted")

    with pytest.raises(RateLimitExhaustedError) as exc_info:
        call_gemini_with_throttle(model, ["prompt"], filename="bad.pdf")

    assert "bad.pdf" in str(exc_info.value)
    # 1 initial + 3 retries = 4 total attempts
    assert model.generate_content.call_count == 4


def test_call_raises_extraction_failed_on_non_rate_limit(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)

    model = MagicMock()
    model.generate_content.side_effect = ValueError("Could not parse response")

    with pytest.raises(ExtractionFailedError) as exc_info:
        call_gemini_with_throttle(model, ["prompt"], filename="bad.pdf")

    assert "bad.pdf" in str(exc_info.value)
    # Non-rate-limit errors are not retried
    assert model.generate_content.call_count == 1


def test_call_enforces_minimum_interval(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.sleep", lambda s: sleeps.append(s)
    )

    # Pretend a previous call happened 0.2s ago — helper should sleep ~0.8s
    fake_now = [1000.0]
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.time.monotonic", lambda: fake_now[0]
    )

    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp1")

    # First call sets last_call_time
    call_gemini_with_throttle(model, ["p1"])
    fake_now[0] += 0.2  # 0.2s later

    # Second call should sleep ~0.8s before invoking the model
    call_gemini_with_throttle(model, ["p2"])

    # Find the throttle sleep (~0.8s); ignore any tiny jitter
    throttle_sleeps = [s for s in sleeps if 0.7 < s < 1.0]
    assert len(throttle_sleeps) == 1, f"expected one throttle sleep, got {sleeps}"


from sql_rag.gemini_throttle import configure_keys, _get_active_keys_for_testing


def test_configure_keys_stores_provided_list():
    configure_keys(["k1", "k2", "k3"])
    assert _get_active_keys_for_testing() == ["k1", "k2", "k3"]


def test_configure_keys_strips_empty_and_none():
    configure_keys(["k1", "", None, "k2", "  "])
    assert _get_active_keys_for_testing() == ["k1", "k2"]


def test_configure_keys_with_empty_list_resets():
    configure_keys(["k1"])
    configure_keys([])
    assert _get_active_keys_for_testing() == []


def test_reset_clears_keys():
    configure_keys(["k1", "k2"])
    _reset_throttle_state_for_testing()
    assert _get_active_keys_for_testing() == []


from sql_rag.gemini_throttle import _select_active_key_idx, _mark_key_exhausted


def test_select_active_returns_none_when_no_keys():
    configure_keys([])
    assert _select_active_key_idx() is None


def test_select_active_returns_first_when_all_eligible():
    configure_keys(["k1", "k2", "k3"])
    assert _select_active_key_idx() == 0


def test_select_active_skips_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2", "k3"])
    _mark_key_exhausted(0)
    assert _select_active_key_idx() == 1


def test_select_active_returns_none_when_all_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    _mark_key_exhausted(1)
    assert _select_active_key_idx() is None


def test_exhausted_key_recovers_after_cooldown(monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: fake_now[0])
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    assert _select_active_key_idx() == 1

    # Advance past 30-minute cooldown
    fake_now[0] += 1800.0 + 1.0
    assert _select_active_key_idx() == 0


import google.generativeai as genai


def test_no_keys_configured_uses_existing_model_key(monkeypatch):
    """Backwards-compat: when configure_keys() never called, no genai.configure swap."""
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )
    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp")

    result = call_gemini_with_throttle(model, ["p"])

    assert result is not None
    # No keys configured → no genai.configure swap should occur
    assert configure_calls == []


def test_rotates_to_second_key_when_first_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )

    model = MagicMock()
    success_response = MagicMock(name="resp")
    # k1: 4 attempts (initial + 3 retries) all 429
    # k2: 1 attempt succeeds
    model.generate_content.side_effect = [
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        Exception("429 Resource exhausted"),
        success_response,
    ]

    result = call_gemini_with_throttle(model, ["p"], filename="test.pdf")

    assert result is success_response
    assert model.generate_content.call_count == 5
    # genai.configure called twice — once for k1, once when rotating to k2
    assert configure_calls == ["k1", "k2"]


def test_raises_when_all_keys_exhausted(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    monkeypatch.setattr("sql_rag.gemini_throttle.genai.configure", lambda **kw: None)

    model = MagicMock()
    model.generate_content.side_effect = Exception("429 Resource exhausted")

    with pytest.raises(RateLimitExhaustedError) as exc_info:
        call_gemini_with_throttle(model, ["p"], filename="bad.pdf")

    # 4 attempts on k1 + 4 attempts on k2 = 8 total
    assert model.generate_content.call_count == 8
    assert "bad.pdf" in str(exc_info.value)


def test_non_rate_limit_error_does_not_rotate(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2"])
    monkeypatch.setattr("sql_rag.gemini_throttle.genai.configure", lambda **kw: None)

    model = MagicMock()
    model.generate_content.side_effect = ValueError("Could not parse response")

    with pytest.raises(ExtractionFailedError):
        call_gemini_with_throttle(model, ["p"], filename="bad.pdf")

    # Only one attempt on k1 — no retry, no rotation
    assert model.generate_content.call_count == 1


def test_first_key_success_no_rotation(monkeypatch):
    monkeypatch.setattr("sql_rag.gemini_throttle.time.sleep", lambda s: None)
    configure_keys(["k1", "k2", "k3"])
    configure_calls: list[str] = []
    monkeypatch.setattr(
        "sql_rag.gemini_throttle.genai.configure",
        lambda **kw: configure_calls.append(kw.get("api_key", "")),
    )

    model = MagicMock()
    model.generate_content.return_value = MagicMock(name="resp")

    result = call_gemini_with_throttle(model, ["p"])

    assert result is not None
    assert model.generate_content.call_count == 1
    assert configure_calls == ["k1"]


def test_configure_keys_preserves_exhaustion_when_list_unchanged(monkeypatch):
    """Reconcilers re-call configure_keys() on every HTTP request. The
    exhaustion timer must survive across those calls — otherwise the
    30-minute cooldown never accumulates and rotation re-burns the
    backoff schedule on every PDF."""
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    assert _select_active_key_idx() == 1

    # Reconciler reconfigures with the same list — exhaustion must persist
    configure_keys(["k1", "k2"])
    assert _select_active_key_idx() == 1


def test_configure_keys_resets_exhaustion_when_list_changes(monkeypatch):
    """Conversely, when the key list actually changes, exhaustion state
    is reset — keys may have been added, removed, or reordered."""
    monkeypatch.setattr("sql_rag.gemini_throttle.time.monotonic", lambda: 1000.0)
    configure_keys(["k1", "k2"])
    _mark_key_exhausted(0)
    configure_keys(["k1", "k2", "k3"])  # Different list
    # All keys eligible again after reset
    assert _select_active_key_idx() == 0
