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
