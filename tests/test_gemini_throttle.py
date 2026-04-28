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
