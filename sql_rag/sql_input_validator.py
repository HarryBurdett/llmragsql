"""Strict input validators for parameters that flow into Opera SQL.

Audit 2026-05-05 cross-cutting F5 / stages-1-2 F5: ~250 f-string SQL
sites in the bank-rec / GoCardless / suppliers route handlers
interpolate user-supplied values directly into queries. Even though
all routes are auth-gated, an authorised user (or an attacker with a
stolen session) could craft a `bank_code` like `BC010'; DROP TABLE
atran--` and the SQL would execute it.

A full conversion of every f-string site to bound parameters is a
separate, multi-week hardening sprint that must not be rushed (the
SE bank-rec / GoCardless paths are tested and any wrong conversion
introduces a regression). The pragmatic interim is to validate
every route-supplied identifier at the FastAPI boundary so it can
NEVER be a SQL-injection payload by the time it reaches any query
builder.

This module provides typed validators usable as FastAPI Path/Query
constraints AND as standalone callables for body fields.

Each validator:
  - Accepts the documented Opera identifier shape (alphanumeric,
    underscores, dashes — with documented length limits).
  - Rejects strings containing semicolons, quotes, or SQL keywords.
  - Returns the cleaned string on success, raises HTTPException(400)
    on failure.
"""
from __future__ import annotations

import re
from typing import Optional

from fastapi import HTTPException


# Patterns. These mirror Opera's own column-shape conventions plus
# generous bounds to cover edge cases.
_BANK_CODE_RE = re.compile(r'^[A-Z0-9_-]{1,12}$', re.IGNORECASE)
_ACCOUNT_CODE_RE = re.compile(r'^[A-Z0-9_./-]{1,16}$', re.IGNORECASE)
_ENTRY_NUMBER_RE = re.compile(r'^[A-Z0-9_./-]{1,20}$', re.IGNORECASE)
_CBTYPE_RE = re.compile(r'^[A-Z0-9]{1,4}$', re.IGNORECASE)
_PAYMENT_REF_RE = re.compile(r'^[A-Z0-9 _./\-]{1,30}$', re.IGNORECASE)
_REFERENCE_RE = re.compile(r'^[A-Z0-9 _./\-:#&,]{0,40}$', re.IGNORECASE)
_BATCH_NUMBER_RE = re.compile(r'^\d{1,9}$')

# Forbidden tokens in any of the above. Even if the regex above
# permits only safe characters, this is a belt-and-braces guard for
# any future regex expansion.
_FORBIDDEN_TOKENS = (
    "'",  # single quote — primary SQL string delimiter
    '"',  # double quote
    ';',  # statement separator
    '--', # SQL line comment
    '/*', # SQL block comment
    '*/',
    '\\',
)


def _has_forbidden(value: str) -> Optional[str]:
    for tok in _FORBIDDEN_TOKENS:
        if tok in value:
            return tok
    return None


def validate_bank_code(value: str) -> str:
    """Validate an Opera bank account code. Letters, digits, _, -, up
    to 12 characters."""
    if value is None or value == '':
        raise HTTPException(status_code=400, detail='bank_code required')
    if not _BANK_CODE_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=(
                f"bank_code '{value}' is not a valid Opera bank code "
                "(alphanumeric/underscore/dash, max 12 chars)."
            ),
        )
    bad = _has_forbidden(value)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"bank_code contains forbidden character '{bad}'.",
        )
    return value.strip()


def validate_account_code(value: str) -> str:
    """Validate a customer/supplier/nominal account code."""
    if value is None or value == '':
        raise HTTPException(status_code=400, detail='account code required')
    if not _ACCOUNT_CODE_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=(
                f"account code '{value}' is not valid "
                "(alphanumeric/_/./-/, max 16 chars)."
            ),
        )
    bad = _has_forbidden(value)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"account code contains forbidden character '{bad}'.",
        )
    return value.strip()


def validate_entry_number(value: str) -> str:
    """Validate an Opera entry number (ae_entry, pt_trref, etc.)."""
    if value is None or value == '':
        raise HTTPException(status_code=400, detail='entry number required')
    if not _ENTRY_NUMBER_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=f"entry number '{value}' is not a valid Opera entry reference.",
        )
    bad = _has_forbidden(value)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"entry number contains forbidden character '{bad}'.",
        )
    return value.strip()


def validate_cbtype(value: str) -> str:
    """Validate a cashbook type code."""
    if not value:
        return ''
    if not _CBTYPE_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=f"cbtype '{value}' is not valid (max 4 alphanumeric chars).",
        )
    return value.strip().upper()


def validate_payment_ref(value: str) -> str:
    """Validate a payment reference (e.g. pt_trref of a payment)."""
    if not value:
        raise HTTPException(status_code=400, detail='payment_ref required')
    if not _PAYMENT_REF_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=f"payment_ref '{value}' contains invalid characters.",
        )
    bad = _has_forbidden(value)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"payment_ref contains forbidden character '{bad}'.",
        )
    return value.strip()


def validate_reference(value: str) -> str:
    """Validate a free-form reference (allows colons, ampersands etc.).
    Empty allowed — many flows pass an empty reference."""
    if value is None:
        return ''
    value = str(value)
    if not _REFERENCE_RE.match(value):
        raise HTTPException(
            status_code=400,
            detail=f"reference '{value}' contains invalid characters.",
        )
    bad = _has_forbidden(value)
    if bad:
        raise HTTPException(
            status_code=400,
            detail=f"reference contains forbidden character '{bad}'.",
        )
    return value.strip()


def validate_batch_number(value) -> int:
    """Validate a rec batch number — digits only, max 9 digits."""
    s = str(value)
    if not _BATCH_NUMBER_RE.match(s):
        raise HTTPException(
            status_code=400,
            detail=f"batch number '{value}' must be digits only (max 9 chars).",
        )
    return int(s)
