"""Per-PDF validation helper for bank-statement scan endpoints.

Audit cross-cutting F9: the per-attachment validation block (cache
lookup → optional inline AI extraction → account match → chain
check → already-processed audit row) is duplicated across four
scan endpoints — scan_emails_for_bank_statements (SE),
opera3_scan_emails_for_bank_statements (O3),
scan_all_banks_for_statements, and scan_folder_for_bank_statements.
Each copy is ~150 lines, totalling ~600 lines of identical-shape
code across the file.

This module extracts the validation pipeline into one orchestration
function `validate_pdf_for_scan` plus three composable helpers that
match the original phases:
  - get_statement_info       : cache lookup OR inline extraction
  - check_account_match      : statement sort/account vs Opera bank
  - check_chain_complete     : opening below reconciled OR closing
                                matches reconciled opening

Behaviour is preserved exactly. The original handler code is replaced
with helper invocations that map verdict fields back onto the
attachment dict and per-statement validity flag.
"""
from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)


# ============================================================
# Result types
# ============================================================

@dataclass
class StatementInfoData:
    """Normalised statement-info dict, sourced from cache or extraction.

    Field shape mirrors the dict the route handlers used to assemble
    inline. `extraction_status` is one of:
      - 'cached'             cache hit, info_data from cache
      - 'extracted'          cache miss + Gemini extraction succeeded
      - 'pending_extraction' cache miss + extract_on_miss=False, OR
                             rate-limit exhausted, OR generic failure
                             (consult extraction_failure_reason)
      - 'failed'             cache miss + extraction returned an error
    """
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    bank_name: Optional[str] = None
    account_number: Optional[str] = None
    sort_code: Optional[str] = None
    extraction_status: str = 'cached'
    extraction_failure_reason: Optional[str] = None


@dataclass
class AccountMatchResult:
    matches: bool
    validation_status: Optional[str] = None      # 'wrong_account' | None
    skip_reason: Optional[str] = None


@dataclass
class ChainCheckResult:
    """Result of the already-processed chain check.

    `chain_complete` True means the statement is past Opera's
    reconciled balance and should be filtered out.

    `reason_kind` distinguishes which branch fired:
      - 'closing_matches_reconciled_opening'
      - 'opening_below_reconciled'
      - None (chain not complete)

    Only the 'opening_below_reconciled' kind triggers writing an
    audit row to bank_statement_imports — preserving original
    behaviour (chain-match suppresses the row, balance-below records
    it).
    """
    chain_complete: bool
    reason_kind: Optional[str] = None
    skip_reason: Optional[str] = None


@dataclass
class StatementValidationVerdict:
    """One-stop result the route handler applies to its loop state.

    `is_valid`: append to statements_found list?
    `validation_status`: which failure mode (if any) — surfaced to the UI
    `skip_reason`: human-readable reason for the skipped_reasons list
    `record_already_processed`: write a bank_statement_imports audit
        row + increment already_processed_count
    `info_updates`: dict of fields to merge into the attachment dict
        (period_start, period_end, bank_name, etc.)
    `statement_opening_balance`: assigned to the per-email
        `statement_opening_balance` accumulator if not None
    """
    is_valid: bool
    validation_status: Optional[str] = None
    skip_reason: Optional[str] = None
    record_already_processed: bool = False
    info_updates: Dict[str, Any] = field(default_factory=dict)
    statement_opening_balance: Optional[float] = None


# ============================================================
# Phase 1 — cache lookup OR inline extraction
# ============================================================

def get_statement_info(
    *,
    content_bytes: bytes,
    filename: str,
    cache,                              # PDFExtractionCache
    sql_connector,
    company_settings: Dict[str, Any],
    config,
    extract_on_miss: bool,
) -> StatementInfoData:
    """Cache lookup; on miss either run inline AI extraction or defer.

    Mirrors the SE scan-emails block at lines ~6431–6564 of
    apps/bank_reconcile/api/routes.py prior to the F9 wedge.
    """
    pdf_hash = cache.hash_pdf(content_bytes)
    cached = cache.get(pdf_hash)

    # ---- Cache hit ----
    if cached:
        info_data, _ = cached
        logger.info(f"Scan cache HIT for {filename}")
        opening_bal_raw = info_data.get('opening_balance')
        closing_bal_raw = info_data.get('closing_balance')
        return StatementInfoData(
            opening_balance=float(opening_bal_raw) if opening_bal_raw is not None else None,
            closing_balance=float(closing_bal_raw) if closing_bal_raw is not None else None,
            period_start=info_data.get('period_start'),
            period_end=info_data.get('period_end'),
            bank_name=info_data.get('bank_name'),
            account_number=info_data.get('account_number'),
            sort_code=info_data.get('sort_code'),
            extraction_status='cached',
        )

    # ---- Cache miss + opt-in defer (audit F8) ----
    if not extract_on_miss:
        logger.info(f"Scan cache MISS for {filename} — extract_on_miss=False, deferring extraction")
        return StatementInfoData(extraction_status='pending_extraction')

    # ---- Cache miss + inline extraction ----
    logger.info(f"Scan cache MISS for {filename} — running full extraction")
    try:
        from sql_rag.statement_reconcile import (
            StatementReconciler, RateLimitExhaustedError, ExtractionFailedError,
        )
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp.write(content_bytes)
            tmp_path = tmp.name
        try:
            api_key = (
                company_settings.get('gemini_api_key')
                or (config.get('gemini', 'api_key', fallback='') if config and config.has_section('gemini') else '')
            )
            reconciler = StatementReconciler(sql_connector, gemini_api_key=api_key)
            stmt_info, _ = reconciler.extract_transactions_from_pdf(tmp_path)
            return StatementInfoData(
                opening_balance=stmt_info.opening_balance,
                closing_balance=stmt_info.closing_balance,
                period_start=stmt_info.period_start.strftime('%Y-%m-%d') if stmt_info.period_start else None,
                period_end=stmt_info.period_end.strftime('%Y-%m-%d') if stmt_info.period_end else None,
                bank_name=stmt_info.bank_name,
                account_number=stmt_info.account_number,
                sort_code=stmt_info.sort_code,
                extraction_status='extracted',
            )
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
    except Exception as ex:
        # Distinguish rate-limit and explicit extraction errors from
        # generic failures because the UI surfaces these differently.
        try:
            from sql_rag.statement_reconcile import (
                RateLimitExhaustedError, ExtractionFailedError,
            )
        except Exception:
            RateLimitExhaustedError = type('RateLimitExhaustedError', (), {})
            ExtractionFailedError = type('ExtractionFailedError', (), {})

        if isinstance(ex, RateLimitExhaustedError):
            logger.warning(f"Rate-limit exhausted for {filename}: {ex}")
            return StatementInfoData(
                extraction_status='pending_extraction',
                extraction_failure_reason='rate_limit',
            )
        if isinstance(ex, ExtractionFailedError):
            logger.warning(f"Extraction error for {filename}: {ex}")
            return StatementInfoData(
                extraction_status='failed',
                extraction_failure_reason='extraction_error',
            )
        logger.warning(f"Full extraction failed for {filename}: {ex}")
        return StatementInfoData(
            extraction_status='failed',
            extraction_failure_reason='extraction_error',
        )


# ============================================================
# Phase 2 — sort code / account number match
# ============================================================

def _normalise(s: Optional[str]) -> str:
    """Strip dashes, spaces and surrounding whitespace."""
    if not s:
        return ''
    return s.replace('-', '').replace(' ', '').strip()


def check_account_match(
    info: StatementInfoData,
    opera_sort_code: Optional[str],
    opera_account_number: Optional[str],
    filename: str,
) -> AccountMatchResult:
    """Compare statement sort/account to the Opera bank's sort/account.

    Three cases:
      1. Both stmt and opera have sort+account → require both to match
      2. Only account numbers available           → match on account
      3. Either side missing                      → assume match (no
         basis to reject; the chain check / balance check will catch it)
    """
    stmt_sort = _normalise(info.sort_code)
    stmt_acct = _normalise(info.account_number)
    opera_sort = _normalise(opera_sort_code)
    opera_acct = _normalise(opera_account_number)

    if stmt_sort and stmt_acct and opera_sort and opera_acct:
        if stmt_sort == opera_sort and stmt_acct == opera_acct:
            return AccountMatchResult(matches=True)
        logger.info(
            f"Statement account mismatch: statement={stmt_sort}/{stmt_acct}, "
            f"opera={opera_sort}/{opera_acct}"
        )
        return AccountMatchResult(
            matches=False,
            validation_status='wrong_account',
            skip_reason=(
                f"Statement {filename}: wrong bank account "
                f"({stmt_sort}/{stmt_acct} vs Opera {opera_sort}/{opera_acct})"
            ),
        )

    if stmt_acct and opera_acct:
        if stmt_acct == opera_acct:
            return AccountMatchResult(matches=True)
        logger.info(f"Statement account number mismatch: statement={stmt_acct}, opera={opera_acct}")
        return AccountMatchResult(
            matches=False,
            validation_status='wrong_account',
            skip_reason=(
                f"Statement {filename}: wrong account number "
                f"({stmt_acct} vs Opera {opera_acct})"
            ),
        )

    return AccountMatchResult(matches=True)


# ============================================================
# Phase 3 — already-processed chain check
# ============================================================

def check_chain_complete(
    *,
    opening_balance: Optional[float],
    closing_balance: Optional[float],
    effective_reconciled_balance: Optional[float],
    fallback_reconciled_balance: Optional[float],
    bank_rec_openings: Set[float],
    filename: str,
    opening_unblocks_chain: Optional[Callable[[Optional[float]], bool]] = None,
) -> ChainCheckResult:
    """Detect already-processed statements via balance-chain logic.

    Two ways a statement can be already-processed:
      A) `closing_matches_reconciled_opening`: this statement's closing
         balance equals a known reconciled statement's opening — i.e.
         the chain has moved past it.
      B) `opening_below_reconciled`: this statement's opening is more
         than a penny below the effective reconciled balance.

    Only case (B) writes a bank_statement_imports audit row in the
    SE caller (Opera 3 writes on both branches — controlled by
    `audit_row_on_chain_match` on `validate_pdf_for_scan`).

    `opening_unblocks_chain` (Opera 3 sequential gating): when the
    immediately-prior statement is in 'imported but not reconciled'
    state, its closing balance is treated as a valid opener for the
    NEXT statement even though Opera's nk_recbal hasn't moved yet.
    If the callback returns True for this statement's opening, the
    `opening_below_reconciled` branch is suppressed (the chain-match
    branch is unaffected).
    """
    if opening_balance is None:
        return ChainCheckResult(chain_complete=False)

    eff_bal = effective_reconciled_balance
    if eff_bal is None:
        eff_bal = fallback_reconciled_balance

    chain_match = (
        closing_balance is not None
        and round(closing_balance, 2) in bank_rec_openings
    )
    below_reconciled = (
        eff_bal is not None
        and opening_balance < eff_bal - 0.01
    )

    if chain_match:
        logger.info(
            f"Statement filtered out (chain): closing £{closing_balance:,.2f} "
            "matches reconciled opening"
        )
        return ChainCheckResult(
            chain_complete=True,
            reason_kind='closing_matches_reconciled_opening',
            skip_reason=(
                f"Statement {filename}: already processed "
                "(closing matches reconciled statement's opening)"
            ),
        )

    if below_reconciled:
        # Opera 3 sequential gating: if the prior statement is
        # imported-but-not-reconciled, its closing balance is the
        # legitimate opener for THIS statement — let it through.
        if opening_unblocks_chain is not None and opening_unblocks_chain(opening_balance):
            return ChainCheckResult(chain_complete=False)
        logger.info(
            f"Statement filtered out: opening £{opening_balance:,.2f} "
            f"< reconciled £{eff_bal:,.2f}"
        )
        return ChainCheckResult(
            chain_complete=True,
            reason_kind='opening_below_reconciled',
            skip_reason=(
                f"Statement {filename}: already processed "
                f"(opening £{opening_balance:,.2f} < reconciled £{eff_bal:,.2f})"
            ),
        )

    return ChainCheckResult(chain_complete=False)


# ============================================================
# Orchestration — single call from the route handler
# ============================================================

def _info_to_attachment_updates(info: StatementInfoData) -> Dict[str, Any]:
    """Translate StatementInfoData into the att-dict field updates the
    handlers used to assign inline."""
    updates: Dict[str, Any] = {
        'period_start': info.period_start,
        'period_end': info.period_end,
        'bank_name': info.bank_name,
        'account_number': info.account_number,
        'sort_code': info.sort_code,
        'closing_balance': info.closing_balance,
    }
    if info.opening_balance is not None:
        updates['opening_balance'] = info.opening_balance
    # Always include extraction_status so downstream UIs can render a
    # consistent cell across all four scan endpoints. Pre-F9, three of
    # the four handlers omitted it on cache hit and one (scan-all-banks)
    # set 'cached'; the harmonised helper aligns on the latter.
    if info.extraction_status:
        updates['extraction_status'] = info.extraction_status
    if info.extraction_failure_reason is not None:
        updates['extraction_failure_reason'] = info.extraction_failure_reason
    if info.extraction_status == 'pending_extraction':
        updates['status'] = 'pending_extraction'
    return updates


def validate_pdf_for_scan(
    *,
    content_bytes: bytes,
    filename: str,
    cache,
    sql_connector,
    company_settings: Dict[str, Any],
    config,
    extract_on_miss: bool,
    opera_sort_code: Optional[str],
    opera_account_number: Optional[str],
    effective_reconciled_balance: Optional[float],
    fallback_reconciled_balance: Optional[float],
    bank_rec_openings: Set[float],
    opening_unblocks_chain: Optional[Callable[[Optional[float]], bool]] = None,
    audit_row_on_chain_match: bool = False,
) -> StatementValidationVerdict:
    """One-stop per-PDF validation pipeline.

    Replaces ~150 lines of in-handler code with a single call. The
    caller applies `info_updates` to its attachment dict and consults
    `is_valid` / `validation_status` / `record_already_processed`.

    Behaviour preservation contract:
      - On extraction failure the verdict stays `is_valid=True` (the
        original code didn't drop the statement on extraction failure;
        it just left the fields unset and let downstream code decide).
      - Account mismatch sets is_valid=False with status 'wrong_account'.
      - Chain match (closing == reconciled opening) sets
        is_valid=False, status='already_processed'. SE: no audit row.
        Opera 3 (audit_row_on_chain_match=True): audit row written.
      - Opening below reconciled sets is_valid=False,
        status='already_processed', record_already_processed=True.

    Opera 3 mode:
      - Pass opera_sort_code=None and opera_account_number=None to
        skip the account-match check (Opera 3 nbank may have less
        reliable sort/account data).
      - Pass `opening_unblocks_chain` for sequential gating: a
        statement whose opening matches an imported-but-not-reconciled
        prior statement's closing is allowed through.
      - Pass `audit_row_on_chain_match=True` so the chain-match branch
        also records an audit row (Opera 3 wants a row for both
        already-processed branches).
    """
    info = get_statement_info(
        content_bytes=content_bytes,
        filename=filename,
        cache=cache,
        sql_connector=sql_connector,
        company_settings=company_settings,
        config=config,
        extract_on_miss=extract_on_miss,
    )
    info_updates = _info_to_attachment_updates(info)

    # If extraction failed/deferred, skip downstream validation but
    # keep is_valid=True (matches original handler behaviour).
    if info.extraction_status in ('pending_extraction', 'failed'):
        return StatementValidationVerdict(
            is_valid=True,
            info_updates=info_updates,
            statement_opening_balance=info.opening_balance,
        )

    match = check_account_match(
        info=info,
        opera_sort_code=opera_sort_code,
        opera_account_number=opera_account_number,
        filename=filename,
    )
    if not match.matches:
        return StatementValidationVerdict(
            is_valid=False,
            validation_status=match.validation_status,
            skip_reason=match.skip_reason,
            info_updates=info_updates,
            statement_opening_balance=info.opening_balance,
        )

    chain = check_chain_complete(
        opening_balance=info.opening_balance,
        closing_balance=info.closing_balance,
        effective_reconciled_balance=effective_reconciled_balance,
        fallback_reconciled_balance=fallback_reconciled_balance,
        bank_rec_openings=bank_rec_openings,
        filename=filename,
        opening_unblocks_chain=opening_unblocks_chain,
    )
    if chain.chain_complete:
        record_audit = (
            chain.reason_kind == 'opening_below_reconciled'
            or (audit_row_on_chain_match
                and chain.reason_kind == 'closing_matches_reconciled_opening')
        )
        return StatementValidationVerdict(
            is_valid=False,
            validation_status='already_processed',
            skip_reason=chain.skip_reason,
            record_already_processed=record_audit,
            info_updates=info_updates,
            statement_opening_balance=info.opening_balance,
        )

    return StatementValidationVerdict(
        is_valid=True,
        info_updates=info_updates,
        statement_opening_balance=info.opening_balance,
    )
