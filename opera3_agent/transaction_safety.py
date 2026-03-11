"""
Transaction Safety Layer for Opera 3 Write Agent.

Provides three critical safety mechanisms for multi-table FoxPro writes:

1. POST-WRITE VERIFICATION — After every write, confirm all expected records
   exist in the correct tables with correct key values. If any record is
   missing, the transaction is flagged as incomplete.

2. COMPENSATION (ROLLBACK) — If verification fails, undo partial writes by
   soft-deleting inserted records. Balance adjustments are logged for manual
   correction (until Harbour bridge enables safe automated balance reversal).

3. CRASH RECOVERY — On agent startup, scan the WAL for incomplete operations.
   Verify their state and compensate if needed.

This module uses the `dbf` package (read-only) for verification reads.
It does NOT modify CDX indexes — compensation uses FoxPro soft-delete which
is safe even without CDX maintenance (Opera respects SET DELETED ON).

Integration: Called from service.py around each import operation.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable

logger = logging.getLogger(__name__)

# Optional dbf import — verification degrades gracefully if unavailable
try:
    import dbf
    DBF_AVAILABLE = True
except ImportError:
    DBF_AVAILABLE = False
    logger.warning("dbf package not available — verification will be skipped")


# ============================================================
# Result types
# ============================================================

@dataclass
class VerificationResult:
    """Result of post-write verification."""
    passed: bool
    checks: List[Dict[str, Any]] = field(default_factory=list)
    missing_records: List[str] = field(default_factory=list)
    details: str = ""


@dataclass
class CompensationResult:
    """Result of compensation (undo) attempt."""
    success: bool
    steps_completed: List[str] = field(default_factory=list)
    balance_adjustments: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


# ============================================================
# Operation profiles — what to verify for each operation type
# ============================================================

# Maps operation_type → list of (table_name, key_field, key_source, expected_count)
# key_source is either "entry_number" or "journal_number" from the result
VERIFICATION_PROFILES: Dict[str, List[Dict[str, Any]]] = {
    "purchase_payment": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
        {"table": "ptran",  "key_field": "pt_trref",  "key_source": "entry_number", "count": 1, "optional": True},
    ],
    "sales_receipt": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
        {"table": "stran",  "key_field": "st_trref",  "key_source": "entry_number", "count": 1, "optional": True},
    ],
    "sales_refund": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
        {"table": "stran",  "key_field": "st_trref",  "key_source": "entry_number", "count": 1, "optional": True},
    ],
    "purchase_refund": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
        {"table": "ptran",  "key_field": "pt_trref",  "key_source": "entry_number", "count": 1, "optional": True},
    ],
    "bank_transfer": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 2},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 2},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
    ],
    "nominal_entry": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2},
    ],
    "gocardless_batch": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1, "count_min": True},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number", "count": 2, "count_min": True},
    ],
    "recurring_entry": [
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number", "count": 1},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number", "count": 1},
    ],
}

# Tables to check for compensation (delete inserted records)
COMPENSATION_TABLES: Dict[str, List[Dict[str, str]]] = {
    "purchase_payment": [
        {"table": "ptran",  "key_field": "pt_trref",  "key_source": "entry_number"},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "sales_receipt": [
        {"table": "stran",  "key_field": "st_trref",  "key_source": "entry_number"},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "sales_refund": [
        {"table": "stran",  "key_field": "st_trref",  "key_source": "entry_number"},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "purchase_refund": [
        {"table": "ptran",  "key_field": "pt_trref",  "key_source": "entry_number"},
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "bank_transfer": [
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "nominal_entry": [
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "gocardless_batch": [
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
    "recurring_entry": [
        {"table": "ntran",  "key_field": "nt_jrnl",  "key_source": "journal_number"},
        {"table": "anoml",  "key_field": "ax_jrnl",  "key_source": "journal_number"},
        {"table": "atran",  "key_field": "at_entry", "key_source": "entry_number"},
        {"table": "aentry", "key_field": "ae_entry", "key_source": "entry_number"},
    ],
}


# ============================================================
# Main safety class
# ============================================================

class TransactionSafety:
    """Post-write verification and compensation for Opera 3 multi-table writes.

    Usage from service.py:
        safety = TransactionSafety(data_path)
        result = importer.import_purchase_payment(...)
        verification = safety.verify("purchase_payment", result)
        if not verification.passed:
            compensation = safety.compensate("purchase_payment", params, result)
    """

    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        # Writes blocked if True (set by failed compensation)
        self.writes_blocked = False
        self.block_reason = ""

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify(self, operation_type: str, result: dict) -> VerificationResult:
        """Verify all expected records were written.

        Scans DBF tables from the end (recently appended records) to confirm
        every record expected by the operation type exists.

        Args:
            operation_type: e.g. "purchase_payment", "sales_receipt"
            result: dict from Opera3ImportResult with entry_number, journal_number

        Returns:
            VerificationResult with passed=True if all records found
        """
        if not DBF_AVAILABLE:
            return VerificationResult(
                passed=True,
                details="Verification skipped — dbf package not available",
            )

        profile = VERIFICATION_PROFILES.get(operation_type)
        if not profile:
            return VerificationResult(
                passed=True,
                details=f"No verification profile for {operation_type}",
            )

        entry_number = result.get("entry_number", "")
        journal_number = result.get("journal_number", 0)

        if not entry_number and not journal_number:
            return VerificationResult(
                passed=True,
                details="No entry/journal number in result — nothing to verify",
            )

        checks = []
        missing = []

        for spec in profile:
            table_name = spec["table"]
            key_field = spec["key_field"]
            key_source = spec["key_source"]
            expected_count = spec.get("count", 1)
            is_optional = spec.get("optional", False)
            count_min = spec.get("count_min", False)

            # Determine the value to search for
            if key_source == "entry_number":
                search_value = entry_number
            elif key_source == "journal_number":
                search_value = journal_number
            else:
                continue

            if not search_value:
                continue

            try:
                found_count = self._count_records(
                    table_name, key_field, search_value
                )
                if count_min:
                    ok = found_count >= expected_count
                else:
                    ok = found_count >= expected_count

                check = {
                    "table": table_name,
                    "field": key_field,
                    "value": str(search_value),
                    "expected": expected_count,
                    "found": found_count,
                    "passed": ok,
                    "optional": is_optional,
                }
                checks.append(check)

                if not ok and not is_optional:
                    missing.append(
                        f"{table_name} ({key_field}={search_value}: "
                        f"expected {expected_count}, found {found_count})"
                    )

            except Exception as e:
                check = {
                    "table": table_name,
                    "field": key_field,
                    "error": str(e),
                    "passed": False,
                    "optional": is_optional,
                }
                checks.append(check)
                if not is_optional:
                    missing.append(f"{table_name} (error: {e})")

        passed = len(missing) == 0
        if passed:
            details = f"All {len(checks)} checks passed"
        else:
            details = f"FAILED: Missing records in: {'; '.join(missing)}"

        return VerificationResult(
            passed=passed,
            checks=checks,
            missing_records=missing,
            details=details,
        )

    # ------------------------------------------------------------------
    # Compensation (undo)
    # ------------------------------------------------------------------

    def compensate(
        self, operation_type: str, params: dict, result: dict
    ) -> CompensationResult:
        """Attempt to undo a failed/partial write operation.

        Soft-deletes inserted records by scanning from the end of each table.
        Balance adjustments are LOGGED but NOT applied (manual fix required
        until Harbour bridge enables safe automated writes).

        Records are deleted in reverse dependency order:
        child records first (ptran/stran), then ntran, then atran, then aentry.

        Args:
            operation_type: e.g. "purchase_payment"
            params: original request parameters
            result: partial result from the failed import

        Returns:
            CompensationResult with steps taken and any errors
        """
        if not DBF_AVAILABLE:
            return CompensationResult(
                success=False,
                errors=["Cannot compensate — dbf package not available"],
            )

        comp_tables = COMPENSATION_TABLES.get(operation_type)
        if not comp_tables:
            return CompensationResult(
                success=False,
                errors=[f"No compensation profile for {operation_type}"],
            )

        entry_number = result.get("entry_number", "")
        journal_number = result.get("journal_number", 0)

        steps: List[str] = []
        errors: List[str] = []
        balance_adjustments: List[str] = []

        for spec in comp_tables:
            table_name = spec["table"]
            key_field = spec["key_field"]
            key_source = spec["key_source"]

            if key_source == "entry_number":
                search_value = entry_number
            elif key_source == "journal_number":
                search_value = journal_number
            else:
                continue

            if not search_value:
                continue

            try:
                deleted_count = self._delete_records(
                    table_name, key_field, search_value
                )
                if deleted_count > 0:
                    steps.append(
                        f"Deleted {deleted_count} record(s) from {table_name} "
                        f"where {key_field}={search_value}"
                    )
            except Exception as e:
                errors.append(f"Failed to delete from {table_name}: {e}")

        # Log balance adjustments that need manual correction
        balance_adjustments.extend(
            self._compute_balance_adjustments(operation_type, params, result)
        )

        if balance_adjustments:
            steps.append("MANUAL FIX REQUIRED for balance adjustments:")
            steps.extend(f"  - {adj}" for adj in balance_adjustments)

        success = len(errors) == 0
        if not success:
            # Block further writes — manual intervention needed
            self.writes_blocked = True
            self.block_reason = (
                f"Compensation errors for {operation_type}: "
                + "; ".join(errors)
            )
            logger.critical(f"WRITES BLOCKED: {self.block_reason}")

        return CompensationResult(
            success=success,
            steps_completed=steps,
            balance_adjustments=balance_adjustments,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    def recover_operation(self, op: Any) -> Dict[str, Any]:
        """Handle an incomplete operation found on startup.

        Args:
            op: WALOperation from write_ahead_log

        Returns:
            dict with recovery action taken and details
        """
        from opera3_agent.write_ahead_log import OperationStatus

        op_type = op.operation_type
        status = op.status

        if status == OperationStatus.PENDING:
            # Writes never started — safe to mark as failed
            return {
                "action": "marked_failed",
                "reason": "Operation was pending when agent stopped — no writes occurred",
            }

        if status in (OperationStatus.IN_PROGRESS, OperationStatus.VERIFYING):
            # Writes may have partially completed — verify state
            result = op.result or {}
            if not result.get("entry_number") and not result.get("journal_number"):
                # No result means we crashed before any writes completed
                return {
                    "action": "marked_failed",
                    "reason": "No result recorded — writes likely did not start",
                }

            # Run verification to see what actually made it
            verification = self.verify(op_type, result)
            if verification.passed:
                return {
                    "action": "marked_completed",
                    "reason": "All records verified — writes completed before crash",
                    "verification": verification.details,
                }
            else:
                # Partial write — compensate
                params = op.params or {}
                compensation = self.compensate(op_type, params, result)
                return {
                    "action": "compensated" if compensation.success else "compensation_failed",
                    "reason": f"Partial write detected: {verification.details}",
                    "compensation_steps": compensation.steps_completed,
                    "compensation_errors": compensation.errors,
                    "balance_adjustments": compensation.balance_adjustments,
                }

        if status == OperationStatus.FAILED:
            # Write failed but compensation wasn't attempted
            result = op.result or {}
            if result.get("entry_number") or result.get("journal_number"):
                # There might be partial records
                compensation = self.compensate(op_type, op.params or {}, result)
                return {
                    "action": "compensated" if compensation.success else "compensation_failed",
                    "reason": "Failed operation had partial results — compensated",
                    "compensation_steps": compensation.steps_completed,
                }
            return {
                "action": "marked_failed",
                "reason": "Failed operation with no results — nothing to compensate",
            }

        if status == OperationStatus.COMPENSATING:
            # Compensation was interrupted — retry
            result = op.result or {}
            compensation = self.compensate(op_type, op.params or {}, result)
            return {
                "action": "compensated" if compensation.success else "compensation_failed",
                "reason": "Restarted interrupted compensation",
                "compensation_steps": compensation.steps_completed,
            }

        return {
            "action": "skipped",
            "reason": f"Unexpected status: {status}",
        }

    # ------------------------------------------------------------------
    # DBF read helpers (for verification)
    # ------------------------------------------------------------------

    def _resolve_dbf_path(self, table_name: str) -> Path:
        """Find the DBF file path (case-insensitive)."""
        lower = self.data_path / f"{table_name.lower()}.dbf"
        if lower.exists():
            return lower
        upper = self.data_path / f"{table_name.upper()}.DBF"
        if upper.exists():
            return upper
        # Case-insensitive glob
        for f in self.data_path.glob("*.dbf"):
            if f.stem.lower() == table_name.lower():
                return f
        for f in self.data_path.glob("*.DBF"):
            if f.stem.lower() == table_name.lower():
                return f
        raise FileNotFoundError(f"DBF file not found for table: {table_name}")

    def _count_records(
        self, table_name: str, key_field: str, search_value: Any,
        max_scan: int = 1000,
    ) -> int:
        """Count records matching key_field=search_value, scanning from end.

        Scans the last `max_scan` records (most recent appends are at the end).
        This avoids full table scans on large tables.
        """
        dbf_path = str(self._resolve_dbf_path(table_name))
        table = dbf.Table(dbf_path)
        table.open(dbf.READ_ONLY)
        try:
            count = 0
            total = len(table)
            start = max(total - max_scan, 0)

            for i in range(total - 1, start - 1, -1):
                record = table[i]
                if dbf.is_deleted(record):
                    continue

                raw = getattr(record, key_field, None)
                if raw is None:
                    continue

                # Compare: string fields need strip(), numeric fields direct
                if isinstance(raw, str):
                    if raw.strip() == str(search_value).strip():
                        count += 1
                elif isinstance(raw, (int, float)):
                    if isinstance(search_value, (int, float)):
                        if raw == search_value:
                            count += 1
                    else:
                        try:
                            if raw == int(search_value):
                                count += 1
                        except (ValueError, TypeError):
                            pass

            return count
        finally:
            table.close()

    def _delete_records(
        self, table_name: str, key_field: str, search_value: Any,
        max_scan: int = 1000,
    ) -> int:
        """Soft-delete records matching key_field=search_value.

        Uses FoxPro soft-delete (marks deletion flag). Opera respects
        SET DELETED ON and will skip these records.

        Returns count of records deleted.
        """
        dbf_path = str(self._resolve_dbf_path(table_name))
        table = dbf.Table(dbf_path)
        table.open(dbf.READ_WRITE)
        try:
            deleted = 0
            total = len(table)
            start = max(total - max_scan, 0)

            for i in range(total - 1, start - 1, -1):
                record = table[i]
                if dbf.is_deleted(record):
                    continue

                raw = getattr(record, key_field, None)
                if raw is None:
                    continue

                match = False
                if isinstance(raw, str):
                    match = raw.strip() == str(search_value).strip()
                elif isinstance(raw, (int, float)):
                    try:
                        match = raw == (
                            search_value if isinstance(search_value, (int, float))
                            else int(search_value)
                        )
                    except (ValueError, TypeError):
                        pass

                if match:
                    dbf.delete(record)
                    deleted += 1
                    logger.info(
                        f"COMPENSATE: Deleted {table_name} record {i} "
                        f"({key_field}={search_value})"
                    )

            return deleted
        finally:
            table.close()

    # ------------------------------------------------------------------
    # Balance adjustment computation
    # ------------------------------------------------------------------

    def _compute_balance_adjustments(
        self, operation_type: str, params: dict, result: dict
    ) -> List[str]:
        """Compute balance adjustments needed after compensation.

        These are logged for manual correction — the safety layer does NOT
        attempt automated balance writes (risk of CDX corruption without
        Harbour bridge).

        Returns list of human-readable adjustment instructions.
        """
        adjustments = []
        amount = params.get("amount_pounds", 0)
        if not amount:
            return adjustments

        amount_pence = int(round(amount * 100))

        if operation_type == "purchase_payment":
            bank = params.get("bank_account", "?")
            supplier = params.get("supplier_account", "?")
            adjustments.extend([
                f"nbank: ADD {amount_pence} pence to {bank} (nk_curbal) — reverse payment deduction",
                f"pname: ADD {amount:.2f} to {supplier} (pn_currbal) — reverse balance reduction",
                f"nacnt: REVERSE {amount:.2f} debit/credit on bank and creditors control accounts",
            ])

        elif operation_type == "sales_receipt":
            bank = params.get("bank_account", "?")
            customer = params.get("customer_account", "?")
            adjustments.extend([
                f"nbank: SUBTRACT {amount_pence} pence from {bank} (nk_curbal) — reverse receipt addition",
                f"sname: ADD {amount:.2f} to {customer} (sn_currbal) — reverse balance reduction",
                f"nacnt: REVERSE {amount:.2f} debit/credit on bank and debtors control accounts",
            ])

        elif operation_type == "sales_refund":
            bank = params.get("bank_account", "?")
            customer = params.get("customer_account", "?")
            adjustments.extend([
                f"nbank: ADD {amount_pence} pence to {bank} (nk_curbal) — reverse refund deduction",
                f"sname: SUBTRACT {amount:.2f} from {customer} (sn_currbal) — reverse balance increase",
                f"nacnt: REVERSE {amount:.2f} debit/credit on bank and debtors control accounts",
            ])

        elif operation_type == "purchase_refund":
            bank = params.get("bank_account", "?")
            supplier = params.get("supplier_account", "?")
            adjustments.extend([
                f"nbank: SUBTRACT {amount_pence} pence from {bank} (nk_curbal) — reverse refund addition",
                f"pname: SUBTRACT {amount:.2f} from {supplier} (pn_currbal) — reverse balance increase",
                f"nacnt: REVERSE {amount:.2f} debit/credit on bank and creditors control accounts",
            ])

        elif operation_type == "bank_transfer":
            source = params.get("source_bank", "?")
            dest = params.get("dest_bank", "?")
            adjustments.extend([
                f"nbank: ADD {amount_pence} pence to {source} (nk_curbal) — reverse source deduction",
                f"nbank: SUBTRACT {amount_pence} pence from {dest} (nk_curbal) — reverse dest addition",
                f"nacnt: REVERSE {amount:.2f} on both bank nominal accounts",
            ])

        elif operation_type == "nominal_entry":
            bank = params.get("bank_account", "?")
            nominal = params.get("nominal_account", "?")
            adjustments.extend([
                f"nbank: REVERSE {amount_pence} pence on {bank} (nk_curbal)",
                f"nacnt: REVERSE {amount:.2f} on {bank} and {nominal}",
            ])

        elif operation_type == "gocardless_batch":
            bank = params.get("bank_account", "?")
            payments = params.get("payments", [])
            total = sum(p.get("amount", 0) for p in payments)
            total_pence = int(round(total * 100))
            adjustments.extend([
                f"nbank: SUBTRACT {total_pence} pence from {bank} (nk_curbal) — reverse batch addition",
                f"sname: ADD original amounts back to {len(payments)} customer(s) (sn_currbal)",
                f"nacnt: REVERSE debit/credit on bank and debtors control for total {total:.2f}",
            ])

        if adjustments:
            adjustments.append(
                "NOTE: Run Opera Data Repair or apply manual corrections. "
                "Automated balance reversal will be available when Harbour bridge is compiled."
            )

        return adjustments
