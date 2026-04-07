# Generic Import Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic integration engine that imports transactions into Opera from any external source (CSV, Excel, API, email) using reusable field-mapping templates, driven by transaction snapshots.

**Architecture:** Transaction type registry (populated from snapshots) defines what fields Opera needs. Import templates map external data to those fields. A validation pipeline checks every row against Opera before posting, using existing `OperaSQLImport` methods via an adapter layer. SQLite stores templates, history, and audit trails.

**Tech Stack:** Python/FastAPI (backend), React/TypeScript (frontend), SQLite (templates/audit), existing `OperaSQLImport` (posting)

**Spec:** `docs/superpowers/specs/2026-04-07-generic-import-engine-design.md`

**Key dependency:** The transaction snapshot tool must capture each transaction type before it becomes available in the import engine. Tasks 1-3 enhance the snapshot tool. Tasks 4+ build the engine.

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `sql_rag/import_registry.py` | Transaction type registry — defines required/optional/auto fields per type |
| `sql_rag/import_engine.py` | Core pipeline: parse, map, transform, validate |
| `sql_rag/import_templates_db.py` | SQLite schema for templates, import history, audit trail |
| `sql_rag/import_adapter_opera_se.py` | Opera SE adapter — wraps `OperaSQLImport` methods |
| `apps/generic_import/api/__init__.py` | Module init |
| `apps/generic_import/api/routes.py` | API endpoints: templates CRUD, upload, validate, import |
| `frontend/src/pages/GenericImport.tsx` | Main import page: upload, map, preview, validate, import |
| `frontend/src/pages/ImportTemplates.tsx` | Template builder and management |
| `tests/test_import_registry.py` | Registry tests |
| `tests/test_import_engine.py` | Pipeline tests |
| `tests/test_import_adapter.py` | Adapter tests |

### Modified Files

| File | Change |
|------|--------|
| `apps/transaction_snapshot/api/routes.py` | Add registry entry generation endpoint |
| `api/main.py` | Register new router |
| `frontend/src/App.tsx` | Add import routes |
| `frontend/src/components/Layout.tsx` | Add menu items |

---

## Task 1: Snapshot Tool — Generate Registry Entry from Snapshot

Enhance the snapshot tool to output a structured registry entry from a captured transaction, bridging snapshot data to import engine requirements.

**Files:**
- Modify: `apps/transaction_snapshot/api/routes.py`
- Test: `tests/test_import_registry.py`

- [ ] **Step 1: Write the test for registry entry structure**

```python
# tests/test_import_registry.py
import pytest
from sql_rag.import_registry import RegistryEntry, ImportField

def test_registry_entry_has_required_structure():
    entry = RegistryEntry(
        type_id="sales_receipt",
        label="Sales Receipt",
        category="cashbook",
        import_method="import_sales_receipt",
        user_fields=[
            ImportField(name="customer_account", label="Customer Code", required=True, field_type="string", lookup_table="sname"),
            ImportField(name="amount_pounds", label="Amount (£)", required=True, field_type="decimal"),
            ImportField(name="reference", label="Reference", required=False, field_type="string", max_length=30),
            ImportField(name="post_date", label="Date", required=True, field_type="date"),
        ],
        default_fields=[
            ImportField(name="bank_account", label="Bank Account", required=True, field_type="string", source="template"),
            ImportField(name="cbtype", label="Type Code", required=False, field_type="string", source="template"),
            ImportField(name="input_by", label="Input By", required=False, field_type="string", default="IMPORT"),
        ],
        auto_fields=["entry_number", "journal_number", "unique_id", "row_id"],
        tables_written=["aentry", "atran", "stran", "ntran", "anoml", "nbank", "nacnt", "sname"],
    )
    assert entry.type_id == "sales_receipt"
    assert len(entry.user_fields) == 4
    assert entry.user_fields[0].required is True
    assert entry.import_method == "import_sales_receipt"
    assert "aentry" in entry.tables_written
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_import_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'sql_rag.import_registry'`

- [ ] **Step 3: Create the registry module with dataclasses**

```python
# sql_rag/import_registry.py
"""
Transaction Type Registry for the Generic Import Engine.

Each registry entry defines what fields Opera needs for a transaction type,
populated from snapshot data and linked to an OperaSQLImport method.
Only types with completed snapshots AND import methods are available.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import json
from pathlib import Path


@dataclass
class ImportField:
    """A single field in a registry entry."""
    name: str
    label: str
    required: bool = True
    field_type: str = "string"  # string, decimal, date, integer
    max_length: Optional[int] = None
    lookup_table: Optional[str] = None  # Opera table for validation/suggestions
    source: Optional[str] = None  # "template" = set in template defaults, "user" = from import data
    default: Optional[str] = None


@dataclass
class DuplicateDetection:
    """How to detect duplicate transactions for this type."""
    match_on: List[str] = field(default_factory=list)
    date_range_days: int = 7


@dataclass
class RegistryEntry:
    """Defines a transaction type available for import."""
    type_id: str
    label: str
    category: str
    import_method: str
    user_fields: List[ImportField] = field(default_factory=list)
    default_fields: List[ImportField] = field(default_factory=list)
    auto_fields: List[str] = field(default_factory=list)
    tables_written: List[str] = field(default_factory=list)
    duplicate_detection: Optional[DuplicateDetection] = None
    description: str = ""
    validate_only_supported: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for API responses and JSON storage."""
        return {
            "type_id": self.type_id,
            "label": self.label,
            "category": self.category,
            "import_method": self.import_method,
            "description": self.description,
            "user_fields": [
                {k: v for k, v in f.__dict__.items() if v is not None}
                for f in self.user_fields
            ],
            "default_fields": [
                {k: v for k, v in f.__dict__.items() if v is not None}
                for f in self.default_fields
            ],
            "auto_fields": self.auto_fields,
            "tables_written": self.tables_written,
            "validate_only_supported": self.validate_only_supported,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RegistryEntry':
        """Deserialize from JSON."""
        return cls(
            type_id=data["type_id"],
            label=data["label"],
            category=data["category"],
            import_method=data["import_method"],
            description=data.get("description", ""),
            user_fields=[ImportField(**f) for f in data.get("user_fields", [])],
            default_fields=[ImportField(**f) for f in data.get("default_fields", [])],
            auto_fields=data.get("auto_fields", []),
            tables_written=data.get("tables_written", []),
            validate_only_supported=data.get("validate_only_supported", True),
        )


class ImportRegistry:
    """
    Registry of available transaction types for the import engine.

    Entries are stored as JSON files in the registry directory.
    Only types with both a snapshot AND an import method are enabled.
    """

    def __init__(self, registry_dir: str = None):
        if registry_dir is None:
            registry_dir = str(Path(__file__).parent.parent / "data" / "import_registry")
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self._entries: Dict[str, RegistryEntry] = {}
        self._load_entries()

    def _load_entries(self):
        """Load all registry entries from JSON files."""
        self._entries = {}
        for f in self.registry_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                entry = RegistryEntry.from_dict(data)
                self._entries[entry.type_id] = entry
            except Exception:
                pass

    def get(self, type_id: str) -> Optional[RegistryEntry]:
        return self._entries.get(type_id)

    def list_all(self) -> List[RegistryEntry]:
        return list(self._entries.values())

    def list_by_category(self, category: str) -> List[RegistryEntry]:
        return [e for e in self._entries.values() if e.category == category]

    def save_entry(self, entry: RegistryEntry):
        """Save a registry entry to disk."""
        path = self.registry_dir / f"{entry.type_id}.json"
        path.write_text(json.dumps(entry.to_dict(), indent=2))
        self._entries[entry.type_id] = entry

    def delete_entry(self, type_id: str):
        path = self.registry_dir / f"{type_id}.json"
        if path.exists():
            path.unlink()
        self._entries.pop(type_id, None)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_import_registry.py -v`
Expected: PASS

- [ ] **Step 5: Add endpoint to generate registry entry from snapshot**

Add to `apps/transaction_snapshot/api/routes.py`:

```python
@router.post("/api/transaction-snapshot/generate-registry-entry")
async def generate_registry_entry(
    library_entry_id: str = Body(...),
    type_id: str = Body(...),
    label: str = Body(...),
    category: str = Body(...),
    import_method: str = Body(...),
    user_fields: List[Dict] = Body(default=[]),
    default_fields: List[Dict] = Body(default=[]),
):
    """
    Generate an import registry entry from a snapshot library entry.

    The snapshot provides tables_written and field analysis.
    The user provides the import_method mapping and field classifications.
    Returns the registry entry JSON for review before saving.
    """
    # Load snapshot library entry
    library_dir = Path(__file__).parent.parent.parent.parent / "docs" / "opera-transaction-library"
    matches = list(library_dir.glob(f"*{library_entry_id}*"))
    if not matches:
        return {"success": False, "error": f"Library entry not found: {library_entry_id}"}

    entry_data = json.loads(matches[0].read_text())

    # Extract tables written from snapshot changes
    tables_written = list(set(
        change.get("table", "") for change in entry_data.get("changes", [])
        if change.get("type") in ("row_added", "row_modified")
    ))

    from sql_rag.import_registry import RegistryEntry, ImportField

    registry_entry = RegistryEntry(
        type_id=type_id,
        label=label,
        category=category,
        import_method=import_method,
        user_fields=[ImportField(**f) for f in user_fields],
        default_fields=[ImportField(**f) for f in default_fields],
        auto_fields=["entry_number", "journal_number", "unique_id", "row_id"],
        tables_written=tables_written,
        description=entry_data.get("description", ""),
    )

    return {
        "success": True,
        "registry_entry": registry_entry.to_dict(),
        "source_snapshot": library_entry_id,
        "message": "Review this entry and POST to /api/imports/registry to save"
    }
```

- [ ] **Step 6: Commit**

```bash
git add sql_rag/import_registry.py tests/test_import_registry.py apps/transaction_snapshot/api/routes.py
git commit -m "feat: add import registry and snapshot-to-registry generation"
```

---

## Task 2: Import Templates Database

SQLite schema for templates, import history, and audit trail.

**Files:**
- Create: `sql_rag/import_templates_db.py`
- Test: `tests/test_import_engine.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_import_engine.py
import pytest
from sql_rag.import_templates_db import ImportTemplatesDB

@pytest.fixture
def db(tmp_path):
    return ImportTemplatesDB(str(tmp_path / "imports_test.db"))

def test_create_and_retrieve_template(db):
    template_id = db.create_template(
        name="Sage Payroll",
        transaction_type="nominal_journal",
        source_type="csv",
        mappings=[{"source": "Amount", "target": "amount_pounds"}],
        defaults={"bank_account": "CURR"},
        transforms=[],
        error_handling="stop_on_error",
    )
    assert template_id > 0
    template = db.get_template(template_id)
    assert template["name"] == "Sage Payroll"
    assert template["transaction_type"] == "nominal_journal"

def test_record_import_history(db):
    record_id = db.record_import(
        template_id=1,
        template_name="Test",
        transaction_type="sales_receipt",
        source_filename="receipts.csv",
        total_rows=10,
        rows_posted=8,
        rows_skipped=1,
        rows_failed=1,
        status="partial",
        imported_by="admin",
    )
    assert record_id > 0
    history = db.get_import_history(limit=10)
    assert len(history) == 1
    assert history[0]["rows_posted"] == 8
```

- [ ] **Step 2: Run test — expect fail**

Run: `pytest tests/test_import_engine.py -v`

- [ ] **Step 3: Implement the database module**

```python
# sql_rag/import_templates_db.py
"""
SQLite database for import templates, history, and audit trail.
Follows the same pattern as supplier_statement_db.py.
"""
import sqlite3
import json
from typing import List, Dict, Any, Optional
from pathlib import Path


class ImportTemplatesDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                target_system TEXT DEFAULT 'opera_se',
                source_type TEXT DEFAULT 'csv',
                mappings_json TEXT,
                defaults_json TEXT,
                transforms_json TEXT,
                error_handling TEXT DEFAULT 'stop_on_error',
                duplicate_handling TEXT DEFAULT 'warn',
                warning_handling TEXT DEFAULT 'require_acknowledgement',
                auto_approve INTEGER DEFAULT 0,
                created_by TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER,
                template_name TEXT,
                transaction_type TEXT,
                source_filename TEXT,
                source_type TEXT DEFAULT 'csv',
                total_rows INTEGER DEFAULT 0,
                rows_posted INTEGER DEFAULT 0,
                rows_skipped INTEGER DEFAULT 0,
                rows_failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                audit_report_json TEXT,
                imported_by TEXT,
                imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()

    def create_template(self, name: str, transaction_type: str, source_type: str = "csv",
                        mappings: List[Dict] = None, defaults: Dict = None,
                        transforms: List[Dict] = None, error_handling: str = "stop_on_error",
                        created_by: str = None) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO import_templates (name, transaction_type, source_type,
                mappings_json, defaults_json, transforms_json, error_handling, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, transaction_type, source_type,
              json.dumps(mappings or []), json.dumps(defaults or {}),
              json.dumps(transforms or []), error_handling, created_by))
        conn.commit()
        template_id = cursor.lastrowid
        conn.close()
        return template_id

    def get_template(self, template_id: int) -> Optional[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM import_templates WHERE id = ?", (template_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        result = dict(row)
        result["mappings"] = json.loads(result.pop("mappings_json") or "[]")
        result["defaults"] = json.loads(result.pop("defaults_json") or "{}")
        result["transforms"] = json.loads(result.pop("transforms_json") or "[]")
        return result

    def list_templates(self) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, transaction_type, source_type, created_at FROM import_templates ORDER BY name")
        templates = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return templates

    def update_template(self, template_id: int, **kwargs):
        conn = self._get_connection()
        cursor = conn.cursor()
        sets = []
        values = []
        for key, val in kwargs.items():
            if key in ("mappings", "defaults", "transforms"):
                sets.append(f"{key}_json = ?")
                values.append(json.dumps(val))
            else:
                sets.append(f"{key} = ?")
                values.append(val)
        sets.append("updated_at = CURRENT_TIMESTAMP")
        values.append(template_id)
        cursor.execute(f"UPDATE import_templates SET {', '.join(sets)} WHERE id = ?", values)
        conn.commit()
        conn.close()

    def delete_template(self, template_id: int):
        conn = self._get_connection()
        conn.execute("DELETE FROM import_templates WHERE id = ?", (template_id,))
        conn.commit()
        conn.close()

    def record_import(self, template_id: int = None, template_name: str = "",
                      transaction_type: str = "", source_filename: str = "",
                      total_rows: int = 0, rows_posted: int = 0,
                      rows_skipped: int = 0, rows_failed: int = 0,
                      status: str = "pending", audit_report: Dict = None,
                      imported_by: str = None) -> int:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO import_history (template_id, template_name, transaction_type,
                source_filename, total_rows, rows_posted, rows_skipped, rows_failed,
                status, audit_report_json, imported_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (template_id, template_name, transaction_type, source_filename,
              total_rows, rows_posted, rows_skipped, rows_failed, status,
              json.dumps(audit_report) if audit_report else None, imported_by))
        conn.commit()
        record_id = cursor.lastrowid
        conn.close()
        return record_id

    def get_import_history(self, limit: int = 50) -> List[Dict]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM import_history ORDER BY imported_at DESC LIMIT ?", (limit,))
        history = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return history
```

- [ ] **Step 4: Run tests — expect pass**

Run: `pytest tests/test_import_engine.py -v`

- [ ] **Step 5: Commit**

```bash
git add sql_rag/import_templates_db.py tests/test_import_engine.py
git commit -m "feat: add import templates SQLite database"
```

---

## Task 3: Import Engine Core — Parse, Map, Validate Pipeline

The heart of the system: reads source data, applies mappings, validates against Opera.

**Files:**
- Create: `sql_rag/import_engine.py`
- Test: `tests/test_import_engine.py` (append)

- [ ] **Step 1: Write tests for the pipeline**

```python
# Append to tests/test_import_engine.py
from sql_rag.import_engine import ImportEngine, ParsedRow, ValidationResult
from sql_rag.import_registry import RegistryEntry, ImportField
import csv, io

@pytest.fixture
def sales_receipt_entry():
    return RegistryEntry(
        type_id="sales_receipt",
        label="Sales Receipt",
        category="cashbook",
        import_method="import_sales_receipt",
        user_fields=[
            ImportField(name="customer_account", label="Customer Code", required=True, field_type="string"),
            ImportField(name="amount_pounds", label="Amount", required=True, field_type="decimal"),
            ImportField(name="post_date", label="Date", required=True, field_type="date"),
            ImportField(name="reference", label="Reference", required=False, field_type="string"),
        ],
        default_fields=[
            ImportField(name="bank_account", label="Bank", required=True, field_type="string", source="template"),
        ],
        auto_fields=["entry_number", "journal_number"],
    )

def test_parse_csv_with_mappings(sales_receipt_entry):
    csv_data = "Customer,Amt,Date,Ref\nSMIT001,100.50,15/04/2026,INV001\nJONE002,200.00,16/04/2026,INV002\n"
    mappings = [
        {"source": "Customer", "target": "customer_account"},
        {"source": "Amt", "target": "amount_pounds"},
        {"source": "Date", "target": "post_date"},
        {"source": "Ref", "target": "reference"},
    ]
    defaults = {"bank_account": "CURR"}
    transforms = [{"field": "post_date", "transform": "date_format", "params": {"from": "DD/MM/YYYY"}}]

    engine = ImportEngine(registry_entry=sales_receipt_entry)
    rows = engine.parse_csv(csv_data, mappings, defaults, transforms)

    assert len(rows) == 2
    assert rows[0].data["customer_account"] == "SMIT001"
    assert rows[0].data["amount_pounds"] == 100.50
    assert rows[0].data["bank_account"] == "CURR"
    assert rows[0].data["post_date"] == "2026-04-15"

def test_validate_missing_required_field(sales_receipt_entry):
    engine = ImportEngine(registry_entry=sales_receipt_entry)
    row = ParsedRow(row_number=1, data={"customer_account": "SMIT001", "bank_account": "CURR"})
    result = engine.validate_row(row)
    assert not result.is_valid
    assert any("Amount" in issue["message"] for issue in result.issues)

def test_validate_valid_row(sales_receipt_entry):
    engine = ImportEngine(registry_entry=sales_receipt_entry)
    row = ParsedRow(row_number=1, data={
        "customer_account": "SMIT001",
        "amount_pounds": 100.50,
        "post_date": "2026-04-15",
        "reference": "INV001",
        "bank_account": "CURR",
    })
    result = engine.validate_row(row)
    assert result.is_valid
```

- [ ] **Step 2: Run test — expect fail**

Run: `pytest tests/test_import_engine.py::test_parse_csv_with_mappings -v`

- [ ] **Step 3: Implement ImportEngine**

```python
# sql_rag/import_engine.py
"""
Generic Import Engine — core pipeline.

Parses source data, applies column mappings and transformations,
validates rows against registry requirements. Does NOT post to Opera —
that's the adapter's job.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
import io
import re

from sql_rag.import_registry import RegistryEntry, ImportField


@dataclass
class ParsedRow:
    """A single row after parsing and mapping."""
    row_number: int
    data: Dict[str, Any] = field(default_factory=dict)
    raw_data: Dict[str, str] = field(default_factory=dict)


@dataclass
class ValidationIssue:
    """A single validation issue on a row."""
    row_number: int
    severity: str  # "error", "warning", "info"
    message: str
    suggestion: str = ""
    field_name: str = ""


@dataclass
class ValidationResult:
    """Validation result for a single row."""
    row_number: int
    is_valid: bool = True
    issues: List[Dict] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Result of the full parse + validate pipeline."""
    rows: List[ParsedRow] = field(default_factory=list)
    validation: List[ValidationResult] = field(default_factory=list)
    total_rows: int = 0
    errors: int = 0
    warnings: int = 0
    ready: int = 0


class ImportEngine:
    """
    Core import pipeline: parse → map → transform → validate.

    Does not know about Opera — validation against the target system
    is done by the adapter. This class handles structural validation only
    (required fields present, correct types, transformations applied).
    """

    # Supported named transformations
    TRANSFORMS = {
        "date_format": "_transform_date",
        "multiply": "_transform_multiply",
        "truncate": "_transform_truncate",
        "strip": "_transform_strip",
        "uppercase": "_transform_uppercase",
        "pad": "_transform_pad",
        "default_if_empty": "_transform_default_if_empty",
    }

    def __init__(self, registry_entry: RegistryEntry):
        self.entry = registry_entry

    def parse_csv(self, csv_data: str, mappings: List[Dict],
                  defaults: Dict[str, Any] = None,
                  transforms: List[Dict] = None) -> List[ParsedRow]:
        """Parse CSV string, apply mappings, defaults, and transforms."""
        defaults = defaults or {}
        transforms = transforms or []

        reader = csv.DictReader(io.StringIO(csv_data))
        rows = []

        for i, raw_row in enumerate(reader, start=1):
            data = {}
            # Apply column mappings
            for mapping in mappings:
                source_col = mapping["source"]
                target_field = mapping["target"]
                if source_col in raw_row:
                    data[target_field] = raw_row[source_col]

            # Apply defaults for missing fields
            for key, val in defaults.items():
                if key not in data or not data[key]:
                    data[key] = val

            # Apply transforms
            for t in transforms:
                field_name = t.get("field")
                transform_name = t.get("transform")
                params = t.get("params", {})
                if field_name in data and transform_name in self.TRANSFORMS:
                    method = getattr(self, self.TRANSFORMS[transform_name])
                    data[field_name] = method(data[field_name], params)

            # Type coercion based on registry field types
            all_fields = self.entry.user_fields + self.entry.default_fields
            for f in all_fields:
                if f.name in data and data[f.name] is not None:
                    data[f.name] = self._coerce_type(data[f.name], f.field_type)

            rows.append(ParsedRow(row_number=i, data=data, raw_data=dict(raw_row)))

        return rows

    def validate_row(self, row: ParsedRow) -> ValidationResult:
        """Validate a parsed row against registry requirements (structural only)."""
        result = ValidationResult(row_number=row.row_number)
        all_fields = self.entry.user_fields + self.entry.default_fields

        for f in all_fields:
            val = row.data.get(f.name)
            if f.required and (val is None or val == ""):
                result.is_valid = False
                result.issues.append({
                    "severity": "error",
                    "field": f.name,
                    "message": f"Row {row.row_number}: {f.label} is required but missing",
                    "suggestion": f"Add {f.label} to your data or set a default in the template",
                })
            elif val is not None and f.max_length and isinstance(val, str) and len(val) > f.max_length:
                result.issues.append({
                    "severity": "warning",
                    "field": f.name,
                    "message": f"Row {row.row_number}: {f.label} is {len(val)} chars (max {f.max_length})",
                    "suggestion": f"Value will be truncated to {f.max_length} characters",
                })
            elif val is not None and f.field_type == "decimal":
                try:
                    float(val)
                except (ValueError, TypeError):
                    result.is_valid = False
                    result.issues.append({
                        "severity": "error",
                        "field": f.name,
                        "message": f"Row {row.row_number}: {f.label} '{val}' is not a valid number",
                        "suggestion": "Check for currency symbols, commas, or text in this column",
                    })

        return result

    def validate_all(self, rows: List[ParsedRow]) -> PipelineResult:
        """Validate all rows and return a pipeline result."""
        pipeline = PipelineResult(rows=rows, total_rows=len(rows))
        for row in rows:
            result = self.validate_row(row)
            pipeline.validation.append(result)
            if not result.is_valid:
                pipeline.errors += 1
            elif any(i["severity"] == "warning" for i in result.issues):
                pipeline.warnings += 1
            else:
                pipeline.ready += 1
        return pipeline

    # --- Transformations ---

    @staticmethod
    def _transform_date(value: str, params: Dict) -> str:
        """Convert date string to YYYY-MM-DD."""
        from_fmt = params.get("from", "DD/MM/YYYY")
        py_fmt = from_fmt.replace("DD", "%d").replace("MM", "%m").replace("YYYY", "%Y").replace("YY", "%y")
        try:
            dt = datetime.strptime(str(value).strip(), py_fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return value  # Return as-is, validation will catch it

    @staticmethod
    def _transform_multiply(value: str, params: Dict) -> float:
        try:
            return float(value) * float(params.get("factor", 1))
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _transform_truncate(value: str, params: Dict) -> str:
        max_len = int(params.get("max_length", 255))
        return str(value)[:max_len]

    @staticmethod
    def _transform_strip(value: str, params: Dict) -> str:
        return str(value).strip()

    @staticmethod
    def _transform_uppercase(value: str, params: Dict) -> str:
        return str(value).upper()

    @staticmethod
    def _transform_pad(value: str, params: Dict) -> str:
        width = int(params.get("width", 6))
        char = params.get("char", "0")
        return str(value).rjust(width, char)

    @staticmethod
    def _transform_default_if_empty(value: str, params: Dict) -> str:
        if not value or str(value).strip() == "":
            return params.get("default", "")
        return value

    @staticmethod
    def _coerce_type(value: Any, field_type: str) -> Any:
        """Coerce a value to the expected type."""
        if value is None or value == "":
            return None
        try:
            if field_type == "decimal":
                # Strip currency symbols and commas
                cleaned = re.sub(r'[£$€,\s]', '', str(value))
                return float(cleaned)
            elif field_type == "integer":
                return int(float(str(value)))
            else:
                return str(value).strip()
        except (ValueError, TypeError):
            return value
```

- [ ] **Step 4: Run tests — expect pass**

Run: `pytest tests/test_import_engine.py -v`

- [ ] **Step 5: Commit**

```bash
git add sql_rag/import_engine.py tests/test_import_engine.py
git commit -m "feat: add import engine core — parse, map, transform, validate pipeline"
```

---

## Task 4: Opera SE Adapter

Wraps existing `OperaSQLImport` methods. Handles Opera-specific validation (account exists, period open, dormancy, duplicates) and posting.

**Files:**
- Create: `sql_rag/import_adapter_opera_se.py`
- Test: `tests/test_import_adapter.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_import_adapter.py
import pytest
from unittest.mock import MagicMock, patch
from sql_rag.import_adapter_opera_se import OperaSEAdapter
from sql_rag.import_engine import ParsedRow
from sql_rag.import_registry import RegistryEntry, ImportField

@pytest.fixture
def sales_receipt_entry():
    return RegistryEntry(
        type_id="sales_receipt",
        label="Sales Receipt",
        category="cashbook",
        import_method="import_sales_receipt",
        user_fields=[
            ImportField(name="customer_account", label="Customer Code", required=True, field_type="string", lookup_table="sname"),
            ImportField(name="amount_pounds", label="Amount", required=True, field_type="decimal"),
            ImportField(name="post_date", label="Date", required=True, field_type="date"),
            ImportField(name="reference", label="Reference", required=False, field_type="string"),
        ],
        default_fields=[
            ImportField(name="bank_account", label="Bank", required=True, field_type="string", source="template"),
        ],
    )

def test_adapter_validate_row_account_not_found(sales_receipt_entry):
    mock_sql = MagicMock()
    mock_sql.execute_query.return_value = MagicMock(empty=True)  # Account not found

    adapter = OperaSEAdapter(sql_connector=mock_sql)
    row = ParsedRow(row_number=1, data={
        "customer_account": "BADCODE",
        "amount_pounds": 100.0,
        "post_date": "2026-04-15",
        "bank_account": "CURR",
    })
    issues = adapter.validate_row(sales_receipt_entry, row)
    assert any("not found" in i["message"].lower() or "does not exist" in i["message"].lower() for i in issues)

def test_adapter_builds_method_params(sales_receipt_entry):
    adapter = OperaSEAdapter(sql_connector=None)
    row = ParsedRow(row_number=1, data={
        "customer_account": "SMIT001",
        "amount_pounds": 100.50,
        "post_date": "2026-04-15",
        "reference": "INV001",
        "bank_account": "CURR",
    })
    params = adapter.build_method_params(sales_receipt_entry, row)
    assert params["customer_account"] == "SMIT001"
    assert params["amount_pounds"] == 100.50
    assert params["bank_account"] == "CURR"
```

- [ ] **Step 2: Run test — expect fail**

Run: `pytest tests/test_import_adapter.py -v`

- [ ] **Step 3: Implement the adapter**

```python
# sql_rag/import_adapter_opera_se.py
"""
Opera SE Adapter for the Generic Import Engine.

Wraps existing OperaSQLImport methods. Handles Opera-specific validation
(account existence, dormancy, period, duplicates) and posting.
Does NOT contain posting logic — calls existing methods.
"""
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from sql_rag.import_registry import RegistryEntry
from sql_rag.import_engine import ParsedRow
import logging

logger = logging.getLogger(__name__)


class OperaSEAdapter:
    """
    Adapter that validates and posts rows to Opera SQL SE.
    """

    def __init__(self, sql_connector=None):
        self.sql_connector = sql_connector

    def validate_row(self, entry: RegistryEntry, row: ParsedRow) -> List[Dict]:
        """
        Validate a row against Opera. Returns list of issues.
        Each issue: {severity, message, suggestion, field}
        """
        issues = []
        if not self.sql_connector:
            return issues

        # Check account fields against Opera tables
        for f in entry.user_fields + entry.default_fields:
            val = row.data.get(f.name)
            if not val or not f.lookup_table:
                continue

            if f.lookup_table == "sname":
                issues.extend(self._check_customer(val, row.row_number))
            elif f.lookup_table == "pname":
                issues.extend(self._check_supplier(val, row.row_number))
            elif f.lookup_table == "nacnt":
                issues.extend(self._check_nominal(val, row.row_number))
            elif f.lookup_table == "nbank":
                issues.extend(self._check_bank(val, row.row_number))

        # Check posting period
        post_date = row.data.get("post_date")
        if post_date:
            issues.extend(self._check_period(post_date, row.row_number))

        # Check for zero amounts
        amount = row.data.get("amount_pounds")
        if amount is not None and float(amount) == 0:
            issues.append({
                "severity": "error",
                "field": "amount_pounds",
                "message": f"Row {row.row_number}: Amount is £0.00",
                "suggestion": "Check source data — zero-value transactions cannot be posted",
            })

        return issues

    def build_method_params(self, entry: RegistryEntry, row: ParsedRow) -> Dict[str, Any]:
        """Build the parameter dict to call the OperaSQLImport method."""
        params = {}
        for f in entry.user_fields + entry.default_fields:
            val = row.data.get(f.name)
            if val is not None:
                # Convert date strings to date objects
                if f.field_type == "date" and isinstance(val, str):
                    try:
                        params[f.name] = datetime.strptime(val, "%Y-%m-%d").date()
                    except ValueError:
                        params[f.name] = val
                else:
                    params[f.name] = val
        return params

    def post_row(self, entry: RegistryEntry, row: ParsedRow, validate_only: bool = False):
        """Post a single row to Opera using the appropriate import method."""
        from sql_rag.opera_sql_import import OperaSQLImport

        importer = OperaSQLImport(self.sql_connector)
        method = getattr(importer, entry.import_method, None)
        if not method:
            return {"success": False, "error": f"Import method '{entry.import_method}' not found"}

        params = self.build_method_params(entry, row)
        params["validate_only"] = validate_only

        try:
            result = method(**params)
            return {
                "success": result.success if hasattr(result, "success") else True,
                "errors": result.errors if hasattr(result, "errors") else [],
                "warnings": result.warnings if hasattr(result, "warnings") else [],
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def post_batch(self, entry: RegistryEntry, rows: List[ParsedRow],
                   error_handling: str = "stop_on_error") -> Dict:
        """
        Post multiple rows with proper locking and error handling.

        Locking strategy (matches existing import patterns):
        - Acquire bank-level import lock before posting (prevents concurrent imports to same bank)
        - Each row posted individually within its own DB transaction (deadlock retry built into import methods)
        - All SELECT queries use WITH (NOLOCK) — no read locks
        - Sequence allocation uses WITH (UPDLOCK, ROWLOCK) — handled inside import methods
        - Lock released in finally block regardless of outcome

        Error handling:
        - stop_on_error: stop at first failure, report what succeeded
        - skip_failed: continue past failures, report all
        - pause_and_ask: not handled here (frontend polls status)
        """
        from sql_rag.import_lock import acquire_import_lock, release_import_lock

        # Determine bank account for lock (from first row's defaults)
        bank_account = None
        for row in rows:
            bank_account = row.data.get("bank_account")
            if bank_account:
                break

        lock_key = f"import_{bank_account}" if bank_account else "import_generic"
        if not acquire_import_lock(lock_key, locked_by="generic_import", endpoint="generic-import"):
            return {
                "success": False,
                "error": f"Bank account {bank_account} is currently being imported by another user. Please wait.",
                "rows_posted": 0, "rows_failed": 0, "rows_skipped": 0,
            }

        results = {"rows_posted": 0, "rows_failed": 0, "rows_skipped": 0, "details": []}

        try:
            for row in rows:
                row_result = self.post_row(entry, row)
                if row_result["success"]:
                    results["rows_posted"] += 1
                    results["details"].append({"row": row.row_number, "status": "posted"})
                else:
                    results["rows_failed"] += 1
                    error_msg = row_result.get("error") or "; ".join(row_result.get("errors", []))
                    results["details"].append({"row": row.row_number, "status": "failed", "error": error_msg})

                    if error_handling == "stop_on_error":
                        # Remaining rows are skipped
                        remaining = len(rows) - results["rows_posted"] - results["rows_failed"]
                        results["rows_skipped"] = remaining
                        break

            results["success"] = results["rows_failed"] == 0
        finally:
            release_import_lock(lock_key)

        return results

    def get_suggestions(self, field_name: str, value: str, lookup_table: str) -> List[Dict]:
        """Fuzzy match suggestions for a bad value."""
        if not self.sql_connector:
            return []

        suggestions = []
        try:
            if lookup_table == "sname":
                df = self.sql_connector.execute_query(f"""
                    SELECT TOP 5 RTRIM(sn_account) as code, RTRIM(sn_name) as name
                    FROM sname WITH (NOLOCK)
                    WHERE sn_dormant = 0
                    AND (RTRIM(sn_name) LIKE '%{value}%' OR RTRIM(sn_account) LIKE '%{value}%')
                """)
            elif lookup_table == "pname":
                df = self.sql_connector.execute_query(f"""
                    SELECT TOP 5 RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK)
                    WHERE pn_dormant = 0
                    AND (RTRIM(pn_name) LIKE '%{value}%' OR RTRIM(pn_account) LIKE '%{value}%')
                """)
            elif lookup_table == "nacnt":
                df = self.sql_connector.execute_query(f"""
                    SELECT TOP 5 RTRIM(na_acnt) as code, RTRIM(na_name) as name
                    FROM nacnt WITH (NOLOCK)
                    WHERE RTRIM(na_acnt) LIKE '%{value}%' OR RTRIM(na_name) LIKE '%{value}%'
                """)
            else:
                return []

            if df is not None and not df.empty:
                for _, r in df.iterrows():
                    suggestions.append({"code": r["code"], "name": r["name"]})
        except Exception:
            pass

        return suggestions

    # --- Private validation helpers ---

    def _check_customer(self, account: str, row_num: int) -> List[Dict]:
        issues = []
        df = self.sql_connector.execute_query(f"""
            SELECT sn_dormant FROM sname WITH (NOLOCK)
            WHERE RTRIM(sn_account) = '{account.strip()}'
        """)
        if df is None or df.empty:
            suggestions = self.get_suggestions("customer_account", account, "sname")
            suggestion = f"Did you mean '{suggestions[0]['code']}' ({suggestions[0]['name']})?" if suggestions else "Check the account code in Opera"
            issues.append({
                "severity": "error", "field": "customer_account",
                "message": f"Row {row_num}: Customer '{account}' does not exist in Opera",
                "suggestion": suggestion,
            })
        elif df.iloc[0].get("sn_dormant", 0) != 0:
            issues.append({
                "severity": "error", "field": "customer_account",
                "message": f"Row {row_num}: Customer '{account}' is dormant",
                "suggestion": "Reactivate in Opera or remove this row",
            })
        return issues

    def _check_supplier(self, account: str, row_num: int) -> List[Dict]:
        issues = []
        df = self.sql_connector.execute_query(f"""
            SELECT pn_dormant FROM pname WITH (NOLOCK)
            WHERE RTRIM(pn_account) = '{account.strip()}'
        """)
        if df is None or df.empty:
            suggestions = self.get_suggestions("supplier_account", account, "pname")
            suggestion = f"Did you mean '{suggestions[0]['code']}' ({suggestions[0]['name']})?" if suggestions else "Check the account code in Opera"
            issues.append({
                "severity": "error", "field": "supplier_account",
                "message": f"Row {row_num}: Supplier '{account}' does not exist in Opera",
                "suggestion": suggestion,
            })
        elif df.iloc[0].get("pn_dormant", 0) != 0:
            issues.append({
                "severity": "error", "field": "supplier_account",
                "message": f"Row {row_num}: Supplier '{account}' is dormant",
                "suggestion": "Reactivate in Opera or remove this row",
            })
        return issues

    def _check_nominal(self, account: str, row_num: int) -> List[Dict]:
        issues = []
        df = self.sql_connector.execute_query(f"""
            SELECT na_acnt FROM nacnt WITH (NOLOCK)
            WHERE RTRIM(na_acnt) = '{account.strip()}'
        """)
        if df is None or df.empty:
            issues.append({
                "severity": "error", "field": "nominal_account",
                "message": f"Row {row_num}: Nominal account '{account}' does not exist",
                "suggestion": "Check the account code in Opera's chart of accounts",
            })
        return issues

    def _check_bank(self, account: str, row_num: int) -> List[Dict]:
        issues = []
        df = self.sql_connector.execute_query(f"""
            SELECT nk_acnt FROM nbank WITH (NOLOCK)
            WHERE RTRIM(nk_acnt) = '{account.strip()}'
        """)
        if df is None or df.empty:
            issues.append({
                "severity": "error", "field": "bank_account",
                "message": f"Row {row_num}: Bank account '{account}' does not exist",
                "suggestion": "Check the bank code in Opera's bank accounts",
            })
        return issues

    def _check_period(self, post_date: str, row_num: int) -> List[Dict]:
        issues = []
        try:
            from sql_rag.opera_config import validate_posting_period
            if isinstance(post_date, str):
                post_date_obj = datetime.strptime(post_date, "%Y-%m-%d").date()
            else:
                post_date_obj = post_date
            result = validate_posting_period(self.sql_connector, post_date_obj)
            if not result.is_valid:
                issues.append({
                    "severity": "error", "field": "post_date",
                    "message": f"Row {row_num}: {result.error_message}",
                    "suggestion": "Change posting date to current period, or ask administrator to reopen the period",
                })
        except Exception:
            pass
        return issues
```

- [ ] **Step 4: Run tests — expect pass**

Run: `pytest tests/test_import_adapter.py -v`

- [ ] **Step 5: Commit**

```bash
git add sql_rag/import_adapter_opera_se.py tests/test_import_adapter.py
git commit -m "feat: add Opera SE adapter for generic import engine"
```

---

## Task 5: API Endpoints

FastAPI routes for templates CRUD, file upload, validation, and import.

**Files:**
- Create: `apps/generic_import/api/__init__.py`
- Create: `apps/generic_import/api/routes.py`
- Modify: `api/main.py` (add router)

- [ ] **Step 1: Create module structure**

```bash
mkdir -p apps/generic_import/api
touch apps/generic_import/__init__.py
touch apps/generic_import/api/__init__.py
```

- [ ] **Step 2: Implement API routes**

Create `apps/generic_import/api/routes.py` — endpoints:

- `GET /api/imports/registry` — list available transaction types
- `GET /api/imports/registry/{type_id}` — get type details with field definitions
- `GET /api/imports/templates` — list saved templates
- `POST /api/imports/templates` — create template
- `GET /api/imports/templates/{id}` — get template
- `PUT /api/imports/templates/{id}` — update template
- `DELETE /api/imports/templates/{id}` — delete template
- `POST /api/imports/upload` — upload CSV/Excel, apply template, return parsed preview
- `POST /api/imports/validate` — dry-run validate parsed rows against Opera
- `POST /api/imports/execute` — post validated rows to Opera
- `GET /api/imports/history` — import history with audit trail

Each endpoint follows existing patterns: per-request globals from `api/main.py`, company-scoped SQLite, NOLOCK on reads.

- [ ] **Step 3: Register router in api/main.py**

Add import and include:
```python
from apps.generic_import.api.routes import router as generic_import_router
# ...
app.include_router(generic_import_router)
```

- [ ] **Step 4: Verify API starts**

Run: `cd /Users/maccb/llmragsql && python -c "from apps.generic_import.api.routes import router; print('OK')"`

- [ ] **Step 5: Commit**

```bash
git add apps/generic_import/ api/main.py
git commit -m "feat: add API endpoints for generic import engine"
```

---

## Task 6: Frontend — Import Page

Upload file, pick template, preview rows, validate, approve, import.

**Files:**
- Create: `frontend/src/pages/GenericImport.tsx`
- Create: `frontend/src/pages/ImportTemplates.tsx`
- Modify: `frontend/src/App.tsx` (add routes)
- Modify: `frontend/src/components/Layout.tsx` (add menu items)

- [ ] **Step 1: Create GenericImport.tsx**

Main page with workflow steps:
1. **Select template** or create new mapping
2. **Upload file** (CSV/Excel drag-and-drop)
3. **Preview** — table showing mapped rows in plain English (account codes resolved to names)
4. **Validate** — dry run button, shows pass/fail per row with plain-English messages and fix suggestions
5. **Import** — approve button, progress bar, post-import report

- [ ] **Step 2: Create ImportTemplates.tsx**

Template builder:
1. Pick transaction type from registry (dropdown grouped by category)
2. System shows required/optional fields with descriptions
3. Upload sample file → AI suggests column mappings (optional)
4. Map columns via dropdowns
5. Set defaults for template-level fields
6. Add transforms (date format, etc.)
7. Set error handling preference
8. Save with name

- [ ] **Step 3: Add routes to App.tsx**

```typescript
import { GenericImport } from './pages/GenericImport';
import { ImportTemplates } from './pages/ImportTemplates';
// In routes:
<Route path="/imports/generic" element={<ProtectedRoute><GenericImport /></ProtectedRoute>} />
<Route path="/imports/templates" element={<ProtectedRoute><ImportTemplates /></ProtectedRoute>} />
```

- [ ] **Step 4: Add menu items to Layout.tsx**

Under a new "Integration" section or within existing "Utilities":
- Import Data
- Import Templates

- [ ] **Step 5: Verify frontend compiles**

Run: `cd frontend && npx tsc --noEmit`

- [ ] **Step 6: Commit**

```bash
git add frontend/src/pages/GenericImport.tsx frontend/src/pages/ImportTemplates.tsx frontend/src/App.tsx frontend/src/components/Layout.tsx
git commit -m "feat: add frontend for generic import engine"
```

---

## Task 7: Seed Registry with Snapshotted Types

Create initial registry entries for transaction types already snapshotted and supported by existing import methods.

**Files:**
- Create: `data/import_registry/sales_receipt.json`
- Create: `data/import_registry/purchase_payment.json`
- Create: `data/import_registry/nominal_entry.json`
- Create: `data/import_registry/sales_refund.json`
- Create: `data/import_registry/purchase_refund.json`
- Create: `data/import_registry/bank_transfer.json`
- Create: `data/import_registry/nominal_journal.json`

Each entry maps the import method signature to user/default/auto fields. These are hand-authored based on snapshot data and import method parameters.

- [ ] **Step 1: Create registry entries for each type**

One JSON file per type, following the schema from Task 1. Each entry specifies:
- `user_fields` — what the user must provide in their CSV/data
- `default_fields` — what can be set once in the template
- `auto_fields` — what Opera generates (entry numbers, journals, IDs)
- `import_method` — exact method name on `OperaSQLImport`
- `duplicate_detection` — which fields to match on

- [ ] **Step 2: Verify registry loads**

```python
from sql_rag.import_registry import ImportRegistry
registry = ImportRegistry("data/import_registry")
entries = registry.list_all()
assert len(entries) >= 7
```

- [ ] **Step 3: Commit**

```bash
git add data/import_registry/
git commit -m "feat: seed import registry with initial transaction types"
```

---

## Task 8: End-to-End Integration Test

Test the full pipeline: upload CSV → parse → validate → dry run against Opera → post.

**Files:**
- Test: `tests/test_import_e2e.py`

- [ ] **Step 1: Write integration test**

```python
def test_full_pipeline_sales_receipts(tmp_path):
    """End-to-end: CSV → parse → validate → preview."""
    from sql_rag.import_registry import ImportRegistry
    from sql_rag.import_engine import ImportEngine

    registry = ImportRegistry("data/import_registry")
    entry = registry.get("sales_receipt")
    assert entry is not None

    csv_data = "Customer,Amount,Date,Reference\nSMIT001,500.00,15/04/2026,INV001\n"
    mappings = [
        {"source": "Customer", "target": "customer_account"},
        {"source": "Amount", "target": "amount_pounds"},
        {"source": "Date", "target": "post_date"},
        {"source": "Reference", "target": "reference"},
    ]
    defaults = {"bank_account": "CURR"}
    transforms = [{"field": "post_date", "transform": "date_format", "params": {"from": "DD/MM/YYYY"}}]

    engine = ImportEngine(registry_entry=entry)
    rows = engine.parse_csv(csv_data, mappings, defaults, transforms)
    result = engine.validate_all(rows)

    assert result.total_rows == 1
    assert result.errors == 0
    assert result.ready == 1
    assert rows[0].data["amount_pounds"] == 500.0
    assert rows[0].data["post_date"] == "2026-04-15"
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_import_e2e.py -v`

- [ ] **Step 3: Commit**

```bash
git add tests/test_import_e2e.py
git commit -m "test: add end-to-end integration test for import engine"
```
