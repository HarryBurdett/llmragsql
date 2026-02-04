"""
Opera 3 FoxPro Data Reader

Reads Opera 3 data from Visual FoxPro DBF files.
Opera 3 stores data in .dbf files with similar table structures to Opera SQL SE.

REQUIREMENTS:
- dbfread package (pip install dbfread)

USAGE:
    from sql_rag.opera3_foxpro import Opera3Reader

    reader = Opera3Reader(r"C:\Apps\O3 Server VFP")

    # List available tables
    tables = reader.list_tables()

    # Read a table
    suppliers = reader.read_table("pname")

    # Query with filtering
    invoices = reader.query("ptran", filters={"pt_account": "SUP001"})
"""

import os
import logging
from typing import Optional, Dict, Any, List, Generator
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, date

logger = logging.getLogger(__name__)

try:
    from dbfread import DBF, FieldParser
    DBF_AVAILABLE = True
except ImportError:
    DBF_AVAILABLE = False
    logger.warning("dbfread not installed. Install with: pip install dbfread")


@dataclass
class TableInfo:
    """Information about a DBF table"""
    name: str
    path: str
    record_count: int
    fields: List[Dict[str, Any]]
    last_modified: datetime


class Opera3FieldParser(FieldParser):
    """Custom field parser for Opera 3 DBF files"""

    def parse(self, field, data):
        """Parse field with Opera 3 specific handling"""
        try:
            return super().parse(field, data)
        except Exception as e:
            # Return None for unparseable data
            logger.debug(f"Could not parse field {field.name}: {e}")
            return None


class Opera3Reader:
    """
    Reader for Opera 3 FoxPro DBF files

    Provides access to Opera 3 data stored in Visual FoxPro format.
    Table structures are similar to Opera SQL SE.
    """

    # Known Opera 3 table mappings (DBF filename -> description)
    KNOWN_TABLES = {
        # Master files
        "pname": "Supplier Master",
        "sname": "Customer Master",
        "nname": "Nominal Account Master",
        "stock": "Stock/Product Master",

        # Transaction files
        "ptran": "Purchase Ledger Transactions",
        "stran": "Sales Ledger Transactions",
        "ntran": "Nominal Ledger Transactions",
        "atran": "Bank Account Transactions",

        # Entry files
        "aentry": "Bank Account Entries",
        "pentry": "Purchase Ledger Entries",
        "sentry": "Sales Ledger Entries",

        # Allocation files
        "palloc": "Purchase Ledger Allocations",
        "salloc": "Sales Ledger Allocations",

        # Other common files
        "sysparm": "System Parameters",
        "company": "Company Information",
        "vatcode": "VAT Codes",
        "currency": "Currency Definitions",
    }

    def __init__(self, data_path: str, encoding: str = 'cp1252'):
        """
        Initialize the Opera 3 reader.

        Args:
            data_path: Path to the Opera 3 data folder (e.g., C:\\Apps\\O3 Server VFP)
            encoding: Character encoding for DBF files (default: cp1252 for Windows)
        """
        self.data_path = Path(data_path)
        self.encoding = encoding
        self._table_cache: Dict[str, TableInfo] = {}

        if not DBF_AVAILABLE:
            raise ImportError(
                "dbfread package required. Install with: pip install dbfread"
            )

        if not self.data_path.exists():
            logger.warning(f"Opera 3 data path does not exist: {data_path}")

    def list_tables(self, include_unknown: bool = True) -> List[Dict[str, Any]]:
        """
        List all available DBF tables in the Opera 3 data folder.

        Args:
            include_unknown: Include tables not in the known tables list

        Returns:
            List of table information dictionaries
        """
        tables = []

        if not self.data_path.exists():
            logger.error(f"Data path does not exist: {self.data_path}")
            return tables

        # Find all .dbf files
        for dbf_file in self.data_path.glob("*.dbf"):
            table_name = dbf_file.stem.lower()

            # Skip if not in known tables and include_unknown is False
            if not include_unknown and table_name not in self.KNOWN_TABLES:
                continue

            try:
                info = self._get_table_info(table_name)
                tables.append({
                    "name": table_name,
                    "description": self.KNOWN_TABLES.get(table_name, "Unknown"),
                    "record_count": info.record_count,
                    "field_count": len(info.fields),
                    "path": str(info.path),
                    "last_modified": info.last_modified.isoformat() if info.last_modified else None
                })
            except Exception as e:
                logger.warning(f"Could not read table {table_name}: {e}")
                tables.append({
                    "name": table_name,
                    "description": self.KNOWN_TABLES.get(table_name, "Unknown"),
                    "error": str(e)
                })

        return sorted(tables, key=lambda x: x["name"])

    def _get_table_info(self, table_name: str) -> TableInfo:
        """Get information about a table"""
        if table_name in self._table_cache:
            return self._table_cache[table_name]

        dbf_path = self.data_path / f"{table_name}.dbf"
        if not dbf_path.exists():
            # Try uppercase
            dbf_path = self.data_path / f"{table_name.upper()}.DBF"

        if not dbf_path.exists():
            raise FileNotFoundError(f"Table not found: {table_name}")

        # Get file modification time
        mtime = datetime.fromtimestamp(dbf_path.stat().st_mtime)

        # Open DBF to get structure
        dbf = DBF(
            str(dbf_path),
            encoding=self.encoding,
            parserclass=Opera3FieldParser,
            load=False  # Don't load all records
        )

        fields = [
            {
                "name": f.name,
                "type": f.type,
                "length": f.length,
                "decimal_count": getattr(f, 'decimal_count', 0)
            }
            for f in dbf.fields
        ]

        info = TableInfo(
            name=table_name,
            path=str(dbf_path),
            record_count=len(dbf),
            fields=fields,
            last_modified=mtime
        )

        self._table_cache[table_name] = info
        return info

    def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """
        Get the structure of a table.

        Args:
            table_name: Name of the table (e.g., "pname", "ptran")

        Returns:
            Dictionary with table structure information
        """
        info = self._get_table_info(table_name)
        return {
            "name": info.name,
            "description": self.KNOWN_TABLES.get(table_name, "Unknown"),
            "path": info.path,
            "record_count": info.record_count,
            "last_modified": info.last_modified.isoformat() if info.last_modified else None,
            "fields": info.fields
        }

    def read_table(
        self,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Read all records from a table.

        Args:
            table_name: Name of the table
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List of record dictionaries
        """
        dbf_path = self._get_dbf_path(table_name)

        dbf = DBF(
            str(dbf_path),
            encoding=self.encoding,
            parserclass=Opera3FieldParser
        )

        records = []
        for i, record in enumerate(dbf):
            if i < offset:
                continue
            if limit and len(records) >= limit:
                break

            # Convert record to dict and clean up values
            record_dict = self._clean_record(dict(record))
            records.append(record_dict)

        return records

    def iter_table(self, table_name: str) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate over records in a table (memory efficient for large tables).

        Args:
            table_name: Name of the table

        Yields:
            Record dictionaries
        """
        dbf_path = self._get_dbf_path(table_name)

        dbf = DBF(
            str(dbf_path),
            encoding=self.encoding,
            parserclass=Opera3FieldParser
        )

        for record in dbf:
            yield self._clean_record(dict(record))

    def query(
        self,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query a table with optional filtering.

        Args:
            table_name: Name of the table
            filters: Dictionary of field_name: value to filter by
            fields: List of field names to include (None = all fields)
            limit: Maximum number of records to return

        Returns:
            List of matching record dictionaries
        """
        filters = filters or {}
        results = []

        for record in self.iter_table(table_name):
            # Apply filters
            match = True
            for field, value in filters.items():
                record_value = record.get(field)

                # Handle string comparison (case-insensitive, trimmed)
                if isinstance(value, str) and isinstance(record_value, str):
                    if record_value.strip().upper() != value.strip().upper():
                        match = False
                        break
                elif record_value != value:
                    match = False
                    break

            if match:
                # Filter fields if specified
                if fields:
                    record = {k: v for k, v in record.items() if k in fields}
                results.append(record)

                if limit and len(results) >= limit:
                    break

        return results

    def _get_dbf_path(self, table_name: str) -> Path:
        """Get the path to a DBF file"""
        # Try lowercase first
        dbf_path = self.data_path / f"{table_name.lower()}.dbf"
        if dbf_path.exists():
            return dbf_path

        # Try uppercase
        dbf_path = self.data_path / f"{table_name.upper()}.DBF"
        if dbf_path.exists():
            return dbf_path

        # Try mixed case
        for f in self.data_path.glob("*.dbf"):
            if f.stem.lower() == table_name.lower():
                return f
        for f in self.data_path.glob("*.DBF"):
            if f.stem.lower() == table_name.lower():
                return f

        raise FileNotFoundError(f"Table not found: {table_name}")

    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean up a record's values"""
        cleaned = {}
        for key, value in record.items():
            # Strip strings
            if isinstance(value, str):
                value = value.strip()
            # Convert dates
            elif isinstance(value, date) and not isinstance(value, datetime):
                value = value.isoformat()
            cleaned[key] = value
        return cleaned

    # =========================================================================
    # Convenience methods for common Opera 3 tables
    # =========================================================================

    def get_suppliers(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get supplier master records"""
        suppliers = self.read_table("pname")
        # Note: filtering for active depends on Opera 3's deletion flag
        return suppliers

    def get_customers(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get customer master records"""
        customers = self.read_table("sname")
        return customers

    def get_nominal_accounts(self) -> List[Dict[str, Any]]:
        """Get nominal account master records"""
        return self.read_table("nname")

    def get_supplier_transactions(
        self,
        supplier_code: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """
        Get purchase ledger transactions.

        Args:
            supplier_code: Filter by supplier account code
            from_date: Filter transactions from this date
            to_date: Filter transactions up to this date
        """
        filters = {}
        if supplier_code:
            filters["pt_account"] = supplier_code

        transactions = self.query("ptran", filters=filters)

        # Apply date filters
        if from_date or to_date:
            filtered = []
            for txn in transactions:
                txn_date = txn.get("pt_trdate")
                if isinstance(txn_date, str):
                    txn_date = date.fromisoformat(txn_date)
                if from_date and txn_date < from_date:
                    continue
                if to_date and txn_date > to_date:
                    continue
                filtered.append(txn)
            transactions = filtered

        return transactions

    def get_customer_transactions(
        self,
        customer_code: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """
        Get sales ledger transactions.

        Args:
            customer_code: Filter by customer account code
            from_date: Filter transactions from this date
            to_date: Filter transactions up to this date
        """
        filters = {}
        if customer_code:
            filters["st_account"] = customer_code

        transactions = self.query("stran", filters=filters)

        # Apply date filters
        if from_date or to_date:
            filtered = []
            for txn in transactions:
                txn_date = txn.get("st_trdate")
                if isinstance(txn_date, str):
                    txn_date = date.fromisoformat(txn_date)
                if from_date and txn_date < from_date:
                    continue
                if to_date and txn_date > to_date:
                    continue
                filtered.append(txn)
            transactions = filtered

        return transactions


# Singleton instance
_opera3_reader: Optional[Opera3Reader] = None


def get_opera3_reader(data_path: str = r"C:\Apps\O3 Server VFP") -> Opera3Reader:
    """Get or create the Opera 3 reader singleton"""
    global _opera3_reader
    if _opera3_reader is None or str(_opera3_reader.data_path) != data_path:
        _opera3_reader = Opera3Reader(data_path)
    return _opera3_reader
