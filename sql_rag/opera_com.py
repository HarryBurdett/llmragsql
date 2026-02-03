"""
Opera 3 COM Automation Module

This module provides Python bindings to Opera 3's COM/ActiveX automation interface.
Allows programmatic access to Opera's import/export functions, posting routines,
and other automation capabilities.

REQUIREMENTS:
- Windows OS with Opera 3 installed
- pywin32 package (pip install pywin32)
- Opera 3 must be registered as a COM server

USAGE:
    from sql_rag.opera_com import OperaCOM

    opera = OperaCOM()
    if opera.connect("COMPANY01"):
        opera.import_sales_orders("path/to/orders.csv")
        opera.disconnect()

OPERA 3 COM OBJECTS:
- Pegasus.Opera3.Application - Main application object
- Pegasus.Opera3.SalesLedger - Sales Ledger automation
- Pegasus.Opera3.PurchaseLedger - Purchase Ledger automation
- Pegasus.Opera3.NominalLedger - Nominal Ledger automation
- Pegasus.Opera3.StockControl - Stock/Inventory automation
"""

import logging
import os
import sys
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

# Configure logging
logger = logging.getLogger(__name__)

# Check if running on Windows
IS_WINDOWS = sys.platform == 'win32'

if IS_WINDOWS:
    try:
        import win32com.client
        import pythoncom
        COM_AVAILABLE = True
    except ImportError:
        COM_AVAILABLE = False
        logger.warning("pywin32 not installed. Install with: pip install pywin32")
else:
    COM_AVAILABLE = False
    logger.info("Opera COM automation only available on Windows")


class OperaModule(Enum):
    """Opera 3 modules available for automation"""
    SALES_LEDGER = "SL"
    PURCHASE_LEDGER = "PL"
    NOMINAL_LEDGER = "NL"
    STOCK_CONTROL = "SC"
    SALES_ORDER = "SOP"
    PURCHASE_ORDER = "POP"
    CASHBOOK = "CB"


class ImportType(Enum):
    """Types of imports supported"""
    SALES_INVOICES = "sales_invoices"
    PURCHASE_INVOICES = "purchase_invoices"
    NOMINAL_JOURNALS = "nominal_journals"
    CUSTOMERS = "customers"
    SUPPLIERS = "suppliers"
    PRODUCTS = "products"
    SALES_ORDERS = "sales_orders"
    PURCHASE_ORDERS = "purchase_orders"


@dataclass
class ImportResult:
    """Result of an import operation"""
    success: bool
    records_processed: int
    records_imported: int
    records_failed: int
    errors: List[str]
    warnings: List[str]


class OperaCOMError(Exception):
    """Exception raised for Opera COM automation errors"""
    pass


class OperaCOM:
    """
    Opera 3 COM Automation Interface

    Provides access to Opera 3's automation capabilities including:
    - Import/Export functions
    - Posting routines
    - Report generation
    - Data validation
    """

    def __init__(self):
        """Initialize the Opera COM connector"""
        self.app = None
        self.company = None
        self.connected = False
        self._modules: Dict[str, Any] = {}

        if not IS_WINDOWS:
            logger.warning("Opera COM automation requires Windows OS")
        elif not COM_AVAILABLE:
            logger.warning("pywin32 package required for COM automation")

    @property
    def is_available(self) -> bool:
        """Check if COM automation is available on this system"""
        return IS_WINDOWS and COM_AVAILABLE

    def connect(self, company_code: str, username: str = "", password: str = "") -> bool:
        """
        Connect to Opera 3 and open a company dataset.

        Args:
            company_code: The Opera company code (e.g., "COMPANY01")
            username: Opera username (optional, uses Windows auth if blank)
            password: Opera password (optional)

        Returns:
            True if connection successful, False otherwise

        Raises:
            OperaCOMError: If COM automation is not available
        """
        if not self.is_available:
            raise OperaCOMError(
                "Opera COM automation not available. "
                "Requires Windows OS with pywin32 installed."
            )

        try:
            # Initialize COM for this thread
            pythoncom.CoInitialize()

            # Create Opera application instance
            logger.info("Creating Opera 3 COM application instance...")
            self.app = win32com.client.Dispatch("Pegasus.Opera3.Application")

            # Set credentials if provided
            if username:
                self.app.UserName = username
            if password:
                self.app.Password = password

            # Open the company
            logger.info(f"Opening company: {company_code}")
            result = self.app.OpenCompany(company_code)

            if result:
                self.company = company_code
                self.connected = True
                logger.info(f"Successfully connected to Opera 3 company: {company_code}")
                return True
            else:
                logger.error(f"Failed to open company: {company_code}")
                return False

        except Exception as e:
            logger.error(f"Error connecting to Opera 3: {e}")
            self.disconnect()
            raise OperaCOMError(f"Failed to connect to Opera 3: {e}")

    def disconnect(self):
        """Disconnect from Opera 3 and release COM resources"""
        try:
            if self.app:
                try:
                    self.app.CloseCompany()
                except:
                    pass
                self.app = None

            self._modules.clear()
            self.company = None
            self.connected = False

            # Uninitialize COM
            if IS_WINDOWS and COM_AVAILABLE:
                try:
                    pythoncom.CoUninitialize()
                except:
                    pass

            logger.info("Disconnected from Opera 3")

        except Exception as e:
            logger.error(f"Error disconnecting from Opera 3: {e}")

    def _get_module(self, module: OperaModule) -> Any:
        """Get or create a reference to an Opera module"""
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        if module.value not in self._modules:
            try:
                # Get the module from the application object
                if module == OperaModule.SALES_LEDGER:
                    self._modules[module.value] = self.app.SalesLedger
                elif module == OperaModule.PURCHASE_LEDGER:
                    self._modules[module.value] = self.app.PurchaseLedger
                elif module == OperaModule.NOMINAL_LEDGER:
                    self._modules[module.value] = self.app.NominalLedger
                elif module == OperaModule.STOCK_CONTROL:
                    self._modules[module.value] = self.app.StockControl
                elif module == OperaModule.SALES_ORDER:
                    self._modules[module.value] = self.app.SalesOrderProcessing
                elif module == OperaModule.PURCHASE_ORDER:
                    self._modules[module.value] = self.app.PurchaseOrderProcessing
                elif module == OperaModule.CASHBOOK:
                    self._modules[module.value] = self.app.Cashbook
            except Exception as e:
                raise OperaCOMError(f"Failed to access module {module.value}: {e}")

        return self._modules[module.value]

    # =========================================================================
    # IMPORT FUNCTIONS
    # =========================================================================

    def import_from_file(
        self,
        import_type: ImportType,
        file_path: str,
        options: Optional[Dict[str, Any]] = None
    ) -> ImportResult:
        """
        Import data from a file into Opera 3.

        Args:
            import_type: Type of data to import
            file_path: Path to the import file (CSV, XML, or Opera format)
            options: Additional import options

        Returns:
            ImportResult with details of the import operation
        """
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        if not os.path.exists(file_path):
            raise OperaCOMError(f"Import file not found: {file_path}")

        options = options or {}
        errors = []
        warnings = []

        try:
            logger.info(f"Starting import: {import_type.value} from {file_path}")

            if import_type == ImportType.SALES_INVOICES:
                return self._import_sales_invoices(file_path, options)
            elif import_type == ImportType.PURCHASE_INVOICES:
                return self._import_purchase_invoices(file_path, options)
            elif import_type == ImportType.NOMINAL_JOURNALS:
                return self._import_nominal_journals(file_path, options)
            elif import_type == ImportType.CUSTOMERS:
                return self._import_customers(file_path, options)
            elif import_type == ImportType.SUPPLIERS:
                return self._import_suppliers(file_path, options)
            elif import_type == ImportType.PRODUCTS:
                return self._import_products(file_path, options)
            elif import_type == ImportType.SALES_ORDERS:
                return self._import_sales_orders(file_path, options)
            elif import_type == ImportType.PURCHASE_ORDERS:
                return self._import_purchase_orders(file_path, options)
            else:
                raise OperaCOMError(f"Unsupported import type: {import_type}")

        except Exception as e:
            logger.error(f"Import failed: {e}")
            return ImportResult(
                success=False,
                records_processed=0,
                records_imported=0,
                records_failed=0,
                errors=[str(e)],
                warnings=[]
            )

    def _import_sales_invoices(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import sales invoices via Opera COM"""
        sl = self._get_module(OperaModule.SALES_LEDGER)

        # Use Opera's built-in import
        importer = sl.CreateImport("SalesInvoices")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)

        # Run the import
        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_purchase_invoices(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import purchase invoices via Opera COM"""
        pl = self._get_module(OperaModule.PURCHASE_LEDGER)

        importer = pl.CreateImport("PurchaseInvoices")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_nominal_journals(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import nominal journal entries via Opera COM"""
        nl = self._get_module(OperaModule.NOMINAL_LEDGER)

        importer = nl.CreateImport("Journals")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)
        importer.PostImmediately = options.get("post_immediately", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_customers(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import customer records via Opera COM"""
        sl = self._get_module(OperaModule.SALES_LEDGER)

        importer = sl.CreateImport("Customers")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)
        importer.UpdateExisting = options.get("update_existing", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_suppliers(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import supplier records via Opera COM"""
        pl = self._get_module(OperaModule.PURCHASE_LEDGER)

        importer = pl.CreateImport("Suppliers")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)
        importer.UpdateExisting = options.get("update_existing", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_products(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import product/stock records via Opera COM"""
        sc = self._get_module(OperaModule.STOCK_CONTROL)

        importer = sc.CreateImport("Products")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)
        importer.UpdateExisting = options.get("update_existing", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_sales_orders(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import sales orders via Opera COM"""
        sop = self._get_module(OperaModule.SALES_ORDER)

        importer = sop.CreateImport("SalesOrders")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    def _import_purchase_orders(
        self,
        file_path: str,
        options: Dict[str, Any]
    ) -> ImportResult:
        """Import purchase orders via Opera COM"""
        pop = self._get_module(OperaModule.PURCHASE_ORDER)

        importer = pop.CreateImport("PurchaseOrders")
        importer.SourceFile = file_path
        importer.FileFormat = options.get("format", "CSV")
        importer.ValidateOnly = options.get("validate_only", False)

        result = importer.Execute()

        return ImportResult(
            success=result.Success,
            records_processed=result.RecordsProcessed,
            records_imported=result.RecordsImported,
            records_failed=result.RecordsFailed,
            errors=list(result.Errors) if result.Errors else [],
            warnings=list(result.Warnings) if result.Warnings else []
        )

    # =========================================================================
    # POSTING FUNCTIONS
    # =========================================================================

    def post_sales_invoices(self, batch_ref: Optional[str] = None) -> bool:
        """Post sales invoices to the nominal ledger"""
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        sl = self._get_module(OperaModule.SALES_LEDGER)

        try:
            if batch_ref:
                result = sl.PostInvoices(batch_ref)
            else:
                result = sl.PostAllInvoices()

            logger.info(f"Posted sales invoices: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to post sales invoices: {e}")
            raise OperaCOMError(f"Post failed: {e}")

    def post_purchase_invoices(self, batch_ref: Optional[str] = None) -> bool:
        """Post purchase invoices to the nominal ledger"""
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        pl = self._get_module(OperaModule.PURCHASE_LEDGER)

        try:
            if batch_ref:
                result = pl.PostInvoices(batch_ref)
            else:
                result = pl.PostAllInvoices()

            logger.info(f"Posted purchase invoices: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to post purchase invoices: {e}")
            raise OperaCOMError(f"Post failed: {e}")

    def post_nominal_journals(self, batch_ref: Optional[str] = None) -> bool:
        """Post nominal journal entries"""
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        nl = self._get_module(OperaModule.NOMINAL_LEDGER)

        try:
            if batch_ref:
                result = nl.PostJournals(batch_ref)
            else:
                result = nl.PostAllJournals()

            logger.info(f"Posted nominal journals: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to post nominal journals: {e}")
            raise OperaCOMError(f"Post failed: {e}")

    # =========================================================================
    # UTILITY FUNCTIONS
    # =========================================================================

    def get_company_info(self) -> Dict[str, Any]:
        """Get information about the current company"""
        if not self.connected:
            raise OperaCOMError("Not connected to Opera 3")

        try:
            return {
                "code": self.company,
                "name": self.app.CompanyName,
                "financial_year": self.app.FinancialYear,
                "current_period": self.app.CurrentPeriod,
                "vat_registered": self.app.VATRegistered,
                "vat_number": self.app.VATNumber if self.app.VATRegistered else None
            }
        except Exception as e:
            logger.error(f"Failed to get company info: {e}")
            return {"code": self.company, "error": str(e)}

    def validate_import_file(
        self,
        import_type: ImportType,
        file_path: str
    ) -> ImportResult:
        """
        Validate an import file without actually importing.

        This performs all validation checks but doesn't commit any data.
        """
        return self.import_from_file(
            import_type,
            file_path,
            {"validate_only": True}
        )

    def get_available_imports(self) -> List[str]:
        """Get list of available import types"""
        return [it.value for it in ImportType]

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure disconnection"""
        self.disconnect()
        return False


# Singleton instance for reuse
_opera_instance: Optional[OperaCOM] = None


def get_opera_connection() -> OperaCOM:
    """Get or create the Opera COM connection singleton"""
    global _opera_instance
    if _opera_instance is None:
        _opera_instance = OperaCOM()
    return _opera_instance
