"""
Multi-Format Bank Statement Parser System

Supports parsing bank statements in multiple formats:
- CSV (existing format with Date, Amount, Memo, Subcategory fields)
- OFX (Open Financial Exchange - XML-based)
- QIF (Quicken Interchange Format - line-based)
- MT940 (SWIFT format)

All parsers produce a unified ParsedTransaction structure.
"""

import csv
import re
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional, Tuple, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ParsedTransaction:
    """
    Unified transaction structure produced by all parsers.

    This is the common format used by the bank import system regardless
    of the original file format.
    """
    date: date
    amount: float  # Positive = credit/receipt, Negative = debit/payment
    name: str  # Payee/payer name
    reference: str = ""  # Transaction reference
    memo: str = ""  # Additional memo/description
    fit_id: str = ""  # Bank's unique transaction ID (from OFX)
    check_number: str = ""  # Check/cheque number if applicable
    subcategory: str = ""  # Transaction type (e.g., 'Funds Transfer', 'Direct Debit')
    raw_data: Dict[str, Any] = field(default_factory=dict)  # Original data for debugging

    @property
    def is_receipt(self) -> bool:
        """True if this is a credit/receipt (money in)"""
        return self.amount > 0

    @property
    def is_payment(self) -> bool:
        """True if this is a debit/payment (money out)"""
        return self.amount < 0

    @property
    def abs_amount(self) -> float:
        """Absolute value of amount"""
        return abs(self.amount)

    def generate_fingerprint(self) -> str:
        """
        Generate unique fingerprint for duplicate detection.

        Format: BKIMP:{hash8}:{YYYYMMDD}
        - BKIMP: Prefix identifying bank import origin
        - hash8: 8-character MD5 hash of name|amount|date
        - YYYYMMDD: Import date (not transaction date)
        """
        data = f"{self.name}|{self.amount}|{self.date.isoformat()}"
        hash8 = hashlib.md5(data.encode()).hexdigest()[:8].upper()
        import_date = date.today().strftime('%Y%m%d')
        return f"BKIMP:{hash8}:{import_date}"


class BankFileParser(ABC):
    """
    Abstract base class for bank file parsers.

    Implement can_parse() and parse() for each supported format.
    """

    @abstractmethod
    def can_parse(self, content: str, filename: str = "") -> bool:
        """
        Check if this parser can handle the given content.

        Args:
            content: File content as string
            filename: Optional filename for extension-based detection

        Returns:
            True if this parser can handle the content
        """
        pass

    @abstractmethod
    def parse(self, content: str, filename: str = "") -> List[ParsedTransaction]:
        """
        Parse the content and return transactions.

        Args:
            content: File content as string
            filename: Optional filename for context

        Returns:
            List of ParsedTransaction objects
        """
        pass

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Human-readable format name"""
        pass


class CSVParser(BankFileParser):
    """
    Parser for CSV bank statements.

    Expected format (from existing bank_import.py):
    - Date: DD/MM/YYYY
    - Amount: Decimal number (comma as thousands separator allowed)
    - Memo: "NAME\tREFERENCE" format
    - Subcategory: Transaction type
    - Account: Optional "sort-code account-number" for validation
    """

    @property
    def format_name(self) -> str:
        return "CSV"

    def can_parse(self, content: str, filename: str = "") -> bool:
        """CSV is the fallback format - check for header row"""
        if filename.lower().endswith('.csv'):
            return True

        # Check for expected header columns
        first_line = content.split('\n')[0].lower() if content else ""
        return 'date' in first_line and 'amount' in first_line

    def parse(self, content: str, filename: str = "") -> List[ParsedTransaction]:
        """Parse CSV content"""
        transactions = []

        lines = content.strip().split('\n')
        if not lines:
            return transactions

        # Parse as CSV
        reader = csv.DictReader(lines)

        for row in reader:
            try:
                # Parse date (DD/MM/YYYY format)
                date_str = row.get('Date', '').strip()
                if not date_str:
                    continue

                try:
                    txn_date = datetime.strptime(date_str, '%d/%m/%Y').date()
                except ValueError:
                    # Try alternative formats
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']:
                        try:
                            txn_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        logger.warning(f"Could not parse date: {date_str}")
                        continue

                # Parse amount
                amount_str = row.get('Amount', '0').strip()
                if not amount_str:
                    continue

                try:
                    amount = float(amount_str.replace(',', ''))
                except ValueError:
                    logger.warning(f"Could not parse amount: {amount_str}")
                    continue

                # Parse memo (NAME\tREFERENCE format)
                memo = row.get('Memo', '').strip()
                name, reference = self._parse_memo(memo)

                # Get subcategory
                subcategory = row.get('Subcategory', '').strip()

                transactions.append(ParsedTransaction(
                    date=txn_date,
                    amount=amount,
                    name=name,
                    reference=reference,
                    memo=memo,
                    subcategory=subcategory,
                    raw_data=dict(row)
                ))

            except Exception as e:
                logger.warning(f"Error parsing CSV row: {e}")
                continue

        return transactions

    def _parse_memo(self, memo: str) -> Tuple[str, str]:
        """
        Parse memo field to extract name and reference.

        Memo format: "NAME                 \tREFERENCE"
        """
        if not memo:
            return "", ""

        parts = memo.split('\t')
        name = parts[0].strip() if parts else ""
        reference = parts[1].strip() if len(parts) > 1 else ""

        return name, reference


class OFXParser(BankFileParser):
    """
    Parser for OFX (Open Financial Exchange) files.

    OFX is an XML-based format commonly used by banks.
    Uses the ofxparse library if available.
    """

    @property
    def format_name(self) -> str:
        return "OFX"

    def can_parse(self, content: str, filename: str = "") -> bool:
        """Check for OFX header or XML declaration"""
        if filename.lower().endswith(('.ofx', '.qfx')):
            return True

        content_start = content[:500].strip().upper()
        return (
            content_start.startswith('OFXHEADER:') or
            '<?OFX' in content_start or
            '<OFX>' in content_start
        )

    def parse(self, content: str, filename: str = "") -> List[ParsedTransaction]:
        """Parse OFX content using ofxparse library"""
        transactions = []

        try:
            from ofxparse import OfxParser
            import io

            # Parse OFX content
            ofx = OfxParser.parse(io.BytesIO(content.encode('latin-1')))

            # Process each account
            for account in getattr(ofx, 'accounts', [ofx.account] if hasattr(ofx, 'account') else []):
                if not hasattr(account, 'statement') or not account.statement:
                    continue

                for txn in account.statement.transactions:
                    try:
                        # Get transaction details
                        txn_date = txn.date.date() if hasattr(txn.date, 'date') else txn.date
                        amount = float(txn.amount)

                        # Extract name and memo
                        name = getattr(txn, 'payee', '') or getattr(txn, 'name', '') or ''
                        memo = getattr(txn, 'memo', '') or ''

                        # Clean up name
                        if not name and memo:
                            name = memo.split('\n')[0][:50]

                        # Get FIT ID (bank's unique transaction ID)
                        fit_id = getattr(txn, 'id', '') or ''

                        # Get check number
                        check_number = getattr(txn, 'checknum', '') or ''

                        # Get transaction type
                        txn_type = getattr(txn, 'type', '') or ''

                        transactions.append(ParsedTransaction(
                            date=txn_date,
                            amount=amount,
                            name=name.strip(),
                            reference=fit_id,
                            memo=memo.strip(),
                            fit_id=fit_id,
                            check_number=check_number,
                            subcategory=txn_type,
                            raw_data={
                                'payee': getattr(txn, 'payee', ''),
                                'name': getattr(txn, 'name', ''),
                                'memo': memo,
                                'type': txn_type,
                                'id': fit_id
                            }
                        ))

                    except Exception as e:
                        logger.warning(f"Error parsing OFX transaction: {e}")
                        continue

        except ImportError:
            logger.error("ofxparse library not installed. Run: pip install ofxparse")
            # Fall back to manual parsing
            transactions = self._parse_ofx_manual(content)
        except Exception as e:
            logger.error(f"Error parsing OFX file: {e}")
            # Try manual parsing as fallback
            transactions = self._parse_ofx_manual(content)

        return transactions

    def _parse_ofx_manual(self, content: str) -> List[ParsedTransaction]:
        """
        Manual OFX parsing fallback when ofxparse is not available.

        OFX format uses SGML-like tags:
        <STMTTRN>
            <TRNTYPE>DEBIT
            <DTPOSTED>20240115
            <TRNAMT>-100.00
            <FITID>12345
            <NAME>MERCHANT NAME
            <MEMO>Optional memo
        </STMTTRN>
        """
        transactions = []

        # Find all transaction blocks
        pattern = r'<STMTTRN>(.*?)</STMTTRN>'
        matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            try:
                # Extract fields
                def get_field(tag: str) -> str:
                    field_pattern = rf'<{tag}>([^<\n]+)'
                    field_match = re.search(field_pattern, match, re.IGNORECASE)
                    return field_match.group(1).strip() if field_match else ''

                # Parse date (YYYYMMDD or YYYYMMDDHHMMSS format)
                date_str = get_field('DTPOSTED')
                if not date_str:
                    continue

                try:
                    txn_date = datetime.strptime(date_str[:8], '%Y%m%d').date()
                except ValueError:
                    logger.warning(f"Could not parse OFX date: {date_str}")
                    continue

                # Parse amount
                amount_str = get_field('TRNAMT')
                if not amount_str:
                    continue

                try:
                    amount = float(amount_str)
                except ValueError:
                    logger.warning(f"Could not parse OFX amount: {amount_str}")
                    continue

                # Get other fields
                fit_id = get_field('FITID')
                name = get_field('NAME') or get_field('PAYEE')
                memo = get_field('MEMO')
                check_num = get_field('CHECKNUM')
                txn_type = get_field('TRNTYPE')

                transactions.append(ParsedTransaction(
                    date=txn_date,
                    amount=amount,
                    name=name,
                    reference=fit_id,
                    memo=memo,
                    fit_id=fit_id,
                    check_number=check_num,
                    subcategory=txn_type,
                    raw_data={'raw_block': match}
                ))

            except Exception as e:
                logger.warning(f"Error in manual OFX parsing: {e}")
                continue

        return transactions


class QIFParser(BankFileParser):
    """
    Parser for QIF (Quicken Interchange Format) files.

    QIF is a line-based format:
    !Type:Bank
    D1/15/2024
    T-100.00
    PMERCHANT NAME
    MOptional memo
    ^
    """

    @property
    def format_name(self) -> str:
        return "QIF"

    def can_parse(self, content: str, filename: str = "") -> bool:
        """Check for QIF header"""
        if filename.lower().endswith('.qif'):
            return True

        content_start = content[:100].strip().upper()
        return content_start.startswith('!TYPE:')

    def parse(self, content: str, filename: str = "") -> List[ParsedTransaction]:
        """Parse QIF content"""
        transactions = []

        # Split into transaction blocks (separated by ^)
        blocks = content.split('^')

        for block in blocks:
            lines = block.strip().split('\n')
            if not lines:
                continue

            # Skip header lines
            if lines[0].strip().upper().startswith('!TYPE:'):
                lines = lines[1:]

            if not lines:
                continue

            try:
                txn_data: Dict[str, str] = {}

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    code = line[0].upper()
                    value = line[1:].strip()

                    if code == 'D':  # Date
                        txn_data['date'] = value
                    elif code == 'T':  # Amount
                        txn_data['amount'] = value
                    elif code == 'P':  # Payee
                        txn_data['payee'] = value
                    elif code == 'M':  # Memo
                        txn_data['memo'] = value
                    elif code == 'N':  # Check number
                        txn_data['check_number'] = value
                    elif code == 'L':  # Category
                        txn_data['category'] = value

                if 'date' not in txn_data or 'amount' not in txn_data:
                    continue

                # Parse date (various formats: MM/DD/YYYY, M/D/YY, etc.)
                date_str = txn_data['date']
                txn_date = None

                for fmt in ['%m/%d/%Y', '%m/%d/%y', '%d/%m/%Y', '%d/%m/%y',
                           '%m-%d-%Y', '%d-%m-%Y', '%Y-%m-%d']:
                    try:
                        txn_date = datetime.strptime(date_str, fmt).date()
                        break
                    except ValueError:
                        continue

                if not txn_date:
                    logger.warning(f"Could not parse QIF date: {date_str}")
                    continue

                # Parse amount
                amount_str = txn_data['amount'].replace(',', '')
                try:
                    amount = float(amount_str)
                except ValueError:
                    logger.warning(f"Could not parse QIF amount: {amount_str}")
                    continue

                transactions.append(ParsedTransaction(
                    date=txn_date,
                    amount=amount,
                    name=txn_data.get('payee', ''),
                    reference=txn_data.get('check_number', ''),
                    memo=txn_data.get('memo', ''),
                    check_number=txn_data.get('check_number', ''),
                    subcategory=txn_data.get('category', ''),
                    raw_data=txn_data
                ))

            except Exception as e:
                logger.warning(f"Error parsing QIF block: {e}")
                continue

        return transactions


class MT940Parser(BankFileParser):
    """
    Parser for MT940 (SWIFT) bank statement format.

    MT940 is used internationally for electronic bank statements.
    Uses the mt940 library if available.
    """

    @property
    def format_name(self) -> str:
        return "MT940"

    def can_parse(self, content: str, filename: str = "") -> bool:
        """Check for MT940 markers"""
        if filename.lower().endswith(('.mt940', '.sta', '.940')):
            return True

        content_start = content[:200].strip()
        return (
            content_start.startswith(':20:') or
            content_start.startswith('{1:') or
            ':60F:' in content_start or
            ':61:' in content_start
        )

    def parse(self, content: str, filename: str = "") -> List[ParsedTransaction]:
        """Parse MT940 content using mt940 library"""
        transactions = []

        try:
            import mt940

            # Parse MT940 content
            statements = mt940.parse(content)

            for statement in statements:
                for txn in statement.transactions:
                    try:
                        # Get transaction details
                        txn_date = txn.data.get('date', date.today())
                        if hasattr(txn_date, 'date'):
                            txn_date = txn_date.date()

                        # Amount (positive or negative)
                        amount = float(txn.data.get('amount', 0))

                        # Get description fields
                        description = txn.data.get('transaction_details', '') or ''
                        extra = txn.data.get('extra_details', '') or ''

                        # Extract name from description
                        name = description.split('\n')[0][:50] if description else ''

                        # Get reference
                        reference = txn.data.get('customer_reference', '') or ''
                        bank_reference = txn.data.get('bank_reference', '') or ''

                        # Get transaction type
                        txn_type = txn.data.get('id', '') or ''

                        transactions.append(ParsedTransaction(
                            date=txn_date,
                            amount=amount,
                            name=name.strip(),
                            reference=reference or bank_reference,
                            memo=(description + ' ' + extra).strip(),
                            fit_id=bank_reference,
                            subcategory=txn_type,
                            raw_data=dict(txn.data)
                        ))

                    except Exception as e:
                        logger.warning(f"Error parsing MT940 transaction: {e}")
                        continue

        except ImportError:
            logger.error("mt940 library not installed. Run: pip install mt940")
            # Fall back to manual parsing
            transactions = self._parse_mt940_manual(content)
        except Exception as e:
            logger.error(f"Error parsing MT940 file: {e}")
            transactions = self._parse_mt940_manual(content)

        return transactions

    def _parse_mt940_manual(self, content: str) -> List[ParsedTransaction]:
        """
        Manual MT940 parsing fallback.

        MT940 transaction line format:
        :61:VALUTADATEAMOUNTTYPEREF
        :86:Details
        """
        transactions = []

        # Find :61: transaction lines
        lines = content.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i].strip()

            if line.startswith(':61:'):
                try:
                    # Parse :61: line
                    data = line[4:]

                    # Date (YYMMDD)
                    date_str = data[:6]
                    try:
                        txn_date = datetime.strptime(date_str, '%y%m%d').date()
                    except ValueError:
                        i += 1
                        continue

                    # Credit/Debit indicator and amount
                    remaining = data[6:]

                    # Find amount (after C/D/RC/RD indicator)
                    amount_match = re.search(r'([CR]D?)([0-9,]+)', remaining)
                    if amount_match:
                        indicator = amount_match.group(1)
                        amount_str = amount_match.group(2).replace(',', '.')
                        amount = float(amount_str)

                        # C = Credit (positive), D = Debit (negative)
                        if indicator.startswith('D'):
                            amount = -amount
                    else:
                        i += 1
                        continue

                    # Look for :86: details on next line
                    description = ""
                    if i + 1 < len(lines) and lines[i + 1].strip().startswith(':86:'):
                        description = lines[i + 1].strip()[4:]
                        i += 1

                    # Extract name from description
                    name = description[:50] if description else ""

                    transactions.append(ParsedTransaction(
                        date=txn_date,
                        amount=amount,
                        name=name,
                        memo=description,
                        raw_data={'line61': line, 'description': description}
                    ))

                except Exception as e:
                    logger.warning(f"Error in manual MT940 parsing: {e}")

            i += 1

        return transactions


# Registry of all available parsers
PARSERS: List[BankFileParser] = [
    OFXParser(),
    QIFParser(),
    MT940Parser(),
    CSVParser(),  # CSV is last as fallback
]


def detect_format(content: str, filename: str = "") -> Optional[str]:
    """
    Detect the format of bank statement content.

    Args:
        content: File content as string
        filename: Optional filename for extension-based detection

    Returns:
        Format name if detected, None otherwise
    """
    for parser in PARSERS:
        if parser.can_parse(content, filename):
            return parser.format_name
    return None


def get_parser(format_name: str) -> Optional[BankFileParser]:
    """
    Get parser by format name.

    Args:
        format_name: Format name (CSV, OFX, QIF, MT940)

    Returns:
        Parser instance or None
    """
    for parser in PARSERS:
        if parser.format_name.upper() == format_name.upper():
            return parser
    return None


def detect_and_parse(filepath: str) -> Tuple[List[ParsedTransaction], str]:
    """
    Auto-detect format and parse bank statement file.

    Args:
        filepath: Path to bank statement file

    Returns:
        Tuple of (transactions, format_name)

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If format cannot be detected
    """
    path = Path(filepath)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # Read file content
    # Try UTF-8 first, fall back to latin-1
    try:
        content = path.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = path.read_text(encoding='latin-1')

    filename = path.name

    # Find appropriate parser
    for parser in PARSERS:
        if parser.can_parse(content, filename):
            logger.info(f"Detected format: {parser.format_name}")
            transactions = parser.parse(content, filename)
            return transactions, parser.format_name

    raise ValueError(f"Could not detect format for file: {filepath}")


def parse_file(filepath: str, format_name: Optional[str] = None) -> List[ParsedTransaction]:
    """
    Parse bank statement file with optional format override.

    Args:
        filepath: Path to bank statement file
        format_name: Optional format override (CSV, OFX, QIF, MT940)

    Returns:
        List of ParsedTransaction objects
    """
    if format_name:
        parser = get_parser(format_name)
        if not parser:
            raise ValueError(f"Unknown format: {format_name}")

        path = Path(filepath)
        try:
            content = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            content = path.read_text(encoding='latin-1')

        return parser.parse(content, path.name)
    else:
        transactions, _ = detect_and_parse(filepath)
        return transactions
