"""
Supplier Statement Extraction using Claude Vision.

Extracts transaction data from supplier statements (PDF or text).
"""

import base64
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

from anthropic import Anthropic

logger = logging.getLogger(__name__)


@dataclass
class SupplierStatementInfo:
    """Information about the supplier statement."""
    supplier_name: str
    account_reference: Optional[str] = None
    statement_date: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    currency: str = "GBP"
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None


@dataclass
class SupplierStatementLine:
    """A single line from a supplier statement."""
    date: str
    reference: Optional[str] = None
    description: Optional[str] = None
    debit: Optional[float] = None  # Invoices (amounts we owe)
    credit: Optional[float] = None  # Payments/credit notes (reducing what we owe)
    balance: Optional[float] = None
    doc_type: Optional[str] = None  # INV, CN, PMT, etc.


class SupplierStatementExtractor:
    """
    Extract supplier statement data using Claude Vision API.

    Handles both PDF attachments and text-based statements.
    """

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        """
        Initialize the extractor.

        Args:
            api_key: Anthropic API key
            model: Claude model to use (should support vision for PDFs)
        """
        self.client = Anthropic(api_key=api_key)
        self.model = model

    def extract_from_pdf(self, pdf_path: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from a PDF file using Claude Vision.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        # Read and encode the PDF
        pdf_bytes = Path(pdf_path).read_bytes()
        pdf_base64 = base64.standard_b64encode(pdf_bytes).decode('utf-8')

        return self._extract_with_vision(pdf_base64, "application/pdf")

    def extract_from_pdf_bytes(self, pdf_bytes: bytes) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from PDF bytes.

        Args:
            pdf_bytes: Raw PDF content

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        pdf_base64 = base64.standard_b64encode(pdf_bytes).decode('utf-8')
        return self._extract_with_vision(pdf_base64, "application/pdf")

    def extract_from_text(self, text: str, sender_email: Optional[str] = None) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract statement data from plain text (email body).

        Args:
            text: The statement text content
            sender_email: Optional sender email for supplier identification

        Returns:
            Tuple of (SupplierStatementInfo, list of SupplierStatementLine)
        """
        extraction_prompt = self._get_text_extraction_prompt(sender_email)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": f"{extraction_prompt}\n\nStatement text:\n```\n{text}\n```"
                }
            ]
        )

        return self._parse_response(response.content[0].text)

    def _extract_with_vision(self, base64_data: str, media_type: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Extract using Claude Vision API.
        """
        extraction_prompt = self._get_pdf_extraction_prompt()

        response = self.client.messages.create(
            model=self.model,
            max_tokens=8000,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64_data
                            }
                        },
                        {
                            "type": "text",
                            "text": extraction_prompt
                        }
                    ]
                }
            ]
        )

        return self._parse_response(response.content[0].text)

    def _get_pdf_extraction_prompt(self) -> str:
        """Get the extraction prompt for PDF statements."""
        return """Analyze this supplier statement and extract ALL line items.

Return a JSON object with this exact structure:
{
    "statement_info": {
        "supplier_name": "Full supplier/company name",
        "account_reference": "Our account number with this supplier",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD or null",
        "period_end": "YYYY-MM-DD or null",
        "opening_balance": 1234.56,
        "closing_balance": 5678.90,
        "currency": "GBP",
        "contact_email": "email if shown",
        "contact_phone": "phone if shown"
    },
    "lines": [
        {
            "date": "YYYY-MM-DD",
            "reference": "Invoice/CN/payment reference",
            "description": "Description or details",
            "debit": 100.00,
            "credit": null,
            "balance": 1234.56,
            "doc_type": "INV"
        }
    ]
}

Important rules:
- Extract EVERY line item, don't skip any
- debit = amounts we owe (invoices) - positive number or null
- credit = amounts reducing what we owe (payments, credit notes) - positive number or null
- doc_type should be: INV (invoice), CN (credit note), PMT (payment), ADJ (adjustment), or null if unclear
- Use YYYY-MM-DD format for all dates
- Include the full reference exactly as shown
- If opening/closing balance not explicit, calculate from the lines
- Return ONLY valid JSON, no other text"""

    def _get_text_extraction_prompt(self, sender_email: Optional[str] = None) -> str:
        """Get the extraction prompt for text-based statements."""
        extra_context = f"\nThe statement was sent from: {sender_email}" if sender_email else ""

        return f"""Analyze this supplier statement text and extract ALL line items.{extra_context}

Return a JSON object with this exact structure:
{{
    "statement_info": {{
        "supplier_name": "Full supplier/company name",
        "account_reference": "Our account number with this supplier",
        "statement_date": "YYYY-MM-DD",
        "period_start": "YYYY-MM-DD or null",
        "period_end": "YYYY-MM-DD or null",
        "opening_balance": 1234.56,
        "closing_balance": 5678.90,
        "currency": "GBP",
        "contact_email": "email if shown",
        "contact_phone": "phone if shown"
    }},
    "lines": [
        {{
            "date": "YYYY-MM-DD",
            "reference": "Invoice/CN/payment reference",
            "description": "Description or details",
            "debit": 100.00,
            "credit": null,
            "balance": 1234.56,
            "doc_type": "INV"
        }}
    ]
}}

Important rules:
- Extract EVERY line item, don't skip any
- debit = amounts we owe (invoices) - positive number or null
- credit = amounts reducing what we owe (payments, credit notes) - positive number or null
- doc_type should be: INV (invoice), CN (credit note), PMT (payment), ADJ (adjustment), or null if unclear
- Use YYYY-MM-DD format for all dates
- Include the full reference exactly as shown
- If balances not explicit, calculate from lines
- Return ONLY valid JSON, no other text"""

    def _parse_response(self, response_text: str) -> Tuple[SupplierStatementInfo, List[SupplierStatementLine]]:
        """
        Parse Claude's response into structured data.
        """
        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', response_text)
        if not json_match:
            raise ValueError(f"Could not extract JSON from response: {response_text[:500]}")

        try:
            data = json.loads(json_match.group())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in response: {e}")

        # Parse statement info
        info_data = data.get('statement_info', {})
        statement_info = SupplierStatementInfo(
            supplier_name=info_data.get('supplier_name', 'Unknown Supplier'),
            account_reference=info_data.get('account_reference'),
            statement_date=info_data.get('statement_date'),
            period_start=info_data.get('period_start'),
            period_end=info_data.get('period_end'),
            opening_balance=info_data.get('opening_balance'),
            closing_balance=info_data.get('closing_balance'),
            currency=info_data.get('currency', 'GBP'),
            contact_email=info_data.get('contact_email'),
            contact_phone=info_data.get('contact_phone')
        )

        # Parse statement lines
        lines = []
        for line_data in data.get('lines', []):
            line = SupplierStatementLine(
                date=line_data.get('date', ''),
                reference=line_data.get('reference'),
                description=line_data.get('description'),
                debit=line_data.get('debit'),
                credit=line_data.get('credit'),
                balance=line_data.get('balance'),
                doc_type=line_data.get('doc_type')
            )
            lines.append(line)

        logger.info(f"Extracted statement from {statement_info.supplier_name}: {len(lines)} lines")
        return statement_info, lines

    def to_dict(self, info: SupplierStatementInfo, lines: List[SupplierStatementLine]) -> Dict[str, Any]:
        """
        Convert extraction results to dictionary for JSON serialization.
        """
        return {
            "statement_info": {
                "supplier_name": info.supplier_name,
                "account_reference": info.account_reference,
                "statement_date": info.statement_date,
                "period_start": info.period_start,
                "period_end": info.period_end,
                "opening_balance": info.opening_balance,
                "closing_balance": info.closing_balance,
                "currency": info.currency,
                "contact_email": info.contact_email,
                "contact_phone": info.contact_phone
            },
            "lines": [
                {
                    "date": line.date,
                    "reference": line.reference,
                    "description": line.description,
                    "debit": line.debit,
                    "credit": line.credit,
                    "balance": line.balance,
                    "doc_type": line.doc_type
                }
                for line in lines
            ],
            "summary": {
                "total_lines": len(lines),
                "total_debits": sum(l.debit or 0 for l in lines),
                "total_credits": sum(l.credit or 0 for l in lines)
            }
        }
