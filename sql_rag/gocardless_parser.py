"""
GoCardless Email Parser

Parses GoCardless payment notification emails to extract:
- Individual customer payments (customer name, description, amount)
- Summary totals (gross, fees, VAT, net)
- Bank reference for matching to bank statement
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from datetime import datetime


@dataclass
class GoCardlessPayment:
    """Individual customer payment from GoCardless batch."""
    customer_name: str
    description: str
    amount: float
    invoice_refs: List[str] = field(default_factory=list)

    # Matching fields (populated during customer matching)
    matched_account: Optional[str] = None
    matched_name: Optional[str] = None
    match_score: float = 0.0
    match_status: str = 'unmatched'  # 'matched', 'unmatched', 'multiple'


@dataclass
class GoCardlessBatch:
    """Parsed GoCardless payment batch."""
    payments: List[GoCardlessPayment]
    gross_amount: float
    gocardless_fees: float
    app_fees: float
    vat_on_fees: float
    net_amount: float
    bank_reference: Optional[str] = None
    payment_date: Optional[datetime] = None
    email_subject: Optional[str] = None

    @property
    def total_fees(self) -> float:
        """Total fees (GoCardless + App + VAT)."""
        return abs(self.gocardless_fees) + abs(self.app_fees) + abs(self.vat_on_fees)

    @property
    def payment_count(self) -> int:
        """Number of individual payments."""
        return len(self.payments)

    @property
    def calculated_gross(self) -> float:
        """Sum of individual payments (should match gross_amount)."""
        return sum(p.amount for p in self.payments)


def parse_amount(amount_str: str) -> float:
    """Parse amount string like '7,380.00 GBP' or '7380.00' to float."""
    # Remove currency codes and whitespace
    cleaned = re.sub(r'[A-Z]{3}|\s', '', amount_str)
    # Remove commas
    cleaned = cleaned.replace(',', '')
    # Handle negative amounts (shown with minus or in red)
    cleaned = cleaned.replace('−', '-').replace('–', '-')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def extract_invoice_refs(description: str) -> List[str]:
    """Extract invoice references from description field.

    Examples:
        'Intsys INV26362,26363' -> ['INV26362', 'INV26363']
        'Intsys INV26365' -> ['INV26365']
        'Intsys Opera 3 Support' -> []
    """
    refs = []

    # Pattern 1: INV followed by numbers, possibly comma-separated
    inv_match = re.search(r'INV(\d+(?:,\d+)*)', description, re.IGNORECASE)
    if inv_match:
        numbers = inv_match.group(1).split(',')
        for num in numbers:
            refs.append(f'INV{num.strip()}')

    return refs


def parse_gocardless_email(content: str) -> GoCardlessBatch:
    """
    Parse GoCardless payment notification email content.

    Args:
        content: Raw email text or HTML content

    Returns:
        GoCardlessBatch with parsed payment details
    """
    payments = []
    gross_amount = 0.0
    gocardless_fees = 0.0
    app_fees = 0.0
    vat_on_fees = 0.0
    net_amount = 0.0
    bank_reference = None
    payment_date = None
    email_subject = None

    # Clean up the content - normalize whitespace
    lines = content.strip().split('\n')
    cleaned_lines = [line.strip() for line in lines if line.strip()]

    # Extract subject line if present
    for line in cleaned_lines:
        if line.lower().startswith('subject:'):
            email_subject = line.split(':', 1)[1].strip()
            # Extract net amount from subject
            amount_match = re.search(r'([\d,]+\.?\d*)\s*GBP', email_subject)
            if amount_match:
                net_amount = parse_amount(amount_match.group(1))
            break

    # Extract bank reference
    for line in cleaned_lines:
        if 'reference:' in line.lower():
            ref_match = re.search(r'reference:\s*(\S+)', line, re.IGNORECASE)
            if ref_match:
                bank_reference = ref_match.group(1)
                break

    # Extract payment date
    for line in cleaned_lines:
        if 'arrive by' in line.lower():
            date_match = re.search(r'(\w+\s+\d+(?:st|nd|rd|th)?)', line)
            if date_match:
                # Store as string for now - can parse to datetime if needed
                pass
            break

    # Parse the payment table
    # Look for patterns like: "Customer Name    Description    Amount"
    # The table data follows with customer entries

    in_payment_table = False
    current_customer = None
    current_description = None

    for i, line in enumerate(cleaned_lines):
        # Detect start of payment table (header row)
        if 'customer' in line.lower() and 'description' in line.lower() and 'amount' in line.lower():
            in_payment_table = True
            continue

        # Check for summary fields (can appear at any point after payments)
        lower_line = line.lower()

        if 'gross amount' in lower_line:
            amount_match = re.search(r'([\d,]+\.?\d*)\s*GBP', line, re.IGNORECASE)
            if amount_match:
                gross_amount = parse_amount(amount_match.group(0))
            in_payment_table = False  # Stop looking for more payments
            continue

        if 'gocardless fees' in lower_line:
            amount_match = re.search(r'-?([\d,]+\.?\d*)\s*GBP', line, re.IGNORECASE)
            if amount_match:
                gocardless_fees = parse_amount(amount_match.group(0))
            continue

        if 'app fees' in lower_line:
            amount_match = re.search(r'-?([\d,]+\.?\d*)\s*GBP', line, re.IGNORECASE)
            if amount_match:
                app_fees = parse_amount(amount_match.group(0))
            continue

        if 'vat total fees' in lower_line or 'vat on fees' in lower_line:
            amount_match = re.search(r'-?([\d,]+\.?\d*)\s*GBP', line, re.IGNORECASE)
            if amount_match:
                vat_on_fees = parse_amount(amount_match.group(0))
            continue

        if 'net amount' in lower_line:
            amount_match = re.search(r'([\d,]+\.?\d*)\s*GBP', line, re.IGNORECASE)
            if amount_match:
                net_amount = parse_amount(amount_match.group(0))
            continue

        # Process payment rows only while in the payment table
        if in_payment_table:

            # Try to parse as payment row
            # Format varies - might be tab-separated or have GBP suffix
            amount_match = re.search(r'([\d,]+\.?\d+)\s*GBP\s*$', line)
            if amount_match:
                amount = parse_amount(amount_match.group(1))
                # Everything before the amount is customer + description
                prefix = line[:amount_match.start()].strip()

                # Try to split prefix into customer and description
                # This is tricky as there's no clear delimiter
                # Look for common patterns
                parts = re.split(r'\t+|\s{2,}', prefix)
                parts = [p.strip() for p in parts if p.strip()]

                if len(parts) >= 2:
                    customer_name = parts[0]
                    description = ' '.join(parts[1:])
                elif len(parts) == 1:
                    customer_name = parts[0]
                    description = ''
                else:
                    continue

                invoice_refs = extract_invoice_refs(description)

                payment = GoCardlessPayment(
                    customer_name=customer_name,
                    description=description,
                    amount=amount,
                    invoice_refs=invoice_refs
                )
                payments.append(payment)

    # If we didn't find gross_amount, calculate from payments
    if gross_amount == 0.0 and payments:
        gross_amount = sum(p.amount for p in payments)

    return GoCardlessBatch(
        payments=payments,
        gross_amount=gross_amount,
        gocardless_fees=gocardless_fees,
        app_fees=app_fees,
        vat_on_fees=vat_on_fees,
        net_amount=net_amount,
        bank_reference=bank_reference,
        payment_date=payment_date,
        email_subject=email_subject
    )


def parse_gocardless_table(table_text: str) -> GoCardlessBatch:
    """
    Parse just the table portion of a GoCardless email.
    Useful when user copies just the payment table.

    Args:
        table_text: Tab or space-separated table data

    Returns:
        GoCardlessBatch with parsed payments
    """
    payments = []
    gross_amount = 0.0
    gocardless_fees = 0.0
    app_fees = 0.0
    vat_on_fees = 0.0
    net_amount = 0.0

    lines = table_text.strip().split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        lower_line = line.lower()

        # Skip header row
        if 'customer' in lower_line and 'description' in lower_line:
            continue

        # Handle summary rows
        if 'gross amount' in lower_line:
            match = re.search(r'([\d,]+\.?\d*)', line.split('amount')[-1])
            if match:
                gross_amount = parse_amount(match.group(1))
            continue

        if 'gocardless fees' in lower_line:
            match = re.search(r'-?([\d,]+\.?\d*)', line.split('fees')[-1])
            if match:
                gocardless_fees = -abs(parse_amount(match.group(1)))
            continue

        if 'app fees' in lower_line:
            match = re.search(r'-?([\d,]+\.?\d*)', line.split('fees')[-1])
            if match:
                app_fees = -abs(parse_amount(match.group(1)))
            continue

        if 'vat' in lower_line and 'fees' in lower_line:
            match = re.search(r'-?([\d,]+\.?\d*)', line)
            if match:
                vat_on_fees = -abs(parse_amount(match.group(1)))
            continue

        if 'net amount' in lower_line:
            match = re.search(r'([\d,]+\.?\d*)', line.split('amount')[-1])
            if match:
                net_amount = parse_amount(match.group(1))
            continue

        # Try to parse as payment row
        # Split by tabs or multiple spaces
        parts = re.split(r'\t+|\s{2,}', line)
        parts = [p.strip() for p in parts if p.strip()]

        if len(parts) >= 3:
            customer_name = parts[0]
            description = parts[1]
            amount_str = parts[-1]
            amount = parse_amount(amount_str)

            if amount > 0:
                invoice_refs = extract_invoice_refs(description)
                payment = GoCardlessPayment(
                    customer_name=customer_name,
                    description=description,
                    amount=amount,
                    invoice_refs=invoice_refs
                )
                payments.append(payment)
        elif len(parts) == 2:
            # Might be customer + amount with no description
            customer_name = parts[0]
            amount = parse_amount(parts[1])
            if amount > 0:
                payment = GoCardlessPayment(
                    customer_name=customer_name,
                    description='',
                    amount=amount,
                    invoice_refs=[]
                )
                payments.append(payment)

    # Calculate gross if not found
    if gross_amount == 0.0 and payments:
        gross_amount = sum(p.amount for p in payments)

    return GoCardlessBatch(
        payments=payments,
        gross_amount=gross_amount,
        gocardless_fees=gocardless_fees,
        app_fees=app_fees,
        vat_on_fees=vat_on_fees,
        net_amount=net_amount
    )
