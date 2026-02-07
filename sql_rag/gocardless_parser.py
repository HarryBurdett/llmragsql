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
    currency: str = 'GBP'  # Currency detected from email (e.g., GBP, EUR)

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


def detect_currency(content: str) -> str:
    """Detect payout currency from email content.

    For GoCardless emails, the payout currency is what matters (what the bank receives).
    This is typically in the subject line or Net amount line.
    Foreign currency transactions get converted to the payout currency.

    Priority:
    1. Subject line currency (e.g., "GoCardless has paid you 520.59 GBP")
    2. Net amount line currency
    3. Most frequently mentioned currency
    """
    # First, check subject line for payout currency
    subject_match = re.search(r'(?:paid|payout|payment)[^\n]*?(GBP|EUR|USD|CAD|AUD)', content, re.IGNORECASE)
    if subject_match:
        return subject_match.group(1).upper()

    # Check "Net amount" line specifically (that's what the bank receives)
    net_match = re.search(r'Net amount[^\n]*?(GBP|EUR|USD|CAD|AUD)', content, re.IGNORECASE)
    if net_match:
        return net_match.group(1).upper()

    # Common currency codes - count all mentions
    currency_pattern = r'\b(GBP|EUR|USD|CAD|AUD|NZD|CHF|SEK|NOK|DKK)\b'
    matches = re.findall(currency_pattern, content.upper())

    if not matches:
        return 'GBP'  # Default

    # Return the most common currency found
    from collections import Counter
    currency_counts = Counter(matches)
    return currency_counts.most_common(1)[0][0]


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

    # Extract payment date (e.g., "the money should arrive by January 7th" or "7 January")
    for line in cleaned_lines:
        if 'arrive by' in line.lower() or 'should arrive' in line.lower() or 'paid on' in line.lower():
            # Try different date formats
            # Format 1: "January 7th" or "January 7"
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d+)(?:st|nd|rd|th)?', line, re.IGNORECASE)
            if date_match:
                month_name = date_match.group(1)
                day = int(date_match.group(2))
                try:
                    # Parse month name to number
                    month_num = datetime.strptime(month_name, '%B').month
                    # Use current year, but handle year boundary (if month is in future, use previous year)
                    current_year = datetime.now().year
                    current_month = datetime.now().month
                    year = current_year if month_num <= current_month + 1 else current_year - 1
                    payment_date = datetime(year, month_num, day)
                except ValueError:
                    pass
                break

            # Format 2: "7 January" or "7th January"
            date_match = re.search(r'(\d+)(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)', line, re.IGNORECASE)
            if date_match:
                day = int(date_match.group(1))
                month_name = date_match.group(2)
                try:
                    month_num = datetime.strptime(month_name, '%B').month
                    current_year = datetime.now().year
                    current_month = datetime.now().month
                    year = current_year if month_num <= current_month + 1 else current_year - 1
                    payment_date = datetime(year, month_num, day)
                except ValueError:
                    pass
                break

    # Parse the payment table
    # Look for patterns like: "Customer Name    Description    Amount" (single line)
    # Or vertical format where Customer/Description/Amount are on separate lines
    # The table data follows with customer entries

    in_payment_table = False
    current_customer = None
    current_description = None
    header_parts_seen = set()  # Track vertical header parts

    for i, line in enumerate(cleaned_lines):
        lower_line = line.lower().strip()

        # Detect start of payment table (header row - horizontal or vertical)
        # Horizontal: "Customer    Description    Amount" on one line
        if 'customer' in lower_line and 'description' in lower_line and 'amount' in lower_line:
            in_payment_table = True
            header_parts_seen.clear()
            continue

        # Vertical header: "Customer" / "Description" / "Amount" on separate lines
        if lower_line == 'customer':
            header_parts_seen.add('customer')
            continue
        if lower_line == 'description':
            header_parts_seen.add('description')
            continue
        if lower_line == 'amount':
            header_parts_seen.add('amount')
            # If we've seen all three header parts, we're in the payment table
            if header_parts_seen == {'customer', 'description', 'amount'}:
                in_payment_table = True
                header_parts_seen.clear()
            continue

        # Check for summary fields (can appear at any point after payments)
        # Handle both horizontal (label + amount on same line) and vertical (amount on next line) formats
        lower_line = line.lower()

        # Helper to get amount from current line or next line
        def get_amount_from_line_or_next(current_line: str, next_idx: int) -> Optional[float]:
            # Try current line first
            amount_match = re.search(r'-?([\d,]+\.?\d*)\s*(?:GBP|EUR|USD)', current_line, re.IGNORECASE)
            if amount_match:
                return parse_amount(amount_match.group(0))
            # Try next line (vertical format)
            if next_idx < len(cleaned_lines):
                next_line = cleaned_lines[next_idx]
                amount_match = re.search(r'^-?([\d,]+\.?\d*)\s*(?:GBP|EUR|USD)?$', next_line.strip(), re.IGNORECASE)
                if amount_match:
                    return parse_amount(amount_match.group(0))
            return None

        if 'gross amount' in lower_line:
            amount = get_amount_from_line_or_next(line, i + 1)
            if amount is not None:
                gross_amount = amount
            in_payment_table = False  # Stop looking for more payments
            continue

        if 'gocardless fees' in lower_line:
            amount = get_amount_from_line_or_next(line, i + 1)
            if amount is not None:
                gocardless_fees = abs(amount)  # Fees are always positive
            continue

        if 'app fees' in lower_line:
            amount = get_amount_from_line_or_next(line, i + 1)
            if amount is not None:
                app_fees = abs(amount)
            continue

        if 'vat total fees' in lower_line or 'vat on fees' in lower_line:
            amount = get_amount_from_line_or_next(line, i + 1)
            if amount is not None:
                vat_on_fees = abs(amount)
            continue

        if 'net amount' in lower_line:
            amount = get_amount_from_line_or_next(line, i + 1)
            if amount is not None:
                net_amount = amount
            continue

        # Process payment rows only while in the payment table
        if in_payment_table:

            # Try to parse as payment row
            # Format 1: Single line "Customer Name    Description    615.00 GBP"
            # Format 2: Multi-line where customer, description, amount are on separate lines
            # Accept any currency (GBP, EUR, USD, etc.) - filtering by payout currency happens later
            amount_match = re.search(r'([\d,]+\.?\d+)\s*(?:GBP|EUR|USD|CAD|AUD)\s*$', line, re.IGNORECASE)
            if amount_match:
                amount = parse_amount(amount_match.group(1))
                # Everything before the amount is customer + description
                prefix = line[:amount_match.start()].strip()

                if prefix:
                    # Format 1: Single line with customer and description before amount
                    # Try to split prefix into customer and description
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
                elif current_customer:
                    # Format 2: Multi-line - we have customer/description from previous lines
                    customer_name = current_customer
                    description = current_description or ''
                    current_customer = None
                    current_description = None
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
            elif not amount_match and line.strip() and not any(kw in line.lower() for kw in ['gross', 'fees', 'vat', 'net', 'exchange', 'arrive', 'reference']):
                # This might be a customer name or description line (multi-line format)
                # If we don't have a current customer, this line is the customer name
                # If we have a customer but no description, this is the description
                if not current_customer:
                    current_customer = line.strip()
                elif not current_description:
                    current_description = line.strip()

    # If we didn't find gross_amount, calculate from payments
    if gross_amount == 0.0 and payments:
        gross_amount = sum(p.amount for p in payments)

    # Detect currency from content
    currency = detect_currency(content)

    return GoCardlessBatch(
        payments=payments,
        gross_amount=gross_amount,
        gocardless_fees=gocardless_fees,
        app_fees=app_fees,
        vat_on_fees=vat_on_fees,
        net_amount=net_amount,
        bank_reference=bank_reference,
        payment_date=payment_date,
        email_subject=email_subject,
        currency=currency
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

    # Detect currency from content
    currency = detect_currency(table_text)

    return GoCardlessBatch(
        payments=payments,
        gross_amount=gross_amount,
        gocardless_fees=gocardless_fees,
        app_fees=app_fees,
        vat_on_fees=vat_on_fees,
        net_amount=net_amount,
        currency=currency
    )
