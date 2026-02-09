"""
Aviva Pension Export

Generates Aviva ESZ (Employer Servicing Zone) Payments format CSV.
Based on Aviva MyAvivaBusiness file specification.

Reference: https://gb-kb.sage.com/portal/app/portlets/results/view2.jsp?k2dockey=210128172659127
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class AvivaExport(BasePensionExport):
    """
    Aviva Pension Export Generator

    Aviva ESZ Payments format with 10 columns.
    NI number is Aviva's unique identifier.
    Employee contribution is net, employer is gross.
    """

    PROVIDER_NAME = "Aviva"
    SCHEME_TYPES = [1]

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Aviva ESZ Payments format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row - 10 columns as per ESZ Payments specification
        writer.writerow([
            'Payroll month',
            'Name',
            'NI number',
            'Alternative unique ID',
            'Employer regular contribution amount',
            'Employee regular deduction',
            'Reason for partial or non-payment',
            'Employer one off contribution',
            'Employee one off contribution',
            'New Category ID'
        ])

        # Format payroll month as MM/YYYY
        payroll_month = payment_date.strftime('%m/%Y')

        for c in contributions:
            # Determine reason code for partial/non-payment
            reason_code = ''
            if c.opt_out:
                reason_code = 'OO'  # Opted Out
            elif c.is_leaver:
                reason_code = 'LV'  # Leaver
            elif c.employee_contribution == 0 and c.employer_contribution == 0:
                reason_code = 'NP'  # No Payment

            # Alternative ID used when NI number unavailable
            alt_id = c.alternative_id or ''

            writer.writerow([
                payroll_month,                         # Payroll month (MM/YYYY)
                f'{c.surname}, {c.forename}',          # Name
                c.ni_number,                           # NI number (unique identifier)
                alt_id,                                # Alternative unique ID
                f'{c.employer_contribution:.2f}',      # Employer regular contribution (gross)
                f'{c.employee_contribution:.2f}',      # Employee regular deduction (net)
                reason_code,                           # Reason for partial or non-payment
                '0.00',                                # Employer one off contribution
                '0.00',                                # Employee one off contribution
                ''                                     # New Category ID
            ])

        return output.getvalue()
