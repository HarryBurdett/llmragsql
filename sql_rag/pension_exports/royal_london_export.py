"""
Royal London Pension Export

Generates Royal London-format CSV files for contribution submissions.
Based on Royal London Making Contributions file specification.

Reference: https://gb-kb.sage.com/portal/app/portlets/results/view2.jsp?k2dockey=210212175156280
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class RoyalLondonExport(BasePensionExport):
    """
    Royal London Pension Export Generator

    Royal London Making Contributions file format with 9 columns.
    """

    PROVIDER_NAME = "Royal London"
    SCHEME_TYPES = [5]

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Royal London Making Contributions format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row - 9 columns as per specification
        writer.writerow([
            'Title',
            'First name',
            'Last name',
            'Date of birth',
            'National insurance number',
            'Payroll reference',
            'Earnings in contribution period',
            'Worker contribution £',
            'Employer contribution £'
        ])

        for c in contributions:
            writer.writerow([
                c.title,
                c.forename,
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                c.ni_number,
                c.employee_ref,
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}'
            ])

        return output.getvalue()
