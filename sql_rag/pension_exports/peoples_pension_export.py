"""
People's Pension Export

Generates People's Pension-format CSV files for contribution submissions.
Uses the H (Header) / D (Detail) / T (Trailer) record format.

Reference: https://gb-kb.sage.com/portal/app/portlets/results/view2.jsp?k2dockey=210203182948403
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class PeoplesPensionExport(BasePensionExport):
    """
    People's Pension Export Generator

    The People's Pension contribution file uses H/D/T record format:
    - H: Header record with account and period info
    - CO/D: Detail records for each employee contribution
    - T: Trailer record with totals
    """

    PROVIDER_NAME = "Peoples Pension"
    SCHEME_TYPES = [4]

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate People's Pension-format CSV with H/D/T records."""
        output = io.StringIO()
        writer = csv.writer(output)

        account_no = scheme_config.get('wps_scref', '').strip()

        # Calculate totals for trailer record
        total_contributions = sum(
            c.employee_contribution + c.employer_contribution
            for c in contributions
        )

        # Header record (H)
        writer.writerow([
            'H',                                    # Record type
            account_no,                             # Account no (Provider Reference)
            period_start.strftime('%d/%m/%Y'),      # Start date (Pay Reference Period start)
            period_end.strftime('%d/%m/%Y')         # End date (Pay Reference Period end)
        ])

        # Detail records (CO/D)
        for c in contributions:
            # Calculate pensionable earnings minus AE thresholds if applicable
            pensionable_per_prp = c.pensionable_earnings

            # Missing/partial pension code (blank unless needed)
            missing_code = ''
            if c.opt_out:
                missing_code = 'OO'  # Opted Out
            elif c.is_leaver:
                missing_code = 'LV'  # Leaver

            writer.writerow([
                'CO',                                   # Record type (Contribution)
                'D',                                    # Detail marker
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',  # Date of birth
                c.employee_ref,                         # Unique identifier (RTI Payroll ID)
                f'{c.employer_contribution:.2f}',       # Employer pension contributions
                f'{c.employee_contribution:.2f}',       # Employee pension contributions
                missing_code,                           # Missing/partial pension code
                '',                                     # EAC/ELC premium (blank)
                f'{pensionable_per_prp:.2f}'            # Pensionable earnings per PRP
            ])

        # Trailer record (T)
        writer.writerow([
            'T',                                    # Record type
            f'{total_contributions:.2f}'            # Contributions total
        ])

        return output.getvalue()
