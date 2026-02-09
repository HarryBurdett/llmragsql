"""
Aviva Pension Export

Generates Aviva-format CSV files for pension contribution submissions.
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

    Aviva CSV format includes:
    - Employee details (NI, name, DOB, address)
    - Scheme membership number
    - Contribution amounts
    - Employment status
    """

    PROVIDER_NAME = "Aviva"
    SCHEME_TYPES = [1]  # wps_type = 1 for Aviva

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Aviva-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Employee Reference',
            'NI Number',
            'Title',
            'Forename',
            'Surname',
            'Date of Birth',
            'Gender',
            'Address Line 1',
            'Address Line 2',
            'Address Line 3',
            'Address Line 4',
            'Postcode',
            'Scheme Membership Number',
            'Earnings Period Start',
            'Earnings Period End',
            'Pensionable Earnings',
            'Employee Contribution',
            'Employer Contribution',
            'Total Contribution',
            'Employee Contribution %',
            'Employer Contribution %',
            'New Starter',
            'Leaver',
            'Opt Out'
        ])

        # Data rows
        scheme_ref = scheme_config.get('wps_scref', '').strip()

        for c in contributions:
            writer.writerow([
                c.employee_ref,
                c.ni_number,
                c.title,
                c.forename,
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                c.gender,
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                c.postcode,
                scheme_ref,
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                f'{c.total_contribution:.2f}',
                f'{c.employee_rate:.2f}',
                f'{c.employer_rate:.2f}',
                'Y' if c.is_new_starter else 'N',
                'Y' if c.is_leaver else 'N',
                'Y' if c.opt_out else 'N'
            ])

        return output.getvalue()
