"""
Royal London Pension Export

Generates Royal London-format CSV files for contribution submissions.
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

    Royal London workplace pension contribution file format.
    """

    PROVIDER_NAME = "Royal London"
    SCHEME_TYPES = [5]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Royal London-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Scheme Number',
            'Employer Reference',
            'Member Reference',
            'NI Number',
            'Title',
            'Forename',
            'Surname',
            'Date of Birth',
            'Sex',
            'Address Line 1',
            'Address Line 2',
            'Address Line 3',
            'Address Line 4',
            'Postcode',
            'Date Joined Scheme',
            'Date Left Scheme',
            'Earnings Period From',
            'Earnings Period To',
            'Pensionable Salary',
            'Employee Contribution',
            'Employer Contribution',
            'Employee AVC',
            'Employer AVC',
            'Salary Sacrifice',
            'Status Code'
        ])

        scheme_ref = scheme_config.get('wps_scref', '').strip()
        scheme_number = scheme_config.get('wps_prref', '').strip()

        for c in contributions:
            # Status codes: A=Active, N=New, L=Leaver, O=Opted Out
            status = 'A'
            if c.is_new_starter:
                status = 'N'
            elif c.is_leaver:
                status = 'L'
            elif c.opt_out:
                status = 'O'

            writer.writerow([
                scheme_number,
                scheme_ref,
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
                c.scheme_join_date.strftime('%d/%m/%Y') if c.scheme_join_date else '',
                c.leave_date.strftime('%d/%m/%Y') if c.leave_date else '',
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                '0.00',  # Employee AVC
                '0.00',  # Employer AVC
                '0.00',  # Salary Sacrifice
                status
            ])

        return output.getvalue()
