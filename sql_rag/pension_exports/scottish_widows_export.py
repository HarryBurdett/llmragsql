"""
Scottish Widows Pension Export

Generates Scottish Widows-format CSV files for pension contribution submissions.
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class ScottishWidowsExport(BasePensionExport):
    """
    Scottish Widows Pension Export Generator

    Scottish Widows CSV format for workplace pension contributions.
    """

    PROVIDER_NAME = "Scottish Widows"
    SCHEME_TYPES = [2]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Scottish Widows-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Payroll Reference',
            'National Insurance Number',
            'Title',
            'First Name',
            'Last Name',
            'Date of Birth',
            'Sex',
            'Address 1',
            'Address 2',
            'Address 3',
            'Town',
            'Postcode',
            'Email Address',
            'Contribution Start Date',
            'Contribution End Date',
            'Payment Date',
            'Qualifying Earnings',
            'Member Contribution Amount',
            'Employer Contribution Amount',
            'Member Contribution Percentage',
            'Employer Contribution Percentage',
            'Reason Code',
            'Scheme Policy Number'
        ])

        scheme_ref = scheme_config.get('wps_scref', '').strip()

        for c in contributions:
            # Determine reason code
            reason = ''
            if c.is_new_starter:
                reason = 'NEW'
            elif c.is_leaver:
                reason = 'LEAVER'
            elif c.opt_out:
                reason = 'OPTOUT'

            writer.writerow([
                c.employee_ref,
                c.ni_number,
                c.title,
                c.forename,
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                'M' if c.gender.upper() == 'M' else 'F',
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                c.postcode,
                '',  # Email - not stored in Opera
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                payment_date.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                f'{c.employee_rate:.2f}',
                f'{c.employer_rate:.2f}',
                reason,
                scheme_ref
            ])

        return output.getvalue()
