"""
People's Pension Export

Generates People's Pension-format CSV files for contribution submissions.
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

    The People's Pension is one of the UK's largest workplace pension providers.
    """

    PROVIDER_NAME = "Peoples Pension"
    SCHEME_TYPES = [4]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate People's Pension-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Employer ID',
            'Payroll ID',
            'National Insurance Number',
            'Title',
            'First Name',
            'Middle Name',
            'Last Name',
            'Date of Birth',
            'Gender',
            'Address Line 1',
            'Address Line 2',
            'Address Line 3',
            'City',
            'Postcode',
            'Country',
            'Email',
            'Mobile',
            'Employment Start Date',
            'Leaving Date',
            'Pensionable Pay',
            'Employee Regular Contribution',
            'Employer Regular Contribution',
            'Employee Additional Contribution',
            'Employer Additional Contribution',
            'Contribution Period Start',
            'Contribution Period End',
            'Payment Due Date',
            'Worker Status',
            'Opt Out Indicator'
        ])

        employer_id = scheme_config.get('wps_scref', '').strip()

        for c in contributions:
            # Worker status
            status = 'Active'
            if c.is_new_starter:
                status = 'New'
            elif c.is_leaver:
                status = 'Leaver'

            writer.writerow([
                employer_id,
                c.employee_ref,
                c.ni_number,
                c.title,
                c.forename,
                '',  # Middle Name
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                c.gender,
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                c.postcode,
                'United Kingdom',
                '',  # Email
                '',  # Mobile
                c.start_date.strftime('%d/%m/%Y') if c.start_date else '',
                c.leave_date.strftime('%d/%m/%Y') if c.leave_date else '',
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                '0.00',  # Employee Additional
                '0.00',  # Employer Additional
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                payment_date.strftime('%d/%m/%Y'),
                status,
                'Y' if c.opt_out else 'N'
            ])

        return output.getvalue()
