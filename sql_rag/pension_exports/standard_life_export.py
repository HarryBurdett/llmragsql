"""
Standard Life Pension Export

Generates Standard Life-format CSV files for contribution submissions.
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class StandardLifeExport(BasePensionExport):
    """
    Standard Life Pension Export Generator

    Standard Life (now part of Phoenix Group) workplace pension format.
    """

    PROVIDER_NAME = "Standard Life"
    SCHEME_TYPES = [6]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Standard Life-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Policy Number',
            'Employer Code',
            'Employee Number',
            'National Insurance Number',
            'Alternative ID',
            'Title',
            'First Name',
            'Surname',
            'Date of Birth',
            'Gender',
            'Address 1',
            'Address 2',
            'Address 3',
            'Town',
            'County',
            'Postcode',
            'Email Address',
            'Telephone',
            'Date Joined Company',
            'Date Joined Scheme',
            'Date Left',
            'Contribution Period Start',
            'Contribution Period End',
            'Payment Date',
            'Basic Pay',
            'Pensionable Earnings',
            'Employee Contribution',
            'Employer Contribution',
            'Bonus',
            'Salary Sacrifice',
            'AVCs',
            'Employer AVCs',
            'Member Category',
            'Event Type'
        ])

        policy = scheme_config.get('wps_prref', '').strip()
        employer_code = scheme_config.get('wps_scref', '').strip()

        for c in contributions:
            # Event types: REG=Regular, NEW=New Joiner, LEV=Leaver
            event = 'REG'
            if c.is_new_starter:
                event = 'NEW'
            elif c.is_leaver:
                event = 'LEV'

            writer.writerow([
                policy,
                employer_code,
                c.employee_ref,
                c.ni_number,
                c.alternative_id or '',
                c.title,
                c.forename,
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                'Male' if c.gender.upper() == 'M' else 'Female',
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                '',  # County
                c.postcode,
                '',  # Email
                '',  # Telephone
                c.start_date.strftime('%d/%m/%Y') if c.start_date else '',
                c.scheme_join_date.strftime('%d/%m/%Y') if c.scheme_join_date else '',
                c.leave_date.strftime('%d/%m/%Y') if c.leave_date else '',
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                payment_date.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',  # Basic Pay
                f'{c.pensionable_earnings:.2f}',  # Pensionable Earnings
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                '0.00',  # Bonus
                '0.00',  # Salary Sacrifice
                '0.00',  # AVCs
                '0.00',  # Employer AVCs
                'Standard',
                event
            ])

        return output.getvalue()
