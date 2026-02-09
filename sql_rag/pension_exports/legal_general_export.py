"""
Legal & General Pension Export

Generates Legal & General-format CSV files for contribution submissions.
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class LegalGeneralExport(BasePensionExport):
    """
    Legal & General Pension Export Generator

    Legal & General WorkSave pension contribution format.
    """

    PROVIDER_NAME = "Legal and General"
    SCHEME_TYPES = [7]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Legal & General-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Scheme ID',
            'Employer ID',
            'Payroll Reference',
            'NI Number',
            'Title',
            'Forenames',
            'Surname',
            'Date of Birth',
            'Gender',
            'Marital Status',
            'Address Line 1',
            'Address Line 2',
            'Address Line 3',
            'Address Line 4',
            'Post Code',
            'Country',
            'Email',
            'Phone',
            'Employment Start Date',
            'Scheme Entry Date',
            'Leaving Date',
            'Pay Frequency',
            'Period Start Date',
            'Period End Date',
            'Payment Due Date',
            'Pensionable Salary',
            'Basic Salary',
            'Employee Contribution Amount',
            'Employer Contribution Amount',
            'Employee Additional Contribution',
            'Employer Additional Contribution',
            'Employee Contribution Rate',
            'Employer Contribution Rate',
            'Salary Sacrifice Indicator',
            'Status',
            'Reason Code'
        ])

        scheme_id = scheme_config.get('wps_prref', '').strip()
        employer_id = scheme_config.get('wps_scref', '').strip()

        for c in contributions:
            # Status: A=Active, L=Leaver
            status = 'A'
            if c.is_leaver:
                status = 'L'

            # Reason codes
            reason = ''
            if c.is_new_starter:
                reason = 'NJ'  # New Joiner
            elif c.is_leaver:
                reason = 'LV'  # Leaver
            elif c.opt_out:
                reason = 'OO'  # Opt Out

            writer.writerow([
                scheme_id,
                employer_id,
                c.employee_ref,
                c.ni_number,
                c.title,
                c.forename,
                c.surname,
                c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else '',
                'M' if c.gender.upper() == 'M' else 'F',
                '',  # Marital Status
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                c.postcode,
                'UK',
                '',  # Email
                '',  # Phone
                c.start_date.strftime('%d/%m/%Y') if c.start_date else '',
                c.scheme_join_date.strftime('%d/%m/%Y') if c.scheme_join_date else '',
                c.leave_date.strftime('%d/%m/%Y') if c.leave_date else '',
                'Monthly',
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                payment_date.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',
                f'{c.pensionable_earnings:.2f}',  # Basic Salary
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                '0.00',  # Employee Additional
                '0.00',  # Employer Additional
                f'{c.employee_rate:.2f}',
                f'{c.employer_rate:.2f}',
                'N',  # Salary Sacrifice Indicator
                status,
                reason
            ])

        return output.getvalue()
