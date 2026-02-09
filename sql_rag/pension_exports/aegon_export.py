"""
Aegon Pension Export

Generates Aegon-format CSV files for contribution submissions.
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class AegonExport(BasePensionExport):
    """
    Aegon Pension Export Generator

    Aegon workplace pension contribution file format.
    """

    PROVIDER_NAME = "Aegon"
    SCHEME_TYPES = [8]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Aegon-format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            'Employer Scheme Reference',
            'Employer Name',
            'Employee Payroll Reference',
            'National Insurance Number',
            'Title',
            'First Name',
            'Middle Names',
            'Surname',
            'Date of Birth',
            'Gender',
            'Address Line 1',
            'Address Line 2',
            'Address Line 3',
            'Address Line 4',
            'Address Line 5',
            'Postcode',
            'Country',
            'Email Address',
            'Mobile Number',
            'Work Phone',
            'Date Joined Employer',
            'Date Joined Scheme',
            'Date Left Scheme',
            'Contribution Period Start',
            'Contribution Period End',
            'Expected Payment Date',
            'Pensionable Earnings',
            'Employee Core Contribution',
            'Employer Core Contribution',
            'Employee Voluntary Contribution',
            'Employer Matching Contribution',
            'Salary Sacrifice Amount',
            'Employee Percentage',
            'Employer Percentage',
            'Pay Frequency',
            'Employment Status',
            'Transaction Type',
            'Member Category'
        ])

        scheme_ref = scheme_config.get('wps_scref', '').strip()
        employer_name = scheme_config.get('wps_prname', '').strip()

        for c in contributions:
            # Employment status
            emp_status = 'Active'
            if c.is_leaver:
                emp_status = 'Left'

            # Transaction type
            trans_type = 'Regular'
            if c.is_new_starter:
                trans_type = 'New Member'
            elif c.is_leaver:
                trans_type = 'Final'

            writer.writerow([
                scheme_ref,
                employer_name,
                c.employee_ref,
                c.ni_number,
                c.title,
                c.forename,
                '',  # Middle Names
                c.surname,
                c.date_of_birth.strftime('%dd/%m/%Y') if c.date_of_birth else '',
                'Male' if c.gender.upper() == 'M' else 'Female',
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                '',  # Address Line 5
                c.postcode,
                'United Kingdom',
                '',  # Email
                '',  # Mobile
                '',  # Work Phone
                c.start_date.strftime('%d/%m/%Y') if c.start_date else '',
                c.scheme_join_date.strftime('%d/%m/%Y') if c.scheme_join_date else '',
                c.leave_date.strftime('%d/%m/%Y') if c.leave_date else '',
                period_start.strftime('%d/%m/%Y'),
                period_end.strftime('%d/%m/%Y'),
                payment_date.strftime('%d/%m/%Y'),
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                '0.00',  # Employee Voluntary
                '0.00',  # Employer Matching
                '0.00',  # Salary Sacrifice
                f'{c.employee_rate:.2f}',
                f'{c.employer_rate:.2f}',
                'Monthly',
                emp_status,
                trans_type,
                'Standard'
            ])

        return output.getvalue()
