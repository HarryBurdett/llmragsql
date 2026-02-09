"""
Aegon Pension Export

Generates Aegon Pre-update Contributions format CSV.
Based on Aegon SmartScheme file specification.

Reference: https://gb-kb.sage.com/portal/app/portlets/results/viewsolution.jsp?solutionid=210125193129290
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

    Aegon Pre-update Contributions format with 23 columns.
    NI numbers must be in format AA123456A.
    Salary and contribution fields must be numeric (no commas, % or £ signs).
    """

    PROVIDER_NAME = "Aegon"
    SCHEME_TYPES = [8]

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate Aegon Pre-update Contributions format CSV."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row - 23 columns as per specification
        writer.writerow([
            'Title',
            'Employee First Name',
            'Employee Surname',
            'Date of Birth',
            'National Insurance Number',
            'Gender',
            'Payroll Number',
            'Address 1',
            'Address 2',
            'Address 3',
            'Address 4',
            'County',
            'Post Code',
            'Country',
            'Email',
            'Employee Start Date',
            'Annual Pensionable Salary',
            'Current Pay Period Earnings',
            'Current Pay Period Pensionable Earnings (Tier 1 & 2)',
            'Current Pay Period All Earnings (Tier 3)',
            'Employee Contribution Percentage',
            'RegPctSalaryEmployee Contribution',
            'Employer Contribution Percentage',
            'RegPctSalaryEmployer Contribution',
            'Category Rule Field',
            'Contract Joiner'
        ])

        for c in contributions:
            # Calculate annual pensionable salary (monthly * 12)
            annual_salary = c.pensionable_earnings * 12

            # Format dates as DD/MM/YYYY
            dob_str = c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else ''
            start_str = c.start_date.strftime('%d/%m/%Y') if c.start_date else ''

            # Contract joiner flag
            contract_joiner = 'Y' if c.is_new_starter else 'N'

            writer.writerow([
                c.title,                               # Title
                c.forename,                            # Employee First Name
                c.surname,                             # Employee Surname
                dob_str,                               # Date of Birth
                c.ni_number,                           # National Insurance Number
                c.gender,                              # Gender
                c.employee_ref,                        # Payroll Number
                c.address_1[:24] if c.address_1 else '',  # Address 1 (max 24 chars)
                c.address_2[:24] if c.address_2 else '',  # Address 2
                c.address_3[:24] if c.address_3 else '',  # Address 3
                c.address_4[:24] if c.address_4 else '',  # Address 4
                '',                                    # County
                c.postcode,                            # Post Code
                'United Kingdom',                      # Country
                '',                                    # Email
                start_str,                             # Employee Start Date
                f'{annual_salary:.2f}',                # Annual Pensionable Salary
                f'{c.pensionable_earnings:.2f}',       # Current Pay Period Earnings
                f'{c.pensionable_earnings:.2f}',       # Current Pay Period Pensionable Earnings (Tier 1 & 2)
                '0.00',                                # Current Pay Period All Earnings (Tier 3)
                f'{c.employee_rate:.2f}' if c.employee_rate else '0',  # Employee Contribution Percentage
                f'{c.employee_contribution:.2f}',      # RegPctSalaryEmployee Contribution
                f'{c.employer_rate:.2f}' if c.employer_rate else '0',  # Employer Contribution Percentage
                f'{c.employer_contribution:.2f}',      # RegPctSalaryEmployer Contribution
                '',                                    # Category Rule Field
                contract_joiner                        # Contract Joiner
            ])

        return output.getvalue()
