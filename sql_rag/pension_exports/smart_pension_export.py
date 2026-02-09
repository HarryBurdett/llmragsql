"""
Smart Pension Export (PAPDIS Format)

Generates PAPDIS-compliant CSV files for Smart Pension submissions.
PAPDIS = Payroll and Pension Data Interface Standard
"""

import csv
import io
from datetime import date
from typing import List, Dict
from decimal import Decimal

from .base_export import BasePensionExport, PensionContribution


class SmartPensionExport(BasePensionExport):
    """
    Smart Pension Export Generator (PAPDIS Format)

    PAPDIS is an open standard supported by Smart Pension and others.
    """

    PROVIDER_NAME = "Smart Pension"
    SCHEME_TYPES = [3]  # Assign appropriate wps_type

    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """Generate PAPDIS-format CSV for Smart Pension."""
        output = io.StringIO()
        writer = csv.writer(output)

        # PAPDIS Header row
        writer.writerow([
            'EmployerPensionReference',
            'EmployeePensionReference',
            'NationalInsuranceNumber',
            'AlternativeIdentifier',
            'Title',
            'Forename1',
            'Forename2',
            'Surname',
            'Gender',
            'DateOfBirth',
            'AddressLine1',
            'AddressLine2',
            'AddressLine3',
            'AddressLine4',
            'AddressLine5',
            'PostCode',
            'Country',
            'EmailAddress',
            'StartDate',
            'LeavingDate',
            'PensionableEarnings',
            'EmployeeContributionAmount',
            'EmployerContributionAmount',
            'EmployeeContributionPercent',
            'EmployerContributionPercent',
            'AVCAmount',
            'EarningsPeriodStartDate',
            'EarningsPeriodEndDate',
            'PaymentDueDate',
            'PayFrequency',
            'AssessmentCode',
            'EnrolmentType',
            'GroupName',
            'GroupPensionSection'
        ])

        employer_ref = scheme_config.get('wps_scref', '').strip()
        group_name = scheme_config.get('wps_pengp', '').strip()
        section = scheme_config.get('wps_penps', '').strip()

        for c in contributions:
            # PAPDIS Assessment codes
            assessment = 'EE'  # Eligible Jobholder - Enrolled
            if c.is_new_starter:
                assessment = 'NE'  # Newly Eligible
            elif c.is_leaver:
                assessment = 'LE'  # Leaver
            elif c.opt_out:
                assessment = 'OO'  # Opted Out

            # Enrolment type
            enrolment = 'AE'  # Auto Enrolment
            if c.opt_out:
                enrolment = 'OP'  # Opt Out

            writer.writerow([
                employer_ref,
                c.employee_ref,
                c.ni_number,
                c.alternative_id or '',
                c.title,
                c.forename,
                '',  # Forename2
                c.surname,
                c.gender,
                c.date_of_birth.strftime('%Y-%m-%d') if c.date_of_birth else '',
                c.address_1,
                c.address_2,
                c.address_3,
                c.address_4,
                '',  # Address Line 5
                c.postcode,
                'GB',  # Country
                '',  # Email
                c.start_date.strftime('%Y-%m-%d') if c.start_date else '',
                c.leave_date.strftime('%Y-%m-%d') if c.leave_date else '',
                f'{c.pensionable_earnings:.2f}',
                f'{c.employee_contribution:.2f}',
                f'{c.employer_contribution:.2f}',
                f'{c.employee_rate:.4f}',
                f'{c.employer_rate:.4f}',
                '0.00',  # AVC Amount
                period_start.strftime('%Y-%m-%d'),
                period_end.strftime('%Y-%m-%d'),
                payment_date.strftime('%Y-%m-%d'),
                'M',  # Monthly
                assessment,
                enrolment,
                group_name,
                section
            ])

        return output.getvalue()
