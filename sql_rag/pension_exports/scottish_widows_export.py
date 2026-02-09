"""
Scottish Widows Pension Export

Generates Scottish Widows-format CSV files for contribution submissions.
Based on Scottish Widows Pre-update Export File specification.

Reference: https://gb-kb.sage.com/portal/app/portlets/results/view2.jsp?k2dockey=210209183123713
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

    Generates CSV in Scottish Widows Pre-update Export format.
    34 columns covering employee details, dates, earnings and contributions.
    """

    PROVIDER_NAME = "Scottish Widows"
    SCHEME_TYPES = [2]

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

        # Header row - 34 columns as per specification
        writer.writerow([
            'Employee ID',
            'Title',
            'First name',
            'Last name',
            'Birth date',
            'Gender',
            'NI number',
            'Nationality',
            'Address line 1',
            'Address line 2',
            'Address line 3',
            'Address line 4',
            'Postcode',
            'In scope of AE',
            'Telephone number',
            'Email address',
            'Contractual scheme join date',
            'Requested scheme join date',
            'Opt out date',
            'Member cessation date',
            'Death date',
            'Employment termination date',
            'Pension provider worker group',
            'Employment start date',
            'Pay group',
            'Pay date',
            'Total assessment earnings',
            'Total pensionable earnings',
            'Employer regular contribution percentage',
            'Employee regular contribution percentage',
            'Employer regular contribution amount',
            'Employee regular contribution amount',
            'Employer single contribution amount',
            'Employee single contribution amount'
        ])

        worker_group = scheme_config.get('wps_pengp', '').strip()
        pay_group = scheme_config.get('wps_paygrp', '').strip()

        for c in contributions:
            # Format dates as DD/MM/YYYY for Scottish Widows
            dob_str = c.date_of_birth.strftime('%d/%m/%Y') if c.date_of_birth else ''
            start_str = c.start_date.strftime('%d/%m/%Y') if c.start_date else ''
            join_str = c.scheme_join_date.strftime('%d/%m/%Y') if c.scheme_join_date else ''
            leave_str = c.leave_date.strftime('%d/%m/%Y') if c.leave_date else ''
            pay_date_str = payment_date.strftime('%d/%m/%Y')

            # Opt out / cessation dates
            opt_out_date = leave_str if c.opt_out else ''
            cessation_date = leave_str if c.is_leaver else ''

            writer.writerow([
                c.employee_ref,                    # Employee ID
                c.title,                           # Title
                c.forename,                        # First name
                c.surname,                         # Last name
                dob_str,                           # Birth date
                c.gender,                          # Gender (M/F)
                c.ni_number,                       # NI number
                'British',                         # Nationality
                c.address_1,                       # Address line 1
                c.address_2,                       # Address line 2
                c.address_3,                       # Address line 3
                c.address_4,                       # Address line 4
                c.postcode,                        # Postcode
                'Y',                               # In scope of AE
                '',                                # Telephone number
                '',                                # Email address
                join_str if not c.is_new_starter else '',  # Contractual scheme join date
                join_str if c.is_new_starter else '',      # Requested scheme join date
                opt_out_date,                      # Opt out date
                cessation_date,                    # Member cessation date
                '',                                # Death date
                leave_str if c.is_leaver else '',  # Employment termination date
                worker_group,                      # Pension provider worker group
                start_str,                         # Employment start date
                pay_group,                         # Pay group
                pay_date_str,                      # Pay date
                f'{c.pensionable_earnings:.2f}',   # Total assessment earnings
                f'{c.pensionable_earnings:.2f}',   # Total pensionable earnings
                f'{c.employer_rate:.2f}',          # Employer regular contribution percentage
                f'{c.employee_rate:.2f}',          # Employee regular contribution percentage
                f'{c.employer_contribution:.2f}',  # Employer regular contribution amount
                f'{c.employee_contribution:.2f}',  # Employee regular contribution amount
                '0.00',                            # Employer single contribution amount
                '0.00'                             # Employee single contribution amount
            ])

        return output.getvalue()
