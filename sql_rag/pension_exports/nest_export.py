"""
NEST Pension Export Utility

Generates NEST-format CSV files for pension contribution submissions.
Based on NEST CSV file specification for contribution schedules.

NEST CSV Format:
- Rows 1-4: Reserved (can be blank)
- Row 5: Header record
- Row 6+: Member contribution records

Header Fields:
- NEST Employer Reference (EMP + 9 digits)
- Process Type (CS = Contribution Schedule)
- Earnings Period End Date (YYYY-MM-DD)
- Payment Source Name
- Payment Due Date (YYYY-MM-DD)
- Frequency (Monthly/Weekly)
- Earnings Period Start Date (YYYY-MM-DD)
- Total Number of Member Records

Member Record Fields:
- NI Number
- Alternative ID (if no NI)
- Pensionable Earnings
- Employer Contribution
- Member Contribution
"""

import csv
import io
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal


@dataclass
class NestContribution:
    """Represents a single employee's NEST contribution record."""
    employee_ref: str
    ni_number: str
    forename: str
    surname: str
    pensionable_earnings: Decimal
    employer_contribution: Decimal
    employee_contribution: Decimal
    alternative_id: Optional[str] = None


@dataclass
class NestExportResult:
    """Result of a NEST export operation."""
    success: bool
    csv_content: str
    filename: str
    record_count: int
    total_employer_contributions: Decimal
    total_employee_contributions: Decimal
    total_pensionable_earnings: Decimal
    errors: List[str]
    warnings: List[str]


class NestExport:
    """
    NEST Pension Export Generator

    Extracts pension contribution data from Opera payroll and generates
    NEST-compatible CSV files for upload to the NEST pension portal.
    """

    PROVIDER_NAME = "NEST"
    SCHEME_TYPES = [11]  # wps_type = 11 for NEST
    SCHEME_CODE = 'AUOTENROL'  # NEST auto-enrolment scheme code in Opera

    def __init__(self, sql_connector):
        """
        Initialize NEST export utility.

        Args:
            sql_connector: SQLConnector instance for database access
        """
        self.sql_connector = sql_connector
        self._nest_config = None

    def get_nest_config(self) -> Dict:
        """Get NEST scheme configuration from wpnsc table."""
        if self._nest_config is None:
            sql = f"""
            SELECT * FROM wpnsc
            WHERE wps_code = '{self.SCHEME_CODE}'
            """
            result = self.sql_connector.execute_query(sql)
            if hasattr(result, 'to_dict'):
                result = result.to_dict('records')

            if result:
                self._nest_config = result[0]
            else:
                raise ValueError(f"NEST scheme '{self.SCHEME_CODE}' not found in wpnsc table")

        return self._nest_config

    def get_employer_reference(self) -> str:
        """Get NEST employer reference (EMP + 9 digits)."""
        config = self.get_nest_config()
        ref = config.get('wps_scref', '').strip()
        if not ref:
            raise ValueError("NEST employer reference (wps_scref) not configured")
        return ref

    def get_enrolled_employees(self) -> List[Dict]:
        """Get list of employees enrolled in NEST scheme."""
        sql = f"""
        SELECT
            e.wep_ref,
            e.wep_code,
            e.wep_erper,
            e.wep_eeper,
            e.wep_jndt,
            e.wep_lfdt,
            w.wn_ref,
            w.wn_surname,
            w.wn_forenam,
            w.wn_ninum,
            w.wn_birth,
            w.wn_addrs1,
            w.wn_addrs2,
            w.wn_addrs3,
            w.wn_pstcde,
            w.wn_gender
        FROM wepen e
        JOIN wname w ON e.wep_ref = w.wn_ref
        WHERE e.wep_code = '{self.SCHEME_CODE}'
          AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())
        ORDER BY w.wn_surname, w.wn_forenam
        """
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        return result or []

    def get_contributions_for_period(
        self,
        tax_year: str,
        period: int
    ) -> List[NestContribution]:
        """
        Get pension contributions for a specific pay period.

        Args:
            tax_year: Tax year in Opera format (e.g., '2526' for 2025/26)
            period: Pay period number (1-12 for monthly)

        Returns:
            List of NestContribution records
        """
        sql = f"""
        SELECT
            w.wn_ref,
            w.wn_surname,
            w.wn_forenam,
            w.wn_ninum,
            h.wh_pen AS employee_contribution,
            h.wh_penbl AS pensionable_earnings,
            e.wep_erper,
            e.wep_eeper
        FROM wname w
        INNER JOIN wepen e ON w.wn_ref = e.wep_ref
        INNER JOIN whist h ON w.wn_ref = h.wh_ref
        WHERE e.wep_code = '{self.SCHEME_CODE}'
          AND h.wh_year = '{tax_year}'
          AND h.wh_period = {period}
          AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())
        ORDER BY w.wn_surname, w.wn_forenam
        """

        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        contributions = []
        for row in result or []:
            pensionable = Decimal(str(row.get('pensionable_earnings') or 0))
            ee_contrib = Decimal(str(row.get('employee_contribution') or 0))

            # Calculate employer contribution from pensionable earnings and rate
            er_rate = Decimal(str(row.get('wep_erper') or 0)) / 100
            er_contrib = (pensionable * er_rate).quantize(Decimal('0.01'))

            contributions.append(NestContribution(
                employee_ref=row['wn_ref'].strip(),
                ni_number=row['wn_ninum'].strip() if row.get('wn_ninum') else '',
                forename=row['wn_forenam'].strip() if row.get('wn_forenam') else '',
                surname=row['wn_surname'].strip() if row.get('wn_surname') else '',
                pensionable_earnings=pensionable,
                employer_contribution=er_contrib,
                employee_contribution=ee_contrib,
                alternative_id=row['wn_ref'].strip() if not row.get('wn_ninum') else None
            ))

        return contributions

    def get_period_dates(self, tax_year: str, period: int) -> Tuple[date, date, date]:
        """
        Get period start, end, and pay dates from wclndr.

        Returns:
            Tuple of (start_date, end_date, pay_date)
        """
        sql = f"""
        SELECT wd_startdt, wd_enddt, wd_paydt, wd_actpydt
        FROM wclndr
        WHERE wd_taxyear = '{tax_year}'
          AND wd_period = {period}
          AND wd_payfrq = 'M'
        ORDER BY id DESC
        """
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if result:
            row = result[0]
            start_dt = row.get('wd_startdt')
            end_dt = row.get('wd_enddt')
            pay_dt = row.get('wd_actpydt') or row.get('wd_paydt')

            # Convert to date objects if needed
            if hasattr(start_dt, 'date'):
                start_dt = start_dt.date()
            if hasattr(end_dt, 'date'):
                end_dt = end_dt.date()
            if hasattr(pay_dt, 'date'):
                pay_dt = pay_dt.date()

            return (start_dt, end_dt, pay_dt)

        # Fallback: calculate dates from tax year and period
        # Tax year 2526 = starts 6 April 2025
        year_start = 2000 + int(tax_year[:2])
        month = ((period - 1) % 12) + 4  # Period 1 = April (month 4)
        if month > 12:
            month -= 12
            year_start += 1

        from calendar import monthrange
        start_dt = date(year_start, month, 1)
        _, last_day = monthrange(year_start, month)
        end_dt = date(year_start, month, last_day)
        pay_dt = end_dt

        return (start_dt, end_dt, pay_dt)

    def generate_csv(
        self,
        tax_year: str,
        period: int,
        payment_source: str = "Bank Account",
        frequency: str = "Monthly"
    ) -> NestExportResult:
        """
        Generate NEST contribution CSV file.

        Args:
            tax_year: Tax year in Opera format (e.g., '2526')
            period: Pay period number
            payment_source: Name of payment source (must match NEST account)
            frequency: Pay frequency ('Monthly' or 'Weekly')

        Returns:
            NestExportResult with CSV content and summary
        """
        errors = []
        warnings = []

        try:
            # Get NEST configuration
            employer_ref = self.get_employer_reference()

            # Get period dates
            start_date, end_date, pay_date = self.get_period_dates(tax_year, period)

            # Get contributions
            contributions = self.get_contributions_for_period(tax_year, period)

            if not contributions:
                return NestExportResult(
                    success=False,
                    csv_content='',
                    filename='',
                    record_count=0,
                    total_employer_contributions=Decimal('0'),
                    total_employee_contributions=Decimal('0'),
                    total_pensionable_earnings=Decimal('0'),
                    errors=['No contribution records found for this period'],
                    warnings=[]
                )

            # Validate NI numbers
            for c in contributions:
                if not c.ni_number:
                    warnings.append(f"Employee {c.employee_ref} ({c.forename} {c.surname}) has no NI number - using alternative ID")

            # Calculate totals
            total_er = sum(c.employer_contribution for c in contributions)
            total_ee = sum(c.employee_contribution for c in contributions)
            total_pensionable = sum(c.pensionable_earnings for c in contributions)

            # Generate CSV content
            output = io.StringIO()
            writer = csv.writer(output)

            # Rows 1-4: Reserved/blank
            writer.writerow([])
            writer.writerow([])
            writer.writerow([])
            writer.writerow([])

            # Row 5: Header record
            header = [
                employer_ref,                    # NEST Employer Reference
                'CS',                            # Process Type (Contribution Schedule)
                end_date.strftime('%Y-%m-%d'),   # Earnings Period End Date
                payment_source,                  # Payment Source Name
                pay_date.strftime('%Y-%m-%d'),   # Payment Due Date
                frequency,                       # Frequency
                start_date.strftime('%Y-%m-%d'), # Earnings Period Start Date
                len(contributions)               # Total Number of Member Records
            ]
            writer.writerow(header)

            # Row 6: Column headers for member records
            writer.writerow([
                'NI Number',
                'Alternative ID',
                'Pensionable Earnings',
                'Employer Contribution',
                'Employee Contribution'
            ])

            # Member records
            for c in contributions:
                writer.writerow([
                    c.ni_number or '',
                    c.alternative_id or '',
                    f'{c.pensionable_earnings:.2f}',
                    f'{c.employer_contribution:.2f}',
                    f'{c.employee_contribution:.2f}'
                ])

            csv_content = output.getvalue()

            # Generate filename
            filename = f"NEST_Contributions_{tax_year}_P{period:02d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            return NestExportResult(
                success=True,
                csv_content=csv_content,
                filename=filename,
                record_count=len(contributions),
                total_employer_contributions=total_er,
                total_employee_contributions=total_ee,
                total_pensionable_earnings=total_pensionable,
                errors=errors,
                warnings=warnings
            )

        except Exception as e:
            return NestExportResult(
                success=False,
                csv_content='',
                filename='',
                record_count=0,
                total_employer_contributions=Decimal('0'),
                total_employee_contributions=Decimal('0'),
                total_pensionable_earnings=Decimal('0'),
                errors=[str(e)],
                warnings=warnings
            )

    def export_to_file(
        self,
        tax_year: str,
        period: int,
        output_dir: str,
        payment_source: str = "Bank Account"
    ) -> NestExportResult:
        """
        Generate NEST CSV and save to file.

        Args:
            tax_year: Tax year in Opera format
            period: Pay period number
            output_dir: Directory to save the file
            payment_source: Name of payment source

        Returns:
            NestExportResult with file path in filename field
        """
        import os

        result = self.generate_csv(tax_year, period, payment_source)

        if result.success:
            filepath = os.path.join(output_dir, result.filename)
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                f.write(result.csv_content)

            # Update filename to full path
            result = NestExportResult(
                success=result.success,
                csv_content=result.csv_content,
                filename=filepath,
                record_count=result.record_count,
                total_employer_contributions=result.total_employer_contributions,
                total_employee_contributions=result.total_employee_contributions,
                total_pensionable_earnings=result.total_pensionable_earnings,
                errors=result.errors,
                warnings=result.warnings
            )

        return result

    def preview_export(self, tax_year: str, period: int) -> Dict:
        """
        Preview what would be exported without generating the file.

        Returns:
            Dictionary with preview information
        """
        contributions = self.get_contributions_for_period(tax_year, period)
        start_date, end_date, pay_date = self.get_period_dates(tax_year, period)

        return {
            'employer_reference': self.get_employer_reference(),
            'tax_year': tax_year,
            'period': period,
            'period_start': start_date.isoformat() if start_date else None,
            'period_end': end_date.isoformat() if end_date else None,
            'pay_date': pay_date.isoformat() if pay_date else None,
            'record_count': len(contributions),
            'total_employer_contributions': float(sum(c.employer_contribution for c in contributions)),
            'total_employee_contributions': float(sum(c.employee_contribution for c in contributions)),
            'total_pensionable_earnings': float(sum(c.pensionable_earnings for c in contributions)),
            'employees': [
                {
                    'ref': c.employee_ref,
                    'name': f"{c.forename} {c.surname}",
                    'ni_number': c.ni_number,
                    'pensionable_earnings': float(c.pensionable_earnings),
                    'employer_contribution': float(c.employer_contribution),
                    'employee_contribution': float(c.employee_contribution)
                }
                for c in contributions
            ]
        }
