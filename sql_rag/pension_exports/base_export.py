"""
Base Pension Export Class

Provides common functionality for all pension provider exports.
Each provider-specific class inherits from this and implements
the format-specific CSV generation.
"""

import csv
import io
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class PensionContribution:
    """Universal pension contribution record."""
    # Employee identification
    employee_ref: str
    ni_number: str
    title: str
    forename: str
    surname: str
    date_of_birth: Optional[date]
    gender: str  # 'M' or 'F'

    # Address
    address_1: str
    address_2: str
    address_3: str
    address_4: str
    postcode: str

    # Employment
    start_date: Optional[date]
    leave_date: Optional[date]
    payroll_group: str

    # Contribution data
    pensionable_earnings: Decimal
    employer_contribution: Decimal
    employee_contribution: Decimal
    employer_rate: Decimal
    employee_rate: Decimal

    # Scheme info
    scheme_code: str
    scheme_join_date: Optional[date]

    # Status flags
    is_new_starter: bool = False
    is_leaver: bool = False
    opt_out: bool = False

    # Alternative ID if no NI number
    alternative_id: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{self.forename} {self.surname}".strip()

    @property
    def total_contribution(self) -> Decimal:
        return self.employer_contribution + self.employee_contribution


@dataclass
class ExportResult:
    """Result of a pension export operation."""
    success: bool
    provider_name: str
    content: str  # CSV or XML content
    filename: str
    record_count: int
    total_employer_contributions: Decimal
    total_employee_contributions: Decimal
    total_pensionable_earnings: Decimal
    new_starters: int
    leavers: int
    current_employees: int
    content_type: str = "text/csv"  # MIME type
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class BasePensionExport(ABC):
    """
    Abstract base class for pension provider exports.

    Each provider implementation must override:
    - PROVIDER_NAME: Display name of the provider
    - SCHEME_TYPES: List of wpnsc.wps_type values for this provider
    - generate_csv_content(): Provider-specific CSV format
    """

    PROVIDER_NAME: str = "Unknown"
    SCHEME_TYPES: List[int] = []  # wps_type values that match this provider

    def __init__(self, sql_connector, scheme_code: str = None):
        """Initialize with database connector and optional scheme code."""
        self.sql_connector = sql_connector
        self.scheme_code = scheme_code

    def get_schemes_for_provider(self) -> List[Dict]:
        """Get pension schemes configured for this provider."""
        if not self.SCHEME_TYPES:
            return []

        types_str = ','.join(str(t) for t in self.SCHEME_TYPES)
        sql = f"""
        SELECT * FROM wpnsc
        WHERE wps_type IN ({types_str})
        """
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        return result or []

    def get_payroll_groups(self) -> List[Dict]:
        """Get all payroll groups."""
        sql = "SELECT wg_group, wg_name FROM wgrup ORDER BY wg_name"
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        return result or []

    def get_contributions(
        self,
        scheme_code: str,
        tax_year: str,
        period: int,
        groups: Optional[List[str]] = None
    ) -> List[PensionContribution]:
        """
        Get pension contributions for a specific period.

        Args:
            scheme_code: Pension scheme code from wpnsc
            tax_year: Tax year (e.g., '2526')
            period: Pay period number
            groups: Optional list of group codes to filter by

        Returns:
            List of PensionContribution records
        """
        group_filter = ""
        if groups:
            groups_str = "','".join(groups)
            group_filter = f"AND w.wn_group IN ('{groups_str}')"

        sql = f"""
        SELECT
            w.wn_ref,
            w.wn_ninum,
            w.wn_title,
            w.wn_forenam,
            w.wn_surname,
            w.wn_birth,
            w.wn_gender,
            w.wn_addrs1,
            w.wn_addrs2,
            w.wn_addrs3,
            w.wn_addrs4,
            w.wn_pstcde,
            w.wn_startdt,
            w.wn_leavdt,
            w.wn_group,
            h.wh_pen AS employee_contribution,
            h.wh_penbl AS pensionable_earnings,
            h.wh_paydt,
            e.wep_erper,
            e.wep_eeper,
            e.wep_jndt,
            e.wep_code
        FROM wname w
        INNER JOIN wepen e ON w.wn_ref = e.wep_ref
        INNER JOIN whist h ON w.wn_ref = h.wh_ref
        WHERE e.wep_code = '{scheme_code}'
          AND h.wh_year = '{tax_year}'
          AND h.wh_period = {period}
          AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())
          {group_filter}
        ORDER BY w.wn_surname, w.wn_forenam
        """

        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Get period dates for new starter/leaver detection
        period_start, period_end, _ = self.get_period_dates(tax_year, period)

        contributions = []
        for row in result or []:
            pensionable = Decimal(str(row.get('pensionable_earnings') or 0))
            ee_contrib = Decimal(str(row.get('employee_contribution') or 0))
            er_rate = Decimal(str(row.get('wep_erper') or 0))
            ee_rate = Decimal(str(row.get('wep_eeper') or 0))

            # Calculate employer contribution
            er_contrib = (pensionable * er_rate / 100).quantize(Decimal('0.01'))

            # Detect new starters and leavers
            start_dt = row.get('wn_startdt')
            leave_dt = row.get('wn_leavdt')

            is_new = False
            is_leaver = False

            if start_dt:
                if hasattr(start_dt, 'date'):
                    start_dt = start_dt.date()
                if period_start and period_end:
                    is_new = period_start <= start_dt <= period_end

            if leave_dt:
                if hasattr(leave_dt, 'date'):
                    leave_dt = leave_dt.date()
                if period_start and period_end:
                    is_leaver = period_start <= leave_dt <= period_end

            dob = row.get('wn_birth')
            if dob and hasattr(dob, 'date'):
                dob = dob.date()

            join_dt = row.get('wep_jndt')
            if join_dt and hasattr(join_dt, 'date'):
                join_dt = join_dt.date()

            contributions.append(PensionContribution(
                employee_ref=row['wn_ref'].strip() if row.get('wn_ref') else '',
                ni_number=row['wn_ninum'].strip() if row.get('wn_ninum') else '',
                title=row['wn_title'].strip() if row.get('wn_title') else '',
                forename=row['wn_forenam'].strip() if row.get('wn_forenam') else '',
                surname=row['wn_surname'].strip() if row.get('wn_surname') else '',
                date_of_birth=dob,
                gender=row['wn_gender'].strip() if row.get('wn_gender') else '',
                address_1=row['wn_addrs1'].strip() if row.get('wn_addrs1') else '',
                address_2=row['wn_addrs2'].strip() if row.get('wn_addrs2') else '',
                address_3=row['wn_addrs3'].strip() if row.get('wn_addrs3') else '',
                address_4=row['wn_addrs4'].strip() if row.get('wn_addrs4') else '',
                postcode=row['wn_pstcde'].strip() if row.get('wn_pstcde') else '',
                start_date=start_dt if isinstance(start_dt, date) else None,
                leave_date=leave_dt if isinstance(leave_dt, date) else None,
                payroll_group=row['wn_group'].strip() if row.get('wn_group') else '',
                pensionable_earnings=pensionable,
                employer_contribution=er_contrib,
                employee_contribution=ee_contrib,
                employer_rate=er_rate,
                employee_rate=ee_rate,
                scheme_code=row['wep_code'].strip() if row.get('wep_code') else '',
                scheme_join_date=join_dt,
                is_new_starter=is_new,
                is_leaver=is_leaver,
                alternative_id=row['wn_ref'].strip() if not row.get('wn_ninum') else None
            ))

        return contributions

    def get_period_dates(self, tax_year: str, period: int) -> Tuple[date, date, date]:
        """Get period start, end, and pay dates."""
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

            if hasattr(start_dt, 'date'):
                start_dt = start_dt.date()
            if hasattr(end_dt, 'date'):
                end_dt = end_dt.date()
            if hasattr(pay_dt, 'date'):
                pay_dt = pay_dt.date()

            return (start_dt, end_dt, pay_dt)

        # Fallback calculation
        year_start = 2000 + int(tax_year[:2])
        month = ((period - 1) % 12) + 4
        if month > 12:
            month -= 12
            year_start += 1

        from calendar import monthrange
        start_dt = date(year_start, month, 1)
        _, last_day = monthrange(year_start, month)
        end_dt = date(year_start, month, last_day)

        return (start_dt, end_dt, end_dt)

    def get_scheme_config(self, scheme_code: str) -> Dict:
        """Get scheme configuration from wpnsc."""
        sql = f"SELECT * FROM wpnsc WHERE wps_code = '{scheme_code}'"
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        return result[0] if result else {}

    @abstractmethod
    def generate_csv_content(
        self,
        contributions: List[PensionContribution],
        scheme_config: Dict,
        period_start: date,
        period_end: date,
        payment_date: date
    ) -> str:
        """
        Generate provider-specific CSV content.

        Must be implemented by each provider class.
        """
        pass

    def generate_export(
        self,
        tax_year: str,
        period: int,
        payment_source: str = "Bank Account",
        group_codes: Optional[List[str]] = None,
        employee_refs: Optional[List[str]] = None
    ) -> ExportResult:
        """
        Generate pension export for the specified period.

        Args:
            tax_year: Tax year (e.g., '2526')
            period: Pay period number
            payment_source: Payment source name
            group_codes: Optional list of group codes to filter by
            employee_refs: Optional list of specific employee refs to include

        Returns:
            ExportResult with content and summary
        """
        errors = []
        warnings = []

        # Use instance scheme_code or raise error
        scheme_code = self.scheme_code
        if not scheme_code:
            return ExportResult(
                success=False,
                provider_name=self.PROVIDER_NAME,
                content='',
                filename='',
                record_count=0,
                total_employer_contributions=Decimal('0'),
                total_employee_contributions=Decimal('0'),
                total_pensionable_earnings=Decimal('0'),
                new_starters=0,
                leavers=0,
                current_employees=0,
                errors=["Scheme code not specified"]
            )

        try:
            # Get scheme config
            scheme_config = self.get_scheme_config(scheme_code)
            if not scheme_config:
                return ExportResult(
                    success=False,
                    provider_name=self.PROVIDER_NAME,
                    content='',
                    filename='',
                    record_count=0,
                    total_employer_contributions=Decimal('0'),
                    total_employee_contributions=Decimal('0'),
                    total_pensionable_earnings=Decimal('0'),
                    new_starters=0,
                    leavers=0,
                    current_employees=0,
                    errors=[f"Scheme '{scheme_code}' not found"]
                )

            # Get period dates
            period_start, period_end, payment_date = self.get_period_dates(tax_year, period)

            # Get contributions
            contributions = self.get_contributions(scheme_code, tax_year, period, group_codes)

            # Filter by specific employees if provided
            if employee_refs:
                contributions = [c for c in contributions if c.employee_ref in employee_refs]

            if not contributions:
                return ExportResult(
                    success=False,
                    provider_name=self.PROVIDER_NAME,
                    content='',
                    filename='',
                    record_count=0,
                    total_employer_contributions=Decimal('0'),
                    total_employee_contributions=Decimal('0'),
                    total_pensionable_earnings=Decimal('0'),
                    new_starters=0,
                    leavers=0,
                    current_employees=0,
                    errors=['No contribution records found for this period']
                )

            # Validate data
            for c in contributions:
                if not c.ni_number:
                    warnings.append(f"Employee {c.employee_ref} ({c.full_name}) has no NI number")

            # Generate content (CSV or XML depending on provider)
            content = self.generate_csv_content(
                contributions, scheme_config, period_start, period_end, payment_date
            )

            # Calculate totals
            total_er = sum(c.employer_contribution for c in contributions)
            total_ee = sum(c.employee_contribution for c in contributions)
            total_pensionable = sum(c.pensionable_earnings for c in contributions)

            new_starters = sum(1 for c in contributions if c.is_new_starter)
            leavers = sum(1 for c in contributions if c.is_leaver)
            current = len(contributions) - new_starters - leavers

            # Generate filename
            provider_short = self.PROVIDER_NAME.replace(' ', '_').replace('&', 'and')
            filename = f"{provider_short}_{scheme_code}_{tax_year}_P{period:02d}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            return ExportResult(
                success=True,
                provider_name=self.PROVIDER_NAME,
                content=content,
                filename=filename,
                record_count=len(contributions),
                total_employer_contributions=total_er,
                total_employee_contributions=total_ee,
                total_pensionable_earnings=total_pensionable,
                new_starters=new_starters,
                leavers=leavers,
                current_employees=current,
                errors=errors,
                warnings=warnings
            )

        except Exception as e:
            return ExportResult(
                success=False,
                provider_name=self.PROVIDER_NAME,
                content='',
                filename='',
                record_count=0,
                total_employer_contributions=Decimal('0'),
                total_employee_contributions=Decimal('0'),
                total_pensionable_earnings=Decimal('0'),
                new_starters=0,
                leavers=0,
                current_employees=0,
                errors=[str(e)],
                warnings=warnings
            )

    def preview_export(
        self,
        tax_year: str,
        period: int,
        group_codes: Optional[List[str]] = None
    ) -> Dict:
        """Preview export without generating file."""
        scheme_code = self.scheme_code
        contributions = self.get_contributions(scheme_code, tax_year, period, group_codes)
        period_start, period_end, payment_date = self.get_period_dates(tax_year, period)
        scheme_config = self.get_scheme_config(scheme_code)

        new_starters = sum(1 for c in contributions if c.is_new_starter)
        leavers = sum(1 for c in contributions if c.is_leaver)

        return {
            'provider_name': self.PROVIDER_NAME,
            'scheme_code': scheme_code,
            'scheme_description': scheme_config.get('wps_desc', '').strip() if scheme_config else '',
            'scheme_reference': scheme_config.get('wps_scref', '').strip() if scheme_config else '',
            'tax_year': tax_year,
            'period': period,
            'period_start': period_start.isoformat() if period_start else None,
            'period_end': period_end.isoformat() if period_end else None,
            'payment_date': payment_date.isoformat() if payment_date else None,
            'record_count': len(contributions),
            'new_starters': new_starters,
            'leavers': leavers,
            'current_employees': len(contributions) - new_starters - leavers,
            'total_employer_contributions': float(sum(c.employer_contribution for c in contributions)),
            'total_employee_contributions': float(sum(c.employee_contribution for c in contributions)),
            'total_pensionable_earnings': float(sum(c.pensionable_earnings for c in contributions)),
            'employees': [
                {
                    'ref': c.employee_ref,
                    'name': c.full_name,
                    'ni_number': c.ni_number,
                    'group': c.payroll_group,
                    'pensionable_earnings': float(c.pensionable_earnings),
                    'employer_contribution': float(c.employer_contribution),
                    'employee_contribution': float(c.employee_contribution),
                    'is_new_starter': c.is_new_starter,
                    'is_leaver': c.is_leaver
                }
                for c in contributions
            ]
        }
