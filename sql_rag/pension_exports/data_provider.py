"""
Pension Data Provider

Abstracts data access for pension exports, supporting both Opera SQL SE and Opera 3.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class EmployeeGroup:
    """Employee group data."""
    code: str
    name: str
    employee_count: int = 0


@dataclass
class PensionScheme:
    """Pension scheme configuration."""
    code: str
    description: str
    provider_name: str
    provider_reference: str
    scheme_reference: str
    employer_rate: Decimal
    employee_rate: Decimal
    auto_enrolment: bool
    scheme_type: int
    enrolled_count: int = 0


@dataclass
class PayrollPeriod:
    """Payroll period data."""
    tax_year: str
    period: int
    pay_date: Optional[date] = None
    employee_count: int = 0


@dataclass
class EmployeeContribution:
    """Employee contribution data for a period."""
    employee_ref: str
    surname: str
    forename: str
    ni_number: str
    group: str
    date_of_birth: Optional[date]
    gender: str
    address_1: str
    address_2: str
    address_3: str
    address_4: str
    postcode: str
    title: str
    start_date: Optional[date]
    scheme_join_date: Optional[date]
    leave_date: Optional[date]
    pensionable_earnings: Decimal
    employee_contribution: Decimal
    employer_contribution: Decimal
    employee_rate: Decimal
    employer_rate: Decimal
    is_new_starter: bool = False
    is_leaver: bool = False


class BasePensionDataProvider(ABC):
    """Abstract base class for pension data providers."""

    @abstractmethod
    def get_employee_groups(self) -> List[EmployeeGroup]:
        """Get all employee groups."""
        pass

    @abstractmethod
    def get_pension_schemes(self) -> List[PensionScheme]:
        """Get all pension schemes."""
        pass

    @abstractmethod
    def get_payroll_periods(self, tax_year: Optional[str] = None) -> Dict[str, Any]:
        """Get payroll periods."""
        pass

    @abstractmethod
    def get_contributions(
        self,
        scheme_code: str,
        tax_year: str,
        period: int,
        group_codes: Optional[List[str]] = None
    ) -> List[EmployeeContribution]:
        """Get contributions for a scheme and period."""
        pass


class OperaSQLPensionProvider(BasePensionDataProvider):
    """Pension data provider for Opera SQL SE."""

    def __init__(self, sql_connector):
        self.sql_connector = sql_connector

    def get_employee_groups(self) -> List[EmployeeGroup]:
        """Get all employee groups from Opera SQL SE."""
        sql = """
        SELECT
            g.wg_group,
            g.wg_name,
            COUNT(DISTINCT w.wn_ref) as employee_count
        FROM wgrup g
        LEFT JOIN wname w ON w.wn_group = g.wg_group AND w.wn_leavdt IS NULL
        GROUP BY g.wg_group, g.wg_name
        ORDER BY g.wg_name
        """
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        groups = []
        for row in result or []:
            groups.append(EmployeeGroup(
                code=row['wg_group'].strip() if row.get('wg_group') else '',
                name=row['wg_name'].strip() if row.get('wg_name') else '',
                employee_count=int(row.get('employee_count') or 0)
            ))
        return groups

    def get_pension_schemes(self) -> List[PensionScheme]:
        """Get all pension schemes from Opera SQL SE."""
        sql = """
        SELECT
            s.wps_code,
            s.wps_desc,
            s.wps_prname,
            s.wps_prref,
            s.wps_scref,
            s.wps_erper,
            s.wps_eeper,
            s.wps_type,
            (SELECT COUNT(*) FROM wepen e WHERE e.wep_code = s.wps_code
             AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())) as enrolled_count
        FROM wpnsc s
        WHERE s.wps_code IS NOT NULL AND s.wps_code != ''
        ORDER BY s.wps_desc
        """
        result = self.sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        schemes = []
        for row in result or []:
            schemes.append(PensionScheme(
                code=row['wps_code'].strip() if row.get('wps_code') else '',
                description=row['wps_desc'].strip() if row.get('wps_desc') else '',
                provider_name=row['wps_prname'].strip() if row.get('wps_prname') else '',
                provider_reference=row['wps_prref'].strip() if row.get('wps_prref') else '',
                scheme_reference=row['wps_scref'].strip() if row.get('wps_scref') else '',
                employer_rate=Decimal(str(row.get('wps_erper') or 0)),
                employee_rate=Decimal(str(row.get('wps_eeper') or 0)),
                auto_enrolment=row.get('wps_type') == 11,
                scheme_type=int(row.get('wps_type') or 0),
                enrolled_count=int(row.get('enrolled_count') or 0)
            ))
        return schemes

    def get_payroll_periods(self, tax_year: Optional[str] = None) -> Dict[str, Any]:
        """Get payroll periods from Opera SQL SE."""
        # Get available tax years
        years_sql = """
        SELECT DISTINCT wh_year
        FROM whist
        WHERE wh_year IS NOT NULL
        ORDER BY wh_year DESC
        """
        years_result = self.sql_connector.execute_query(years_sql)
        if hasattr(years_result, 'to_dict'):
            years_result = years_result.to_dict('records')

        tax_years = [r['wh_year'].strip() for r in years_result if r.get('wh_year')]

        # Use provided tax year or most recent
        current_year = tax_year if tax_year else (tax_years[0] if tax_years else '')

        if not current_year:
            return {'tax_year': '', 'tax_years': [], 'periods': []}

        # Get periods for the tax year
        periods_sql = f"""
        SELECT DISTINCT
            wh_period,
            wh_paydt,
            COUNT(DISTINCT wh_ref) as employee_count
        FROM whist
        WHERE wh_year = '{current_year}'
        GROUP BY wh_period, wh_paydt
        ORDER BY wh_period DESC
        """
        periods_result = self.sql_connector.execute_query(periods_sql)
        if hasattr(periods_result, 'to_dict'):
            periods_result = periods_result.to_dict('records')

        periods = []
        for row in periods_result or []:
            pay_date = row.get('wh_paydt')
            if pay_date and hasattr(pay_date, 'isoformat'):
                pay_date = pay_date.isoformat()
            periods.append(PayrollPeriod(
                tax_year=current_year,
                period=int(row.get('wh_period') or 0),
                pay_date=pay_date,
                employee_count=int(row.get('employee_count') or 0)
            ))

        return {
            'tax_year': current_year,
            'tax_years': tax_years,
            'periods': [{'period': p.period, 'pay_date': p.pay_date, 'employee_count': p.employee_count} for p in periods]
        }

    def get_contributions(
        self,
        scheme_code: str,
        tax_year: str,
        period: int,
        group_codes: Optional[List[str]] = None
    ) -> List[EmployeeContribution]:
        """Get contributions for a scheme and period from Opera SQL SE."""
        group_filter = ""
        if group_codes:
            codes = [f"'{c.strip()}'" for c in group_codes]
            group_filter = f"AND w.wn_group IN ({','.join(codes)})"

        sql = f"""
        SELECT
            w.wn_ref,
            w.wn_surname,
            w.wn_forenam,
            w.wn_ninum,
            w.wn_group,
            w.wn_birth,
            w.wn_sex,
            w.wn_addrs1,
            w.wn_addrs2,
            w.wn_addrs3,
            w.wn_addrs4,
            w.wn_pstcde,
            w.wn_title,
            w.wn_startdt,
            h.wh_pen AS employee_contribution,
            h.wh_penbl AS pensionable_earnings,
            e.wep_erper,
            e.wep_eeper,
            e.wep_jndt,
            e.wep_lfdt
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

        contributions = []
        for row in result or []:
            pensionable = Decimal(str(row.get('pensionable_earnings') or 0))
            ee_contrib = Decimal(str(row.get('employee_contribution') or 0))
            er_rate = Decimal(str(row.get('wep_erper') or 0)) / 100
            er_contrib = (pensionable * er_rate).quantize(Decimal('0.01'))

            contributions.append(EmployeeContribution(
                employee_ref=row['wn_ref'].strip() if row.get('wn_ref') else '',
                surname=row['wn_surname'].strip() if row.get('wn_surname') else '',
                forename=row['wn_forenam'].strip() if row.get('wn_forenam') else '',
                ni_number=row['wn_ninum'].strip() if row.get('wn_ninum') else '',
                group=row['wn_group'].strip() if row.get('wn_group') else '',
                date_of_birth=row['wn_birth'].date() if row.get('wn_birth') and hasattr(row['wn_birth'], 'date') else row.get('wn_birth'),
                gender=row['wn_sex'].strip() if row.get('wn_sex') else '',
                address_1=row['wn_addrs1'].strip() if row.get('wn_addrs1') else '',
                address_2=row['wn_addrs2'].strip() if row.get('wn_addrs2') else '',
                address_3=row['wn_addrs3'].strip() if row.get('wn_addrs3') else '',
                address_4=row['wn_addrs4'].strip() if row.get('wn_addrs4') else '' if row.get('wn_addrs4') else '',
                postcode=row['wn_pstcde'].strip() if row.get('wn_pstcde') else '',
                title=row['wn_title'].strip() if row.get('wn_title') else '',
                start_date=row['wn_startdt'].date() if row.get('wn_startdt') and hasattr(row['wn_startdt'], 'date') else row.get('wn_startdt'),
                scheme_join_date=row['wep_jndt'].date() if row.get('wep_jndt') and hasattr(row['wep_jndt'], 'date') else row.get('wep_jndt'),
                leave_date=row['wep_lfdt'].date() if row.get('wep_lfdt') and hasattr(row['wep_lfdt'], 'date') else row.get('wep_lfdt'),
                pensionable_earnings=pensionable,
                employee_contribution=ee_contrib,
                employer_contribution=er_contrib,
                employee_rate=Decimal(str(row.get('wep_eeper') or 0)),
                employer_rate=Decimal(str(row.get('wep_erper') or 0)),
                is_new_starter=False,
                is_leaver=row.get('wep_lfdt') is not None
            ))

        return contributions


class Opera3PensionProvider(BasePensionDataProvider):
    """Pension data provider for Opera 3 (FoxPro DBF)."""

    def __init__(self, opera3_reader):
        self.reader = opera3_reader

    def get_employee_groups(self) -> List[EmployeeGroup]:
        """Get all employee groups from Opera 3."""
        try:
            # Read groups from wgrup table
            groups_data = self.reader.read_table('wgrup')
            employees_data = self.reader.read_table('wname')

            # Count employees per group
            group_counts = {}
            for emp in employees_data:
                if emp.get('wn_leavdt') is None:  # Active employees only
                    grp = (emp.get('wn_group') or '').strip()
                    group_counts[grp] = group_counts.get(grp, 0) + 1

            groups = []
            for row in groups_data:
                code = (row.get('wg_group') or '').strip()
                groups.append(EmployeeGroup(
                    code=code,
                    name=(row.get('wg_name') or '').strip(),
                    employee_count=group_counts.get(code, 0)
                ))

            return sorted(groups, key=lambda g: g.name)

        except Exception as e:
            logger.error(f"Error reading employee groups from Opera 3: {e}")
            return []

    def get_pension_schemes(self) -> List[PensionScheme]:
        """Get all pension schemes from Opera 3."""
        try:
            schemes_data = self.reader.read_table('wpnsc')
            enrolments_data = self.reader.read_table('wepen')

            # Count enrolled employees per scheme
            scheme_counts = {}
            today = date.today()
            for enrol in enrolments_data:
                leave_dt = enrol.get('wep_lfdt')
                if leave_dt is None or (isinstance(leave_dt, date) and leave_dt > today):
                    code = (enrol.get('wep_code') or '').strip()
                    scheme_counts[code] = scheme_counts.get(code, 0) + 1

            schemes = []
            for row in schemes_data:
                code = (row.get('wps_code') or '').strip()
                if not code:
                    continue

                schemes.append(PensionScheme(
                    code=code,
                    description=(row.get('wps_desc') or '').strip(),
                    provider_name=(row.get('wps_prname') or '').strip(),
                    provider_reference=(row.get('wps_prref') or '').strip(),
                    scheme_reference=(row.get('wps_scref') or '').strip(),
                    employer_rate=Decimal(str(row.get('wps_erper') or 0)),
                    employee_rate=Decimal(str(row.get('wps_eeper') or 0)),
                    auto_enrolment=row.get('wps_type') == 11,
                    scheme_type=int(row.get('wps_type') or 0),
                    enrolled_count=scheme_counts.get(code, 0)
                ))

            return sorted(schemes, key=lambda s: s.description)

        except Exception as e:
            logger.error(f"Error reading pension schemes from Opera 3: {e}")
            return []

    def get_payroll_periods(self, tax_year: Optional[str] = None) -> Dict[str, Any]:
        """Get payroll periods from Opera 3."""
        try:
            history_data = self.reader.read_table('whist')

            # Get unique tax years
            tax_years = sorted(set(
                (r.get('wh_year') or '').strip()
                for r in history_data
                if r.get('wh_year')
            ), reverse=True)

            current_year = tax_year if tax_year else (tax_years[0] if tax_years else '')

            if not current_year:
                return {'tax_year': '', 'tax_years': [], 'periods': []}

            # Get periods for the current year
            period_data = {}
            for row in history_data:
                if (row.get('wh_year') or '').strip() == current_year:
                    period = int(row.get('wh_period') or 0)
                    if period not in period_data:
                        period_data[period] = {
                            'period': period,
                            'pay_date': row.get('wh_paydt'),
                            'employee_count': 0
                        }
                    period_data[period]['employee_count'] += 1

            periods = sorted(period_data.values(), key=lambda p: p['period'], reverse=True)

            # Format pay dates
            for p in periods:
                if p['pay_date'] and hasattr(p['pay_date'], 'isoformat'):
                    p['pay_date'] = p['pay_date'].isoformat()

            return {
                'tax_year': current_year,
                'tax_years': tax_years,
                'periods': periods
            }

        except Exception as e:
            logger.error(f"Error reading payroll periods from Opera 3: {e}")
            return {'tax_year': '', 'tax_years': [], 'periods': []}

    def get_contributions(
        self,
        scheme_code: str,
        tax_year: str,
        period: int,
        group_codes: Optional[List[str]] = None
    ) -> List[EmployeeContribution]:
        """Get contributions for a scheme and period from Opera 3."""
        try:
            employees = {r['wn_ref'].strip(): r for r in self.reader.read_table('wname') if r.get('wn_ref')}
            enrolments = self.reader.read_table('wepen')
            history = self.reader.read_table('whist')

            # Filter enrolments for this scheme
            today = date.today()
            enrolled_refs = {}
            for enrol in enrolments:
                if (enrol.get('wep_code') or '').strip() == scheme_code:
                    leave_dt = enrol.get('wep_lfdt')
                    if leave_dt is None or (isinstance(leave_dt, date) and leave_dt > today):
                        ref = (enrol.get('wep_ref') or '').strip()
                        enrolled_refs[ref] = enrol

            # Filter history for this period
            period_history = {}
            for hist in history:
                if ((hist.get('wh_year') or '').strip() == tax_year and
                    int(hist.get('wh_period') or 0) == period):
                    ref = (hist.get('wh_ref') or '').strip()
                    period_history[ref] = hist

            contributions = []
            for ref, enrol in enrolled_refs.items():
                if ref not in employees or ref not in period_history:
                    continue

                emp = employees[ref]
                hist = period_history[ref]

                # Apply group filter
                emp_group = (emp.get('wn_group') or '').strip()
                if group_codes and emp_group not in group_codes:
                    continue

                pensionable = Decimal(str(hist.get('wh_penbl') or 0))
                ee_contrib = Decimal(str(hist.get('wh_pen') or 0))
                er_rate = Decimal(str(enrol.get('wep_erper') or 0)) / 100
                er_contrib = (pensionable * er_rate).quantize(Decimal('0.01'))

                contributions.append(EmployeeContribution(
                    employee_ref=ref,
                    surname=(emp.get('wn_surname') or '').strip(),
                    forename=(emp.get('wn_forenam') or '').strip(),
                    ni_number=(emp.get('wn_ninum') or '').strip(),
                    group=emp_group,
                    date_of_birth=emp.get('wn_birth'),
                    gender=(emp.get('wn_sex') or '').strip(),
                    address_1=(emp.get('wn_addrs1') or '').strip(),
                    address_2=(emp.get('wn_addrs2') or '').strip(),
                    address_3=(emp.get('wn_addrs3') or '').strip(),
                    address_4=(emp.get('wn_addrs4') or '').strip(),
                    postcode=(emp.get('wn_pstcde') or '').strip(),
                    title=(emp.get('wn_title') or '').strip(),
                    start_date=emp.get('wn_startdt'),
                    scheme_join_date=enrol.get('wep_jndt'),
                    leave_date=enrol.get('wep_lfdt'),
                    pensionable_earnings=pensionable,
                    employee_contribution=ee_contrib,
                    employer_contribution=er_contrib,
                    employee_rate=Decimal(str(enrol.get('wep_eeper') or 0)),
                    employer_rate=Decimal(str(enrol.get('wep_erper') or 0)),
                    is_new_starter=False,
                    is_leaver=enrol.get('wep_lfdt') is not None
                ))

            return sorted(contributions, key=lambda c: (c.surname, c.forename))

        except Exception as e:
            logger.error(f"Error reading contributions from Opera 3: {e}")
            return []


def get_pension_provider(data_source: str, sql_connector=None, opera3_reader=None) -> BasePensionDataProvider:
    """
    Factory function to get the appropriate pension data provider.

    Args:
        data_source: 'sql' for Opera SQL SE, 'opera3' for Opera 3
        sql_connector: SQLConnector instance (required for 'sql')
        opera3_reader: Opera3Reader instance (required for 'opera3')

    Returns:
        BasePensionDataProvider instance
    """
    if data_source == 'opera3':
        if not opera3_reader:
            raise ValueError("opera3_reader required for Opera 3 data source")
        return Opera3PensionProvider(opera3_reader)
    else:
        if not sql_connector:
            raise ValueError("sql_connector required for SQL data source")
        return OperaSQLPensionProvider(sql_connector)
