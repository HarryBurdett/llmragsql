"""
Pension Export API routes.

Extracted from api/main.py — provides endpoints for pension scheme management,
contribution retrieval, and export file generation for various providers
(NEST, Aviva, Scottish Widows, etc.).
"""

import logging
import os
from fastapi import APIRouter, HTTPException, Query, Request

logger = logging.getLogger(__name__)

router = APIRouter()


# =============================================================================
# Helper: get pension data provider
# =============================================================================

def get_pension_data_provider(data_source: str = "sql"):
    """Get the appropriate pension data provider based on data source."""
    from sql_rag.pension_exports.data_provider import OperaSQLPensionProvider, Opera3PensionProvider
    from api.main import sql_connector, config

    if data_source == "opera3":
        # Check if Opera 3 is configured
        opera3_path = None
        if config and config.has_section("opera"):
            opera3_path = config.get("opera", "opera3_base_path", fallback=None)
        if not opera3_path:
            raise HTTPException(status_code=400, detail="Opera 3 path not configured")

        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(opera3_path)
        return Opera3PensionProvider(reader)
    else:
        return OperaSQLPensionProvider(sql_connector)


# =============================================================================
# PENSION EXPORT ENDPOINTS
# =============================================================================

@router.get("/api/pension/schemes")
async def get_pension_schemes(data_source: str = Query("sql", description="Data source: sql or opera3")):
    """Get all configured pension schemes."""
    try:
        provider = get_pension_data_provider(data_source)
        schemes = provider.get_pension_schemes()

        return {
            'success': True,
            'data_source': data_source,
            'schemes': [
                {
                    'code': s.code,
                    'description': s.description,
                    'provider_name': s.provider_name,
                    'provider_reference': s.provider_reference,
                    'scheme_reference': s.scheme_reference,
                    'employer_rate': float(s.employer_rate),
                    'employee_rate': float(s.employee_rate),
                    'auto_enrolment': s.auto_enrolment,
                    'scheme_type': s.scheme_type,
                    'enrolled_count': s.enrolled_count
                }
                for s in schemes
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pension schemes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/enrolled-employees")
async def get_pension_enrolled_employees(scheme_code: str = Query(...)):
    """Get employees enrolled in a specific pension scheme."""
    try:
        from api.main import sql_connector

        sql = f"""
        SELECT
            e.wep_ref,
            e.wep_code,
            e.wep_erper,
            e.wep_eeper,
            e.wep_jndt,
            e.wep_lfdt,
            e.wep_ter,
            e.wep_tee,
            w.wn_surname,
            w.wn_forenam,
            w.wn_ninum,
            w.wn_birth
        FROM wepen e
        JOIN wname w ON e.wep_ref = w.wn_ref
        WHERE e.wep_code = '{scheme_code}'
          AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())
        ORDER BY w.wn_surname, w.wn_forenam
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        employees = []
        for row in result or []:
            employees.append({
                'employee_ref': row['wep_ref'].strip() if row.get('wep_ref') else '',
                'surname': row['wn_surname'].strip() if row.get('wn_surname') else '',
                'forename': row['wn_forenam'].strip() if row.get('wn_forenam') else '',
                'ni_number': row['wn_ninum'].strip() if row.get('wn_ninum') else '',
                'date_of_birth': row['wn_birth'].isoformat() if row.get('wn_birth') else None,
                'join_date': row['wep_jndt'].isoformat() if row.get('wep_jndt') else None,
                'leave_date': row['wep_lfdt'].isoformat() if row.get('wep_lfdt') else None,
                'employer_rate': float(row.get('wep_erper') or 0),
                'employee_rate': float(row.get('wep_eeper') or 0),
                'total_employer_contributions': float(row.get('wep_ter') or 0),
                'total_employee_contributions': float(row.get('wep_tee') or 0)
            })

        return {
            'success': True,
            'scheme_code': scheme_code,
            'employees': employees
        }
    except Exception as e:
        logger.error(f"Error getting enrolled employees: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/payroll-periods")
async def get_payroll_periods(
    tax_year: str = Query(None),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Get available payroll periods."""
    try:
        provider = get_pension_data_provider(data_source)
        result = provider.get_payroll_periods(tax_year)

        return {
            'success': True,
            'data_source': data_source,
            'tax_year': result.get('tax_year', ''),
            'tax_years': result.get('tax_years', []),
            'periods': result.get('periods', [])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting payroll periods: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/nest/preview")
async def preview_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...)
):
    """Preview NEST pension export for a specific period."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport
        from api.main import sql_connector

        nest = NestExport(sql_connector)
        preview = nest.preview_export(tax_year, period)

        return {
            'success': True,
            'preview': preview
        }
    except Exception as e:
        logger.error(f"Error previewing NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/pension/nest/generate")
async def generate_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account")
):
    """Generate NEST pension contribution CSV file."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport
        from api.main import sql_connector

        nest = NestExport(sql_connector)
        result = nest.generate_csv(tax_year, period, payment_source)

        if result.success:
            return {
                'success': True,
                'filename': result.filename,
                'csv_content': result.csv_content,
                'record_count': result.record_count,
                'total_employer_contributions': float(result.total_employer_contributions),
                'total_employee_contributions': float(result.total_employee_contributions),
                'total_pensionable_earnings': float(result.total_pensionable_earnings),
                'warnings': result.warnings
            }
        else:
            return {
                'success': False,
                'errors': result.errors,
                'warnings': result.warnings
            }
    except Exception as e:
        logger.error(f"Error generating NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/nest/download")
async def download_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account")
):
    """Download NEST pension contribution CSV file."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport
        from fastapi.responses import Response
        from api.main import sql_connector

        nest = NestExport(sql_connector)
        result = nest.generate_csv(tax_year, period, payment_source)

        if result.success:
            return Response(
                content=result.csv_content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={result.filename}"
                }
            )
        else:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/config")
async def get_pension_config():
    """Get pension configuration for the current company."""
    from api.main import current_company, config

    company_name = "Unknown"
    export_folder = ""
    pension_provider = ""
    data_source = "sql"  # Default to SQL SE

    if current_company:
        company_name = current_company.get("name", "Unknown")
        payroll_config = current_company.get("payroll", {})
        export_folder = payroll_config.get("pension_export_folder", "")
        pension_provider = payroll_config.get("pension_provider", "")

    # Check if Opera 3 is configured
    opera3_available = False
    if config and config.has_section("opera"):
        opera3_path = config.get("opera", "opera3_base_path", fallback=None)
        opera3_available = bool(opera3_path)

    return {
        "success": True,
        "company_name": company_name,
        "export_folder": export_folder,
        "pension_provider": pension_provider,
        "data_source": data_source,
        "opera3_available": opera3_available,
        "providers": [
            {"key": "nest", "name": "NEST"},
            {"key": "aviva", "name": "Aviva"},
            {"key": "scottish_widows", "name": "Scottish Widows"},
            {"key": "smart_pension", "name": "Smart Pension (PAPDIS)"},
            {"key": "peoples_pension", "name": "People's Pension"},
            {"key": "royal_london", "name": "Royal London"},
            {"key": "standard_life", "name": "Standard Life"},
            {"key": "legal_general", "name": "Legal & General"},
            {"key": "aegon", "name": "Aegon"}
        ]
    }


@router.post("/api/pension/config")
async def save_pension_config(request: Request):
    """Save pension configuration for the current company."""
    import json as json_mod
    from api.main import current_company, COMPANIES_DIR

    if not current_company:
        raise HTTPException(status_code=400, detail="No company selected")

    try:
        body = await request.json()
        pension_provider = body.get("pension_provider", "")
        pension_export_folder = body.get("pension_export_folder", "")

        # Update current_company in memory
        if "payroll" not in current_company:
            current_company["payroll"] = {}
        current_company["payroll"]["pension_provider"] = pension_provider
        current_company["payroll"]["pension_export_folder"] = pension_export_folder

        # Save to company JSON file
        company_id = current_company.get("id")
        if company_id:
            filepath = os.path.join(COMPANIES_DIR, f"{company_id}.json")
            if os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    json_mod.dump(current_company, f, indent=2)

        return {
            "success": True,
            "message": "Pension settings saved",
            "pension_provider": pension_provider,
            "pension_export_folder": pension_export_folder
        }
    except Exception as e:
        logger.error(f"Error saving pension config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/employee-groups")
async def get_employee_groups(data_source: str = Query("sql", description="Data source: sql or opera3")):
    """Get all employee groups for payroll filtering."""
    try:
        provider = get_pension_data_provider(data_source)
        groups = provider.get_employee_groups()

        return {
            'success': True,
            'data_source': data_source,
            'groups': [
                {
                    'code': g.code,
                    'description': g.name,
                    'employee_count': g.employee_count
                }
                for g in groups
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting employee groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/payment-sources")
async def get_pension_payment_sources(scheme_code: str = Query(...)):
    """Get payment sources configured for a pension scheme."""
    try:
        from api.main import sql_connector

        # Get payment sources from wpnps (pension payment sources) table
        sql = f"""
        SELECT
            wpp_code,
            wpp_name,
            wpp_default
        FROM wpnps
        WHERE wpp_schcode = '{scheme_code}'
        ORDER BY wpp_name
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        sources = []
        for row in result or []:
            sources.append({
                'code': row['wpp_code'].strip() if row.get('wpp_code') else '',
                'name': row['wpp_name'].strip() if row.get('wpp_name') else '',
                'is_default': bool(row.get('wpp_default'))
            })

        # If no payment sources found, return a default one
        if not sources:
            sources = [{'code': 'DEFAULT', 'name': 'Bank Account', 'is_default': True}]

        return {
            'success': True,
            'scheme_code': scheme_code,
            'payment_sources': sources
        }
    except Exception as e:
        logger.error(f"Error getting payment sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/contribution-groups")
async def get_pension_contribution_groups(scheme_code: str = Query(...)):
    """Get contribution groups for a pension scheme."""
    try:
        from api.main import sql_connector

        # Get contribution groups from wpncg (pension contribution groups)
        sql = f"""
        SELECT
            wpc_code,
            wpc_desc,
            wpc_freq
        FROM wpncg
        WHERE wpc_schcode = '{scheme_code}'
        ORDER BY wpc_desc
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        groups = []
        for row in result or []:
            groups.append({
                'code': row['wpc_code'].strip() if row.get('wpc_code') else '',
                'description': row['wpc_desc'].strip() if row.get('wpc_desc') else '',
                'frequency': row['wpc_freq'].strip() if row.get('wpc_freq') else 'Monthly'
            })

        # If no contribution groups found, return a default one
        if not groups:
            groups = [{'code': 'MONTHLY', 'description': 'Monthly', 'frequency': 'Monthly'}]

        return {
            'success': True,
            'scheme_code': scheme_code,
            'contribution_groups': groups
        }
    except Exception as e:
        logger.error(f"Error getting contribution groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/providers")
async def get_pension_providers():
    """Get list of all available pension export providers."""
    try:
        from sql_rag.pension_exports import list_providers

        providers = list_providers()

        return {
            'success': True,
            'providers': providers
        }
    except Exception as e:
        logger.error(f"Error getting pension providers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/contributions")
async def get_pension_contributions(
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    group_codes: str = Query(None, description="Comma-separated group codes to filter by"),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Get pension contributions for a specific scheme and period."""
    try:
        from decimal import Decimal

        provider = get_pension_data_provider(data_source)
        group_list = group_codes.split(',') if group_codes else None
        contributions_data = provider.get_contributions(scheme_code, tax_year, period, group_list)

        # Calculate totals
        total_ee = Decimal('0')
        total_er = Decimal('0')
        total_pensionable = Decimal('0')
        new_starters = 0
        leavers = 0

        contributions = []
        for c in contributions_data:
            total_ee += c.employee_contribution
            total_er += c.employer_contribution
            total_pensionable += c.pensionable_earnings

            if c.is_new_starter:
                new_starters += 1
            if c.is_leaver:
                leavers += 1

            contributions.append({
                'employee_ref': c.employee_ref,
                'surname': c.surname,
                'forename': c.forename,
                'ni_number': c.ni_number,
                'group': c.group,
                'date_of_birth': c.date_of_birth.isoformat() if c.date_of_birth else None,
                'gender': c.gender,
                'address_1': c.address_1,
                'address_2': c.address_2,
                'address_3': c.address_3,
                'postcode': c.postcode,
                'title': c.title,
                'start_date': c.start_date.isoformat() if c.start_date else None,
                'scheme_join_date': c.scheme_join_date.isoformat() if c.scheme_join_date else None,
                'leave_date': c.leave_date.isoformat() if c.leave_date else None,
                'pensionable_earnings': float(c.pensionable_earnings),
                'employee_contribution': float(c.employee_contribution),
                'employer_contribution': float(c.employer_contribution),
                'employee_rate': float(c.employee_rate),
                'employer_rate': float(c.employer_rate),
                'is_new_starter': c.is_new_starter,
                'is_leaver': c.is_leaver
            })

        return {
            'success': True,
            'data_source': data_source,
            'scheme_code': scheme_code,
            'tax_year': tax_year,
            'period': period,
            'contributions': contributions,
            'summary': {
                'total_employees': len(contributions),
                'new_starters': new_starters,
                'leavers': leavers,
                'total_pensionable_earnings': float(total_pensionable),
                'total_employee_contributions': float(total_ee),
                'total_employer_contributions': float(total_er)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pension contributions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/pension/generate")
async def generate_pension_export(
    provider: str = Query(..., description="Provider key: nest, aviva, scottish_widows, etc."),
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account"),
    group_codes: str = Query(None, description="Comma-separated group codes"),
    employee_refs: str = Query(None, description="Comma-separated employee refs to include"),
    output_folder: str = Query(None, description="Folder path to save the export file"),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Generate pension export file for any provider. Supports both Opera SQL SE and Opera 3."""
    try:
        from sql_rag.pension_exports import get_provider_class, PENSION_PROVIDERS
        from api.main import sql_connector

        # Get the provider class
        provider_class = get_provider_class(provider)
        if not provider_class:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown provider: {provider}. Available: {list(PENSION_PROVIDERS.keys())}"
            )

        # Check if this is the base export class or NEST (which has its own implementation)
        if provider == 'nest':
            from sql_rag.pension_exports.nest_export import NestExport
            exporter = NestExport(sql_connector)
            result = exporter.generate_csv(tax_year, period, payment_source)

            if result.success:
                return {
                    'success': True,
                    'provider': provider,
                    'filename': result.filename,
                    'csv_content': result.csv_content,
                    'record_count': result.record_count,
                    'total_employer_contributions': float(result.total_employer_contributions),
                    'total_employee_contributions': float(result.total_employee_contributions),
                    'total_pensionable_earnings': float(result.total_pensionable_earnings),
                    'warnings': result.warnings
                }
            else:
                return {
                    'success': False,
                    'errors': result.errors,
                    'warnings': result.warnings
                }

        # For other providers, use the base export class
        exporter = provider_class(sql_connector, scheme_code)

        # Get contributions with optional filtering
        group_list = group_codes.split(',') if group_codes else None
        employee_list = employee_refs.split(',') if employee_refs else None

        result = exporter.generate_export(
            tax_year=tax_year,
            period=period,
            payment_source=payment_source,
            group_codes=group_list,
            employee_refs=employee_list,
            output_folder=output_folder
        )

        if result.success:
            return {
                'success': True,
                'provider': provider,
                'filename': result.filename,
                'filepath': result.filepath,
                'content': result.content,
                'content_type': result.content_type,
                'record_count': result.record_count,
                'total_employer_contributions': float(result.total_employer_contributions),
                'total_employee_contributions': float(result.total_employee_contributions),
                'total_pensionable_earnings': float(result.total_pensionable_earnings),
                'warnings': result.warnings
            }
        else:
            return {
                'success': False,
                'errors': result.errors,
                'warnings': result.warnings
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating pension export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pension/download")
async def download_pension_export(
    provider: str = Query(...),
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account"),
    group_codes: str = Query(None),
    employee_refs: str = Query(None),
    output_folder: str = Query(None, description="Optional folder to also save file to")
):
    """Download pension export file for any provider."""
    try:
        from sql_rag.pension_exports import get_provider_class
        from fastapi.responses import Response
        from api.main import sql_connector

        provider_class = get_provider_class(provider)
        if not provider_class:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

        # Handle NEST separately
        if provider == 'nest':
            from sql_rag.pension_exports.nest_export import NestExport
            exporter = NestExport(sql_connector)
            result = exporter.generate_csv(tax_year, period, payment_source)

            if result.success:
                return Response(
                    content=result.csv_content,
                    media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={result.filename}"}
                )
            else:
                raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")

        # For other providers
        exporter = provider_class(sql_connector, scheme_code)
        group_list = group_codes.split(',') if group_codes else None
        employee_list = employee_refs.split(',') if employee_refs else None

        result = exporter.generate_export(
            tax_year=tax_year,
            period=period,
            payment_source=payment_source,
            group_codes=group_list,
            employee_refs=employee_list,
            output_folder=output_folder
        )

        if result.success:
            return Response(
                content=result.content,
                media_type=result.content_type,
                headers={"Content-Disposition": f"attachment; filename={result.filename}"}
            )
        else:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading pension export: {e}")
        raise HTTPException(status_code=500, detail=str(e))
