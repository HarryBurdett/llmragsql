# Pension Export Utilities
# Exports payroll pension data to various provider formats

from .base_export import BasePensionExport, PensionContribution, ExportResult
from .nest_export import NestExport
from .aviva_export import AvivaExport
from .scottish_widows_export import ScottishWidowsExport
from .smart_pension_export import SmartPensionExport
from .peoples_pension_export import PeoplesPensionExport
from .royal_london_export import RoyalLondonExport
from .standard_life_export import StandardLifeExport
from .legal_general_export import LegalGeneralExport
from .aegon_export import AegonExport


# Provider registry - maps provider names to export classes
PENSION_PROVIDERS = {
    'nest': NestExport,
    'aviva': AvivaExport,
    'scottish_widows': ScottishWidowsExport,
    'smart_pension': SmartPensionExport,
    'peoples_pension': PeoplesPensionExport,
    'royal_london': RoyalLondonExport,
    'standard_life': StandardLifeExport,
    'legal_general': LegalGeneralExport,
    'aegon': AegonExport,
}

# Scheme type to provider mapping (from wpnsc.wps_type)
SCHEME_TYPE_PROVIDERS = {
    1: AvivaExport,           # Aviva
    2: ScottishWidowsExport,  # Scottish Widows
    3: SmartPensionExport,    # Smart Pension (PAPDIS)
    4: PeoplesPensionExport,  # People's Pension
    5: RoyalLondonExport,     # Royal London
    6: StandardLifeExport,    # Standard Life
    7: LegalGeneralExport,    # Legal & General
    8: AegonExport,           # Aegon
    11: NestExport,           # NEST (auto-enrolment)
}


def get_provider_class(provider_name: str):
    """Get export class by provider name."""
    return PENSION_PROVIDERS.get(provider_name.lower())


def get_provider_by_scheme_type(scheme_type: int):
    """Get export class by scheme type (from wpnsc.wps_type)."""
    return SCHEME_TYPE_PROVIDERS.get(scheme_type)


def list_providers():
    """List all available pension providers."""
    return [
        {'key': key, 'name': cls.PROVIDER_NAME, 'scheme_types': cls.SCHEME_TYPES}
        for key, cls in PENSION_PROVIDERS.items()
    ]


__all__ = [
    'BasePensionExport',
    'PensionContribution',
    'ExportResult',
    'NestExport',
    'AvivaExport',
    'ScottishWidowsExport',
    'SmartPensionExport',
    'PeoplesPensionExport',
    'RoyalLondonExport',
    'StandardLifeExport',
    'LegalGeneralExport',
    'AegonExport',
    'PENSION_PROVIDERS',
    'SCHEME_TYPE_PROVIDERS',
    'get_provider_class',
    'get_provider_by_scheme_type',
    'list_providers',
]
