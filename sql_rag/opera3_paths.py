"""
Opera 3 company data-path resolution.

THE RULE (Harry, 2026-07-30): the only authoritative source for a company's
data folder is Opera's own company parameters — `System/seqco.dbf` →
`co_subdir`. Pegasus ships duplicate demo folders as pristine masters, and
installations move company folders (e.g. the 2026-07 refresh moved Z from
`Data\` to `Data\DemoData\`), so NEVER infer the path from folder layout or
hardcode it in tool configs. Resolve from seqco at connect/capture time.

`co_subdir` is stored root-relative in various shapes seen in the wild:
`\DATA\DEMODATA`, `...O3 SERVER VFP\DATA\INTSYS`, with either slash, any
case, optional trailing slash. We normalise against the mount root by
locating the path relative to the Opera root (the folder containing
`System/seqco.dbf`).
"""
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def resolve_company_subdir(mount_root: str, company_code: str) -> Optional[str]:
    """Return the company's data folder RELATIVE to mount_root (e.g.
    'Data/DemoData'), resolved from System/seqco.dbf. None if seqco or the
    company can't be read — callers fall back to their configured value.
    """
    try:
        from dbfread import DBF
    except ImportError:
        logger.warning("opera3_paths: dbfread not installed — cannot resolve seqco")
        return None

    root = Path(mount_root)
    seqco = root / 'System' / 'seqco.dbf'
    if not seqco.exists():
        seqco = root / 'System' / 'SEQCO.DBF'
    if not seqco.exists():
        logger.warning(f"opera3_paths: no seqco.dbf under {root}/System")
        return None

    want = (company_code or '').strip().upper()
    try:
        for rec in DBF(str(seqco), encoding='cp1252', char_decode_errors='ignore'):
            code = ''
            subdir = ''
            for k, v in rec.items():
                kl = k.lower()
                val = v.strip() if isinstance(v, str) else (v or '')
                if kl == 'co_code':
                    code = str(val).strip().upper()
                elif kl == 'co_subdir':
                    subdir = str(val).strip()
            if code != want or not subdir:
                continue
            norm = subdir.replace('\\', '/').strip('/')
            parts = [p for p in norm.split('/') if p]
            # Try progressively shorter suffixes of the stored path against
            # the mount root — handles both root-relative ('DATA/DEMODATA')
            # and absolute-ish ('C:/APPS/O3 SERVER VFP/DATA/DEMODATA') forms.
            for i in range(len(parts)):
                candidate = root.joinpath(*parts[i:])
                if candidate.is_dir():
                    rel = '/'.join(parts[i:])
                    logger.info(f"opera3_paths: {want} → '{rel}' (from seqco co_subdir '{subdir}')")
                    return rel
            logger.warning(f"opera3_paths: {want} co_subdir '{subdir}' matches no folder under {root}")
            return None
    except Exception as e:
        logger.warning(f"opera3_paths: seqco read failed: {e}")
    return None
