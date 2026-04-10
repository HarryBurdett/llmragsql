"""
Migration Assistant API — helps migrate modules to a new platform.

Reads module.json manifests, packages code/config, generates integration
code for the target system (FastAPI + React).
"""
from fastapi import APIRouter, Query
from typing import Dict, List, Optional
import json
import os
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)
router = APIRouter()

APPS_DIR = Path(__file__).parent.parent.parent
PROJECT_ROOT = APPS_DIR.parent


def _discover_modules() -> List[Dict]:
    """Discover all modules with module.json manifests."""
    modules = []
    for app_dir in sorted(APPS_DIR.iterdir()):
        manifest_path = app_dir / 'module.json'
        if manifest_path.exists():
            try:
                with open(manifest_path) as f:
                    manifest = json.load(f)
                manifest['_path'] = str(app_dir.relative_to(PROJECT_ROOT))
                manifest['_manifest_path'] = str(manifest_path.relative_to(PROJECT_ROOT))
                modules.append(manifest)
            except Exception as e:
                logger.warning(f"Could not read {manifest_path}: {e}")
    return modules


def _get_module_files(module_id: str) -> Dict[str, List[str]]:
    """Get all files needed for a module, categorised by type."""
    module_dir = APPS_DIR / module_id
    if not module_dir.exists():
        return {}

    files = {
        'backend': [],       # Python API files
        'frontend': [],      # React/TSX pages
        'shared_libs': [],   # sql_rag/ dependencies
        'data_files': [],    # SQLite DBs, config files
        'tests': [],         # Test files
    }

    # Backend files
    for f in sorted(module_dir.rglob('*.py')):
        files['backend'].append(str(f.relative_to(PROJECT_ROOT)))

    # Module manifest
    manifest_path = module_dir / 'module.json'
    if manifest_path.exists():
        files['backend'].append(str(manifest_path.relative_to(PROJECT_ROOT)))

    # Frontend pages — scan for matching component names
    manifest = {}
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)

    frontend_dir = PROJECT_ROOT / 'frontend' / 'src' / 'pages'
    for page in manifest.get('frontend_pages', []):
        # Try to find the matching TSX file
        path_part = page['path'].strip('/').split('/')[-1]
        for tsx_file in frontend_dir.glob('*.tsx'):
            # Match by page path or label
            name_lower = tsx_file.stem.lower()
            if path_part.lower().replace('-', '') in name_lower.replace('-', ''):
                files['frontend'].append(str(tsx_file.relative_to(PROJECT_ROOT)))
                break

    # Shared libraries — scan imports in the backend files
    shared_libs = set()
    for backend_file in files['backend']:
        if backend_file.endswith('.py'):
            try:
                content = (PROJECT_ROOT / backend_file).read_text()
                for line in content.split('\n'):
                    if 'from sql_rag.' in line or 'import sql_rag.' in line:
                        # Extract module name
                        parts = line.split('sql_rag.')[1].split()[0].split('.')[0]
                        lib_file = f"sql_rag/{parts}.py"
                        if (PROJECT_ROOT / lib_file).exists():
                            shared_libs.add(lib_file)
            except Exception:
                pass

    files['shared_libs'] = sorted(shared_libs)

    return files


def _generate_router_code(manifest: Dict) -> str:
    """Generate the Python code to register this module's router."""
    module_id = manifest['module_id']
    lines = []
    lines.append(f"# --- {manifest['name']} ---")
    lines.append(f"from apps.{module_id}.api.routes import router as {module_id}_router")
    lines.append(f"app.include_router({module_id}_router)")
    return '\n'.join(lines)


def _generate_react_routes(manifest: Dict) -> str:
    """Generate React route code for this module's pages."""
    lines = []
    lines.append(f"// --- {manifest['name']} ---")
    for page in manifest.get('frontend_pages', []):
        # Derive component name from label
        component = page['label'].replace(' ', '').replace('&', 'And').replace('-', '')
        lines.append(f"import {{ {component} }} from './pages/{component}';")
    lines.append("")
    for page in manifest.get('frontend_pages', []):
        component = page['label'].replace(' ', '').replace('&', 'And').replace('-', '')
        lines.append(f'<Route path="{page["path"]}" element={{<ProtectedRoute><{component} /></ProtectedRoute>}} />')
    return '\n'.join(lines)


def _generate_menu_items(manifest: Dict) -> str:
    """Generate Layout.tsx menu items for this module."""
    lines = []
    lines.append(f"// --- {manifest['name']} ---")
    for page in manifest.get('frontend_pages', []):
        lines.append(f"{{ path: '{page['path']}', label: '{page['label']}', icon: FileText, description: '{manifest.get('description', '')[:50]}' }},")
    return '\n'.join(lines)


# ============================================================
# API Endpoints
# ============================================================

@router.get("/api/migration/modules")
async def list_migration_modules():
    """List all modules available for migration."""
    modules = _discover_modules()

    result = []
    for m in modules:
        files = _get_module_files(m['module_id'])
        result.append({
            "module_id": m['module_id'],
            "name": m['name'],
            "description": m.get('description', ''),
            "version": m.get('version', ''),
            "path": m.get('_path', ''),
            "dependencies": list(m.get('dependencies', {}).keys()),
            "settings_count": len(m.get('settings', [])),
            "data_stores": [d['file'] for d in m.get('data_stores', [])],
            "endpoints": m.get('api_endpoints', []),
            "pages": m.get('frontend_pages', []),
            "locking_rules": m.get('locking_rules', {}),
            "file_counts": {k: len(v) for k, v in files.items()},
        })

    return {"success": True, "modules": result, "count": len(result)}


@router.get("/api/migration/modules/{module_id}")
async def get_module_detail(module_id: str):
    """Get full migration detail for a specific module."""
    modules = _discover_modules()
    manifest = next((m for m in modules if m['module_id'] == module_id), None)
    if not manifest:
        return {"success": False, "error": f"Module '{module_id}' not found"}

    files = _get_module_files(module_id)

    return {
        "success": True,
        "module": manifest,
        "files": files,
        "integration_code": {
            "router_registration": _generate_router_code(manifest),
            "react_routes": _generate_react_routes(manifest),
            "menu_items": _generate_menu_items(manifest),
        },
    }


@router.get("/api/migration/export/{module_id}")
async def export_module_package(module_id: str):
    """
    Generate a complete migration package for a module.
    Returns all code, config, and integration instructions.
    """
    modules = _discover_modules()
    manifest = next((m for m in modules if m['module_id'] == module_id), None)
    if not manifest:
        return {"success": False, "error": f"Module '{module_id}' not found"}

    files = _get_module_files(module_id)

    # Build file listing with sizes
    file_details = {}
    for category, file_list in files.items():
        file_details[category] = []
        for f in file_list:
            full_path = PROJECT_ROOT / f
            size = full_path.stat().st_size if full_path.exists() else 0
            file_details[category].append({
                "path": f,
                "size": size,
                "size_formatted": f"{size / 1024:.1f} KB" if size > 1024 else f"{size} bytes",
            })

    # Settings with current values (for this company)
    current_settings = {}
    try:
        from sql_rag.supplier_statement_db import get_supplier_statement_db
        db = get_supplier_statement_db()
        for s in manifest.get('settings', []):
            val = db.get_config(s['key'], s.get('default', ''))
            current_settings[s['key']] = {
                "label": s['label'],
                "type": s['type'],
                "value": str(val) if val else '',
                "default": str(s.get('default', '')),
                "required": s.get('required', False),
            }
    except Exception:
        for s in manifest.get('settings', []):
            current_settings[s['key']] = {
                "label": s['label'],
                "type": s['type'],
                "value": '',
                "default": str(s.get('default', '')),
                "required": s.get('required', False),
            }

    # Data files with sizes
    data_files = []
    for d in manifest.get('data_stores', []):
        # Try to find the actual file
        for search_dir in ['data/intsys', 'data']:
            for root, dirs, fnames in os.walk(PROJECT_ROOT / search_dir):
                if d['file'] in fnames:
                    full = Path(root) / d['file']
                    data_files.append({
                        "file": d['file'],
                        "path": str(full.relative_to(PROJECT_ROOT)),
                        "size": full.stat().st_size,
                        "size_formatted": f"{full.stat().st_size / 1024:.1f} KB",
                        "description": d.get('description', ''),
                    })
                    break

    return {
        "success": True,
        "module": {
            "id": manifest['module_id'],
            "name": manifest['name'],
            "description": manifest.get('description', ''),
            "version": manifest.get('version', ''),
        },
        "files": file_details,
        "settings": current_settings,
        "data_files": data_files,
        "dependencies": manifest.get('dependencies', {}),
        "locking_rules": manifest.get('locking_rules', {}),
        "integration_code": {
            "router_registration": _generate_router_code(manifest),
            "react_routes": _generate_react_routes(manifest),
            "menu_items": _generate_menu_items(manifest),
        },
        "migration_steps": [
            f"1. Copy backend files: {len(files.get('backend', []))} files from apps/{module_id}/",
            f"2. Copy shared libraries: {len(files.get('shared_libs', []))} files from sql_rag/",
            f"3. Copy frontend pages: {len(files.get('frontend', []))} TSX files",
            f"4. Add router registration to main.py (code provided below)",
            f"5. Add React routes to App.tsx (code provided below)",
            f"6. Add menu items to Layout.tsx (code provided below)",
            f"7. Copy data files: {len(data_files)} SQLite databases",
            f"8. Configure {len(current_settings)} settings",
            f"9. Verify locking rules are supported by the target database",
            f"10. Test each endpoint and page",
        ],
        "generated_at": datetime.now().isoformat(),
    }


@router.get("/api/migration/export-all")
async def export_all_modules():
    """Generate migration summary for all modules."""
    modules = _discover_modules()
    all_files = set()
    all_libs = set()

    summaries = []
    for m in modules:
        files = _get_module_files(m['module_id'])
        for f_list in files.values():
            all_files.update(f_list)
        all_libs.update(files.get('shared_libs', []))

        summaries.append({
            "module_id": m['module_id'],
            "name": m['name'],
            "backend_files": len(files.get('backend', [])),
            "frontend_files": len(files.get('frontend', [])),
            "shared_libs": len(files.get('shared_libs', [])),
            "settings": len(m.get('settings', [])),
            "data_stores": len(m.get('data_stores', [])),
            "endpoints": len(m.get('api_endpoints', [])),
            "pages": len(m.get('frontend_pages', [])),
            "router_code": _generate_router_code(m),
            "react_routes": _generate_react_routes(m),
            "menu_items": _generate_menu_items(m),
        })

    return {
        "success": True,
        "total_modules": len(modules),
        "total_files": len(all_files),
        "total_shared_libs": len(all_libs),
        "shared_libs": sorted(all_libs),
        "modules": summaries,
        "combined_router_code": '\n\n'.join(s['router_code'] for s in summaries),
        "combined_react_routes": '\n\n'.join(s['react_routes'] for s in summaries),
        "combined_menu_items": '\n\n'.join(s['menu_items'] for s in summaries),
        "generated_at": datetime.now().isoformat(),
    }
