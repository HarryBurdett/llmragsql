#!/usr/bin/env bash
# Extract a SAM plugin from this monorepo into a standalone-repo
# layout under ~/sam-plugins-staging/sam-<plugin>.
#
# Usage:
#   ./apps-sam/scripts/extract-plugin.sh <plugin>
#
# Examples:
#   ./apps-sam/scripts/extract-plugin.sh balance-check
#   ./apps-sam/scripts/extract-plugin.sh bank-reconcile
#
# After running this, cd into the staging dir, verify the build, then
# create a GitHub repo and `git push -u origin main && git push --tags`.

set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: $0 <plugin>" >&2
  exit 1
fi

PLUGIN="$1"
case "$PLUGIN" in
  bank-reconcile|gocardless|suppliers|balance-check) ;;
  *) echo "Unknown plugin: $PLUGIN. Expected one of bank-reconcile, gocardless, suppliers, balance-check." >&2; exit 1 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC="$REPO_ROOT/apps-sam/$PLUGIN"
SHARED="$REPO_ROOT/apps-sam/shared/src"
OUT="${SAM_PLUGIN_STAGING:-$HOME/sam-plugins-staging}/sam-$PLUGIN"

if [ ! -d "$SRC" ]; then
  echo "Source not found: $SRC" >&2
  exit 1
fi

echo "[extract] Source: $SRC"
echo "[extract] Output: $OUT"

# 1. Wipe + copy
rm -rf "$OUT"
mkdir -p "$OUT"
cp -R "$SRC"/* "$OUT/"
cp "$SRC/.gitignore" "$OUT/" 2>/dev/null || true

# 2. Remove ephemera
rm -rf "$OUT/node_modules" "$OUT/dist" \
       "$OUT/frontend/node_modules" "$OUT/frontend/dist" \
       "$OUT/package-lock.json" "$OUT/frontend/package-lock.json"

# 3. Vendor shared into src/_shared/
mkdir -p "$OUT/src/_shared"
cp -R "$SHARED"/* "$OUT/src/_shared/"

# 4. Rewrite imports — services may go up one level (../_shared) or
#    two (../../_shared) depending on nesting. We use a relative
#    rewrite that handles both.
find "$OUT/src" -type f -name "*.ts" \
  -not -path "$OUT/src/_shared/*" \
  -exec sed -i '' "s|from '@sqlrag/sam-shared/opera'|from '$OUT/src/_shared/opera/index.js'|g" {} +
find "$OUT/src" -type f -name "*.ts" \
  -not -path "$OUT/src/_shared/*" \
  -exec sed -i '' "s|from '@sqlrag/sam-shared/posting'|from '$OUT/src/_shared/posting/index.js'|g" {} +
find "$OUT/src" -type f -name "*.ts" \
  -not -path "$OUT/src/_shared/*" \
  -exec sed -i '' "s|from '@sqlrag/sam-shared'|from '$OUT/src/_shared/index.js'|g" {} +

# Now turn the absolute paths into relative ones per file. Each file's
# relative depth from src/_shared determines the prefix.
python3 - <<PY
import os, re
root = "$OUT"
shared_abs = os.path.join(root, "src", "_shared")
for dirpath, _, files in os.walk(os.path.join(root, "src")):
    if dirpath.startswith(shared_abs):
        continue
    for fname in files:
        if not fname.endswith(".ts"):
            continue
        full = os.path.join(dirpath, fname)
        with open(full, "r") as fh:
            text = fh.read()
        if shared_abs not in text:
            continue
        # Compute relative path from this file's dir to shared_abs
        rel = os.path.relpath(shared_abs, dirpath)
        # rel will be like "_shared" (same dir) or "../_shared" or "../../_shared"
        # TS needs explicit ./ prefix for same-dir relative imports —
        # bare "_shared/..." would be treated as a node_modules package.
        if not rel.startswith(".."):
            rel = "./" + rel
        text = text.replace(shared_abs, rel)
        with open(full, "w") as fh:
            fh.write(text)
PY

# 5. Rewrite package.json: drop @sqlrag/sam-shared dep, build does
#    backend + frontend in one shot.
python3 - <<PY
import json, os
root = "$OUT"
plugin = "$PLUGIN"
pkg_path = os.path.join(root, "package.json")
with open(pkg_path) as fh:
    pkg = json.load(fh)
pkg["name"] = f"sam-{plugin}"
pkg["private"] = True
pkg.setdefault("scripts", {})
pkg["scripts"]["build"] = "tsc -p tsconfig.json && cd frontend && npm install --no-audit --no-fund && npm run build"
deps = pkg.get("dependencies", {})
deps.pop("@sqlrag/sam-shared", None)
pkg["dependencies"] = deps
# remove peerDependencies block — SAM provides express/knex via the host context
pkg.pop("peerDependencies", None)
with open(pkg_path, "w") as fh:
    json.dump(pkg, fh, indent=2)
    fh.write("\n")
PY

# 6. Drop tests that reference apps-sam workspace paths (rare). For
#    most plugins this is a no-op.

# 7. Initial install + build sanity check
echo "[extract] Running npm install..."
( cd "$OUT" && npm install --no-audit --no-fund --silent )

echo "[extract] Running tests..."
( cd "$OUT" && npm test 2>&1 | tail -3 )

echo "[extract] Running lint..."
( cd "$OUT" && npm run lint )

echo "[extract] Running build..."
( cd "$OUT" && npm run build 2>&1 | tail -5 )

echo
echo "Done. Standalone repo at: $OUT"
echo
echo "Next steps:"
echo "  cd $OUT"
echo "  git init && git add -A && git commit -m 'Initial commit — sam-$PLUGIN v1.0.0'"
echo "  git tag v1.0.0"
echo "  git remote add origin git@github.com:intsysuk/sam-$PLUGIN.git"
echo "  git push -u origin main && git push --tags"
echo
echo "Then register the repo in SAM Central (apps catalogue) and assign v1.0.0 to your client."
