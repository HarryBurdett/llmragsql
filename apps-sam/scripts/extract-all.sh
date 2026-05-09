#!/usr/bin/env bash
# Extract all four SAM plugins from the monorepo in one shot.
#
# Usage:
#   ./apps-sam/scripts/extract-all.sh
#
# Output: ~/sam-plugins-staging/sam-{balance-check,bank-reconcile,gocardless,suppliers}/
# Each is a fully-built standalone repo ready to push to GitHub.
#
# Env:
#   SAM_PLUGIN_STAGING — override staging directory (default: ~/sam-plugins-staging)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PLUGINS=(balance-check bank-reconcile gocardless suppliers)

echo
echo "════════════════════════════════════════════════════════════════"
echo "  Extracting all 4 SAM plugins from monorepo"
echo "════════════════════════════════════════════════════════════════"
echo

failed=()
for plugin in "${PLUGINS[@]}"; do
  echo "─── $plugin ──────────────────────────────────────────────────"
  if "$SCRIPT_DIR/extract-plugin.sh" "$plugin" >/tmp/extract-$plugin.log 2>&1; then
    tail -3 /tmp/extract-$plugin.log | sed 's/^/  /'
  else
    echo "  ✗ FAILED — see /tmp/extract-$plugin.log"
    failed+=("$plugin")
  fi
  echo
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "════════════════════════════════════════════════════════════════"
  echo "  ✗ ${#failed[@]} extraction(s) failed: ${failed[*]}"
  echo "════════════════════════════════════════════════════════════════"
  exit 1
fi

STAGING="${SAM_PLUGIN_STAGING:-$HOME/sam-plugins-staging}"

echo "════════════════════════════════════════════════════════════════"
echo "  ✓ All 4 plugins extracted to $STAGING"
echo "════════════════════════════════════════════════════════════════"
echo
echo "  $STAGING/sam-balance-check"
echo "  $STAGING/sam-bank-reconcile"
echo "  $STAGING/sam-gocardless"
echo "  $STAGING/sam-suppliers"
echo
echo "Next: push each to GitHub and register in SAM Central."
echo "See apps-sam/EMBEDDING.md for the full sequence, or run"
echo "  ./apps-sam/scripts/push-to-github.sh <plugin>"
echo "after creating the GitHub repos under your org."
