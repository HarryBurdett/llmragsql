#!/usr/bin/env bash
# Hit each installed plugin's status endpoint via the SAM portal and
# report green/red. Run this AFTER SAM has installed the plugins.
#
# Usage:
#   ./apps-sam/scripts/post-install-smoke.sh <sam-portal-url> <session-cookie>
#
# Where:
#   <sam-portal-url> is the URL of your SAM portal (e.g. http://localhost)
#   <session-cookie> is the value of your sam_session cookie (from the
#                     browser devtools Application tab) — needed because
#                     SAM auth-gates the plugin routes
#
# Each check:
#   - GET <url>/api/apps/<plugin>/api/<plugin>/status
#   - 200 + {success: true} → green
#   - anything else → red, print the response
#
# Returns 0 if all green, non-zero otherwise.

set -uo pipefail

URL="${1:-}"
COOKIE="${2:-}"

if [ -z "$URL" ] || [ -z "$COOKIE" ]; then
  cat >&2 <<EOF
Usage: $0 <sam-portal-url> <session-cookie>

Get the cookie value:
  1. Open SAM portal in browser, log in
  2. DevTools → Application → Cookies → sam_session → copy Value
EOF
  exit 1
fi

# Trim trailing slash
URL="${URL%/}"

# Plugin → status path
declare -A STATUS_PATHS=(
  [balance-check]="/api/apps/balance-check/api/reconcile/summary"
  [bank-reconcile]="/api/apps/bank-reconcile/api/bank-reconcile/status"
  [gocardless]="/api/apps/gocardless/api/gocardless/setup-status"
  [suppliers]="/api/apps/suppliers/api/suppliers/status"
)

failed=0
echo
echo "═══════════════════════════════════════════════════════════"
echo "  SAM plugin smoke test"
echo "  Target: $URL"
echo "═══════════════════════════════════════════════════════════"
echo

for plugin in balance-check bank-reconcile gocardless suppliers; do
  path="${STATUS_PATHS[$plugin]}"
  printf "  %-18s " "$plugin"
  response=$(curl -s -w "\n%{http_code}" \
    -H "Cookie: sam_session=$COOKIE" \
    "$URL$path" 2>&1)
  code=$(echo "$response" | tail -1)
  body=$(echo "$response" | sed '$d')

  if [ "$code" = "200" ]; then
    if echo "$body" | grep -q '"success":true\|"success": true'; then
      echo "✓ green ($code)"
    else
      echo "○ amber ($code, no success:true)"
      echo "    body: $(echo "$body" | head -c 200)"
      failed=$((failed + 1))
    fi
  else
    echo "✗ red ($code)"
    echo "    body: $(echo "$body" | head -c 200)"
    failed=$((failed + 1))
  fi
done

echo
if [ "$failed" -eq 0 ]; then
  echo "All 4 plugins green — install successful."
  exit 0
else
  echo "$failed plugin(s) failed."
  exit 1
fi
