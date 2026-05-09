#!/usr/bin/env bash
# Initialise + commit + tag + push a staged plugin repo to GitHub.
#
# Assumes:
#   1. The staged repo exists at ~/sam-plugins-staging/sam-<plugin>/
#      (run ./extract-all.sh first if not)
#   2. The GitHub repo has been created under intsysuk/sam-<plugin>
#      (we don't auto-create — that needs your token + UI confirmation)
#   3. Your SSH key has push access to intsysuk
#
# Usage:
#   ./apps-sam/scripts/push-to-github.sh <plugin> [version]
#
# Examples:
#   ./apps-sam/scripts/push-to-github.sh balance-check
#   ./apps-sam/scripts/push-to-github.sh bank-reconcile v1.0.0
#
# Override the GitHub org with $SAM_GH_ORG (default: intsysuk).

set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: $0 <plugin> [version]" >&2
  exit 1
fi

PLUGIN="$1"
VERSION="${2:-v1.0.0}"
ORG="${SAM_GH_ORG:-intsysuk}"
STAGING="${SAM_PLUGIN_STAGING:-$HOME/sam-plugins-staging}"
REPO_DIR="$STAGING/sam-$PLUGIN"
REMOTE_URL="git@github.com:$ORG/sam-$PLUGIN.git"

case "$PLUGIN" in
  bank-reconcile|gocardless|suppliers|balance-check) ;;
  *) echo "Unknown plugin: $PLUGIN" >&2; exit 1 ;;
esac

if [ ! -d "$REPO_DIR" ]; then
  echo "Staged repo not found: $REPO_DIR" >&2
  echo "Run ./apps-sam/scripts/extract-plugin.sh $PLUGIN first." >&2
  exit 1
fi

cd "$REPO_DIR"

if [ ! -d .git ]; then
  echo "[push] git init..."
  git init -q
fi

# Default branch = main
git symbolic-ref HEAD refs/heads/main 2>/dev/null || true

git add -A
if git diff --cached --quiet && [ -z "$(git rev-list --count HEAD 2>/dev/null || echo)" ]; then
  echo "[push] nothing to commit"
else
  echo "[push] committing..."
  git commit -q -m "sam-$PLUGIN $VERSION" || true
fi

# Tag
if git rev-parse --verify "$VERSION" >/dev/null 2>&1; then
  echo "[push] tag $VERSION already exists — skipping"
else
  git tag "$VERSION"
  echo "[push] tagged $VERSION"
fi

# Remote
if git remote get-url origin >/dev/null 2>&1; then
  current=$(git remote get-url origin)
  if [ "$current" != "$REMOTE_URL" ]; then
    echo "[push] updating remote (was $current)"
    git remote set-url origin "$REMOTE_URL"
  fi
else
  echo "[push] adding remote $REMOTE_URL"
  git remote add origin "$REMOTE_URL"
fi

echo "[push] pushing main + tags..."
git push -u origin main
git push --tags

echo
echo "✓ sam-$PLUGIN $VERSION pushed to $REMOTE_URL"
echo
echo "Next: register $REMOTE_URL in SAM Central (apps catalogue),"
echo "      assign $VERSION to your client, hit Sync."
