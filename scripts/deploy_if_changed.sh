#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/home/austin/globalstar_receiver"
BRANCH="main"

cd "$REPO_DIR"

# Skip if there are local changes (avoid clobbering work-in-progress)
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "$(date -Is) - Local changes present; skipping auto-deploy."
  exit 0
fi

# Fetch and compare
git fetch origin

LOCAL="$(git rev-parse "$BRANCH" || true)"
REMOTE="$(git rev-parse "origin/$BRANCH" || true)"

if [ "$LOCAL" != "$REMOTE" ]; then
  echo "$(date -Is) - Updating $BRANCH from origin..."
  git pull --ff-only origin "$BRANCH"
  echo "$(date -Is) - Restarting service..."
  sudo /usr/bin/systemctl restart globalstar_receiver.service
  echo "$(date -Is) - Deploy complete."
else
  echo "$(date -Is) - Already up-to-date."
fi
