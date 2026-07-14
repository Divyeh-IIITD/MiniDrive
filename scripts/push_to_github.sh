#!/usr/bin/env bash
# Push the current workspace to the specified GitHub repository.
# IMPORTANT: This script does not include credentials. Run locally where
# your git is configured (SSH key or credential helper) or after `gh auth login`.

set -euo pipefail

REMOTE_URL="https://github.com/Divyeh-IIITD/MiniDrive.git"
BRANCH=${1:-main}

echo "Using remote: $REMOTE_URL"

# initialize repo if needed
if [ ! -d .git ]; then
  git init
fi

# ensure remote is set
if git remote | grep -q '^origin$'; then
  git remote set-url origin "$REMOTE_URL"
else
  git remote add origin "$REMOTE_URL"
fi

git add .
git commit -m "Day 1: scaffold coordinator, storage-node, frontend; chunking and replication modules" || echo "No changes to commit"

git branch -M "$BRANCH"
git push -u origin "$BRANCH"

echo "Pushed to $REMOTE_URL on branch $BRANCH"
