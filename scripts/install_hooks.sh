#!/bin/sh
# Installs the repo's git hooks: gitleaks + brand-name check (pre-commit), trailer strip + brand check (commit-msg).
set -e
root=$(git rev-parse --show-toplevel)
install -m 0755 "$root/scripts/hooks/pre-commit" "$root/scripts/hooks/commit-msg" "$root/.git/hooks/"
echo "hooks installed: pre-commit, commit-msg"
