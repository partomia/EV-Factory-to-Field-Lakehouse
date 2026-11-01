#!/bin/sh
# Prints the EVX_BRAND_NAME value (from the environment, else the repo's .env) for the hooks to match on.
# Callers must never echo it: they only test whether it occurs.
if [ -n "${EVX_BRAND_NAME:-}" ]; then
  printf '%s' "$EVX_BRAND_NAME"
  exit 0
fi
root=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
[ -f "$root/.env" ] || exit 0
sed -n -E 's/^(export +)?EVX_BRAND_NAME=//p' "$root/.env" | tail -n 1 | sed -E 's/^["'\'']//; s/["'\'']$//; s/[[:space:]]+$//'
