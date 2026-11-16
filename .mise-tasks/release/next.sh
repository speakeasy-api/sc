#!/usr/bin/env bash

#MISE description="Print the next release version (without the v): patch, minor, or an exact 0.x.y version"
#USAGE arg "<bump>" help="patch, minor, or an exact version such as 0.4.0"

set -euo pipefail

bump="${usage_bump:-${1:-}}"

# Stable 0.x tags only; sc never has a 1.0 release.
latest="$(git tag -l 'v0.*' | grep -E '^v0\.[0-9]+\.[0-9]+$' | sort -V | tail -n 1 || true)"
latest="${latest#v}"
latest="${latest:-0.0.0}"
IFS=. read -r major minor patch <<<"$latest"

case "$bump" in
  patch) next="$major.$minor.$((patch + 1))" ;;
  minor) next="$major.$((minor + 1)).0" ;;
  *) next="${bump#v}" ;;
esac

if ! [[ "$next" =~ ^0\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: $next is not a 0.x.y version; sc stays on 0.x.y, so use minor for breaking changes" >&2
  exit 1
fi

if [ "$next" = "$latest" ] || [ "$(printf '%s\n%s\n' "$latest" "$next" | sort -V | tail -n 1)" != "$next" ]; then
  echo "error: $next is not newer than the latest release, $latest" >&2
  exit 1
fi

echo "$next"
