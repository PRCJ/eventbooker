#!/usr/bin/env bash
# One-command on-sale stampede.
# Usage: ./burst.sh BASE_URL [--hot N] [--wide-seats N] [--wide-users N]
set -euo pipefail
cd "$(dirname "$0")"
if [[ $# -lt 1 ]]; then
	echo "usage: ./burst.sh BASE_URL [--hot N] [--wide-seats N] [--wide-users N]" >&2
	exit 2
fi
URL="$1"
shift
if [[ -x ./bin/burst ]]; then
	exec ./bin/burst --url "$URL" "$@"
fi
if command -v go >/dev/null 2>&1 && [[ -f go.mod ]]; then
	exec go run ./cmd/burst --url "$URL" "$@"
fi
exec python3 ./burst.py --url "$URL" "$@"
