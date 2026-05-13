#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NGINX_BIN="${NGINX_BIN:-nginx}"

if ! command -v "${NGINX_BIN}" >/dev/null 2>&1; then
  echo "nginx binary not found on PATH. Install nginx or set NGINX_BIN to its full path." >&2
  exit 1
fi

mkdir -p "${PROJECT_ROOT}/tmp/nginx"

exec "${NGINX_BIN}" \
  -p "${PROJECT_ROOT}" \
  -c "${PROJECT_ROOT}/nginx.conf" \
  -g 'daemon off;'
