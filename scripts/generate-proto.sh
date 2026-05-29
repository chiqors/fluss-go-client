#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="${PATH}:$(go env GOPATH)/bin"

cd "${ROOT_DIR}"
mkdir -p internal/proto/gen/fluss
buf generate
