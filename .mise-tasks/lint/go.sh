#!/usr/bin/env bash

#MISE description="Lint the Go code with golangci-lint"

set -euo pipefail

exec golangci-lint run ./...
