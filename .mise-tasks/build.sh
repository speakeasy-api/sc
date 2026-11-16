#!/usr/bin/env bash

#MISE description="Build every package and vet it"

set -euo pipefail

go build ./...
exec go vet ./...
