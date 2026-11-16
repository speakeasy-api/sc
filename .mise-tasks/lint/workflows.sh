#!/usr/bin/env bash

#MISE description="Audit the GitHub Actions workflows with zizmor"

set -euo pipefail

exec zizmor --pedantic .github
