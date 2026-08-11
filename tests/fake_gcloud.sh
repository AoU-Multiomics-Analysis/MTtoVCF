#!/usr/bin/env bash
set -euo pipefail
test "$1" = "storage"
test "$2" = "cp"
cp "$3" "$4"
