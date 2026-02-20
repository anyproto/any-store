#!/usr/bin/env bash
#
# Switch benchmark/go.mod from local replace to a released version.
#
# Usage:
#   ./use-release.sh                   # latest on default branch
#   ./use-release.sh v0.3.0            # specific tag
#   ./use-release.sh <commit-hash>     # specific commit
#
# To restore local development mode:
#   git checkout go.mod go.sum
#
set -euo pipefail
cd "$(dirname "$0")"

VERSION="${1:-latest}"

# Drop the replace directive
sed -i '/^replace github.com\/anyproto\/any-store/d' go.mod

# Drop the old v0.0.0 require so go get can resolve cleanly
go mod edit -droprequire github.com/anyproto/any-store

# Bypass proxy caches for private/fast-moving repos
export GOPRIVATE=github.com/anyproto/any-store
export GONOSUMCHECK=github.com/anyproto/any-store

go get "github.com/anyproto/any-store@${VERSION}"
go mod tidy

echo ""
echo "Switched to:"
grep 'github.com/anyproto/any-store ' go.mod | grep require
echo ""
echo "To restore local mode:  git checkout go.mod go.sum"
