#!/bin/bash
# Version bump script for Thunder library.
# Copyright (c) YOAB. All rights reserved.
#
# Usage:
#   ./scripts/bump-version.sh major    # 0.3.0 -> 1.0.0
#   ./scripts/bump-version.sh minor    # 0.3.0 -> 0.4.0
#   ./scripts/bump-version.sh patch    # 0.3.0 -> 0.3.1
#   ./scripts/bump-version.sh 1.2.3    # Set explicit version

set -e

CARGO_TOML="Cargo.toml"

if [ ! -f "$CARGO_TOML" ]; then
    echo "Error: $CARGO_TOML not found. Run from repository root."
    exit 1
fi

# Extract current version from Cargo.toml
CURRENT_VERSION=$(grep -m1 '^version = ' "$CARGO_TOML" | sed 's/version = "\(.*\)"/\1/')

if [ -z "$CURRENT_VERSION" ]; then
    echo "Error: Could not extract version from $CARGO_TOML"
    exit 1
fi

echo "Current version: $CURRENT_VERSION"

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Determine new version based on argument
case "$1" in
    major)
        NEW_VERSION="$((MAJOR + 1)).0.0"
        ;;
    minor)
        NEW_VERSION="${MAJOR}.$((MINOR + 1)).0"
        ;;
    patch)
        NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))"
        ;;
    "")
        echo "Usage: $0 <major|minor|patch|X.Y.Z>"
        echo "  major  - Bump major version (breaking changes)"
        echo "  minor  - Bump minor version (new features)"
        echo "  patch  - Bump patch version (bug fixes)"
        echo "  X.Y.Z  - Set explicit version"
        exit 1
        ;;
    *)
        # Validate explicit version format
        if [[ ! "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "Error: Invalid version format. Use X.Y.Z (e.g., 1.2.3)"
            exit 1
        fi
        NEW_VERSION="$1"
        ;;
esac

echo "New version: $NEW_VERSION"

# Update Cargo.toml
sed -i "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" "$CARGO_TOML"

# Verify the change
UPDATED_VERSION=$(grep -m1 '^version = ' "$CARGO_TOML" | sed 's/version = "\(.*\)"/\1/')

if [ "$UPDATED_VERSION" != "$NEW_VERSION" ]; then
    echo "Error: Version update failed"
    exit 1
fi

echo "Successfully updated version from $CURRENT_VERSION to $NEW_VERSION"

# Update Cargo.lock if it exists
if [ -f "Cargo.lock" ]; then
    cargo generate-lockfile 2>/dev/null || true
fi

echo ""
echo "Next steps:"
echo "  1. Review changes: git diff"
echo "  2. Commit: git commit -am 'chore: bump version to $NEW_VERSION'"
echo "  3. Tag: git tag v$NEW_VERSION"
echo "  4. Push: git push && git push --tags"
