#!/bin/bash
#
# Releases <package> at specified <version>.
#
# USAGE
#
#     ./release.sh dagster-anthropic 0.0.2
#

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <library> <version>"
    exit 1
fi

PACKAGE="$1"
VERSION="$2"

if [ ! -d "libraries/${PACKAGE}" ]; then
  echo "ERROR: Package libraries/${PACKAGE} does not exist"
  exit 1
fi

if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: ${VERSION} does not match pattern X.X.X"
  exit 1
fi

sed -i '' 's/version = "[^"]*"/version = "'"$VERSION"'"/' "libraries/${PACKAGE}/pyproject.toml"

git add "libraries/${PACKAGE}/pyproject.toml"
git commit -m "Release $PACKAGE $VERSION"

RELEASE_TAG="${PACKAGE//-/_}-${VERSION}"

echo "Release ${RELEASE_TAG}?"
read -r -p "Press enter to continue..."

git tag "$RELEASE_TAG"
git push origin "$RELEASE_TAG"
