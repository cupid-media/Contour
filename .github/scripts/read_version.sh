#!/usr/bin/env bash
set -euo pipefail

PROJECT_FILE=${PROJECT_FILE:?PROJECT_FILE env variable is required}
MODE=${1:-basic}

VERSION=$(grep -oP '<Version>\K[^<]+' "${PROJECT_FILE}" || true)
if [[ -z "${VERSION}" ]]; then
  echo "Unable to read <Version> from ${PROJECT_FILE}" >&2
  exit 1
fi

echo "Detected version: ${VERSION}"

if [[ "${MODE}" == "publish" ]]; then
  echo "new_version=${VERSION}" >> "$GITHUB_OUTPUT"
  echo "effective_version=${VERSION}" >> "$GITHUB_OUTPUT"
else
  echo "version=${VERSION}" >> "$GITHUB_OUTPUT"
fi
