#!/usr/bin/env bash
set -euo pipefail

DEFAULT_BRANCH=${DEFAULT_BRANCH:-main}
PROJECT_FILE=${PROJECT_FILE:-./Sources/Contour/Contour.csproj}
PROJECT_FILE_STRIPPED=${PROJECT_FILE#./}
EVENT_NAME=${GITHUB_EVENT_NAME:-push}

# Optional hints for diff calculation
EVENT_BEFORE=${EVENT_BEFORE:-}
PULL_BASE_SHA=${PULL_BASE_SHA:-}
PULL_HEAD_SHA=${PULL_HEAD_SHA:-}

run_git_fetch_default_branch() {
  git fetch origin "${DEFAULT_BRANCH}" --quiet
}

diff_from_default_branch() {
  local head="$1"
  run_git_fetch_default_branch
  local base
  base=$(git merge-base "origin/${DEFAULT_BRANCH}" "${head}" || true)
  if [[ -z "${base}" ]]; then
    base=$(git rev-list --max-parents=0 "${head}" | tail -n 1 || true)
  fi
  if [[ -z "${base}" ]]; then
    base="${head}"
  fi
  echo "${base}..${head}"
}

# Determine diff range based on event type
if [[ "${EVENT_NAME}" == "push" ]]; then
  AFTER=${GITHUB_SHA:?GITHUB_SHA is required}
  if [[ -z "${EVENT_BEFORE}" || "${EVENT_BEFORE}" =~ ^0+$ ]]; then
    DIFF_RANGE=$(diff_from_default_branch "${AFTER}")
  else
    DIFF_RANGE="${EVENT_BEFORE}..${AFTER}"
  fi
elif [[ "${EVENT_NAME}" == "pull_request" ]]; then
  BEFORE=${PULL_BASE_SHA:-}
  AFTER=${PULL_HEAD_SHA:-}
  if [[ -n "${BEFORE}" && -n "${AFTER}" ]]; then
    DIFF_RANGE="${BEFORE}..${AFTER}"
  else
    BASE=$(git merge-base origin/${DEFAULT_BRANCH} HEAD)
    DIFF_RANGE="${BASE}..HEAD"
  fi
else
  run_git_fetch_default_branch
  BASE=$(git merge-base origin/${DEFAULT_BRANCH} HEAD)
  DIFF_RANGE="${BASE}..HEAD"
fi

SOURCE_CHANGED=false
DOCS_CHANGED=false
VERSION_CHANGED=false

if [[ "$(git rev-list --count HEAD)" -eq 1 ]]; then
  SOURCE_CHANGED=true
else
  FILES=$(git diff --name-only "${DIFF_RANGE}" || true)

  if [[ -n "${FILES}" ]]; then
    if echo "${FILES}" | grep -Fxq "${PROJECT_FILE_STRIPPED}"; then
      if git diff "${DIFF_RANGE}" -- "${PROJECT_FILE_STRIPPED}" | grep -qE '^[+-].*<Version>'; then
        VERSION_CHANGED=true
      fi
    fi

    if echo "${FILES}" | grep -qE '(^docs/)|(\.(md|mdx|txt)$)'; then
      DOCS_CHANGED=true
    fi

    if echo "${FILES}" | grep -vE '(^docs/)|(\.(md|mdx|txt)$)' | grep -q .; then
      SOURCE_CHANGED=true
    fi
  fi
fi

echo "source_changed=${SOURCE_CHANGED}" >> "$GITHUB_OUTPUT"
echo "docs_changed=${DOCS_CHANGED}" >> "$GITHUB_OUTPUT"
echo "version_changed=${VERSION_CHANGED}" >> "$GITHUB_OUTPUT"

BRANCH_NAME=${GITHUB_REF_NAME:-HEAD}
RUN_NUMBER=${GITHUB_RUN_NUMBER:-0}
RUN_ATTEMPT=${GITHUB_RUN_ATTEMPT:-1}
IDENTIFIER="${RUN_NUMBER}.${RUN_ATTEMPT}"
SUFFIX=""
BRANCH_KIND="main"

if [[ "${BRANCH_NAME}" == "prerelease" ]]; then
  SUFFIX="-pr${IDENTIFIER}"
  BRANCH_KIND="prerelease"
elif [[ "${BRANCH_NAME}" != "main" ]]; then
  SANITIZED=$(echo "${BRANCH_NAME}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-' | sed 's/^-*//;s/-*$//')
  if [[ -z "${SANITIZED}" ]]; then
    SANITIZED="feature"
  fi
  SUFFIX="-${SANITIZED}${IDENTIFIER}"
  BRANCH_KIND="feature"
fi

echo "branch_name=${BRANCH_NAME}" >> "$GITHUB_OUTPUT"
echo "branch_kind=${BRANCH_KIND}" >> "$GITHUB_OUTPUT"
echo "suffix_identifier=${IDENTIFIER}" >> "$GITHUB_OUTPUT"
echo "version_suffix=${SUFFIX}" >> "$GITHUB_OUTPUT"
