#!/usr/bin/env bash
set -euo pipefail

MODE=${1:?"Mode (github-release|nuget-feed) is required"}
CURRENT_VERSION=${2:?"Current version is required"}

# Goes to stderr: stdout of the fetch functions is the version they return.
log() {
  echo "[version-check] $*" >&2
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "Missing required tool: $1"
    exit 1
  fi
}

require_tool curl
require_tool jq

# Prints the current version when the list (one version per line) already has it, otherwise
# the most recent one: the first or the last line, as the second argument says.
pick_version() {
  local versions="$1"
  local most_recent="$2"

  if [[ -z "${versions}" ]]; then
    return 0
  fi

  # NuGet versions are case-insensitive; the feed returns them lowercased.
  local wanted known
  wanted=$(tr '[:upper:]' '[:lower:]' <<<"${CURRENT_VERSION}")
  known=$(tr '[:upper:]' '[:lower:]' <<<"${versions}")

  if grep -Fxq -- "${wanted}" <<<"${known}"; then
    echo "${CURRENT_VERSION}"
  elif [[ "${most_recent}" == "first" ]]; then
    head -n 1 <<<"${versions}"
  else
    tail -n 1 <<<"${versions}"
  fi
}

fetch_github_release_version() {
  local repo="${GITHUB_REPOSITORY:-}"
  local token="${GITHUB_PUBLISH_TOKEN:-${GITHUB_TOKEN:-}}"
  if [[ -z "${repo}" ]]; then
    log "GITHUB_REPOSITORY is not set; cannot inspect releases"
    return 0
  fi

  local api="https://api.github.com/repos/${repo}/releases?per_page=100"
  local curl_args=(-fsSL -H "Accept: application/vnd.github+json")
  if [[ -n "${token}" ]]; then
    curl_args+=(-H "Authorization: Bearer ${token}")
  fi

  local response
  if ! response=$(curl "${curl_args[@]}" "${api}" 2>/dev/null); then
    log "Failed to query GitHub releases"
    return 0
  fi

  # The repository carries several release lines (3.x, 7.x), so "the latest release" says nothing:
  # report the current version when it is already released, otherwise the most recent release.
  local tags
  if ! tags=$(jq -r '.[] | .tag_name // empty' <<<"${response}" 2>/dev/null); then
    log "Unable to parse GitHub release response"
    return 0
  fi

  pick_version "$(grep '^v' <<<"${tags}" | sed 's/^v//' || true)" first
}

fetch_nuget_feed_version() {
  local feed="${GITHUB_FEED:-}"
  local package_id="${PACKAGE_ID:-}"
  local token="${GITHUB_PUBLISH_TOKEN:-${GITHUB_TOKEN:-}}"

  if [[ -z "${feed}" || -z "${package_id}" ]]; then
    log "GITHUB_FEED or PACKAGE_ID missing; cannot query feed"
    return 0
  fi

  local curl_args=(-fsSL -H "Accept: application/json")
  if [[ -n "${token}" ]]; then
    local actor="${GITHUB_ACTOR:-github-actions[bot]}"
    local creds
    creds=$(printf "%s:%s" "${actor}" "${token}" | base64 | tr -d '\r\n')
    curl_args+=(-H "Authorization: Basic ${creds}")
  fi

  local catalog
  if ! catalog=$(curl "${curl_args[@]}" "${feed}" 2>/dev/null); then
    log "Failed to download NuGet service index"
    return 0
  fi

  local base
  base=$(jq -r '
    .resources[]
    | select(
        ((."@type" // []) | (if type=="array" then . else [.] end))
        | map(tostring)
        | any(test("PackageBaseAddress"))
      )
    | ."@id"
  ' <<<"${catalog}" 2>/dev/null | head -n1)
  if [[ -z "${base}" ]]; then
    log "NuGet service index does not expose PackageBaseAddress"
    return 0
  fi

  base=${base%/}/
  local package_lower
  package_lower=$(echo "${package_id}" | tr '[:upper:]' '[:lower:]')
  local index_url="${base}${package_lower}/index.json"

  local index
  if ! index=$(curl "${curl_args[@]}" "${index_url}" 2>/dev/null); then
    log "Failed to download package index from feed"
    return 0
  fi

  # The feed carries several release lines (3.x, 7.x), so "the last version" says nothing:
  # report the current version when it is already published, otherwise the last one in the index.
  local versions
  if ! versions=$(jq -r '.versions[]?' <<<"${index}" 2>/dev/null); then
    log "Unable to parse package index"
    return 0
  fi

  pick_version "${versions}" last
}

fetch_remote_version() {
  case "${MODE}" in
    github-release)
      fetch_github_release_version
      ;;
    nuget-feed)
      fetch_nuget_feed_version
      ;;
    *)
      log "Unknown mode: ${MODE}"
      return 1
      ;;
  esac
}

REMOTE_VERSION=$(fetch_remote_version || true)

if [[ -z "${REMOTE_VERSION}" ]]; then
  log "No remote version detected; skipping comparison."
  exit 0
fi

if [[ "${REMOTE_VERSION}" == "${CURRENT_VERSION}" ]]; then
  log "Version ${CURRENT_VERSION} is already published; please bump <Version> in the project."
  exit 1
fi

log "Version ${CURRENT_VERSION} is not published yet (most recent remote version: ${REMOTE_VERSION}); proceeding."
