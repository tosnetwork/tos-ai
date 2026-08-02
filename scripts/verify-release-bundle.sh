#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 && $# -ne 3 ]]; then
  echo "usage: $0 BUNDLE [ED25519_PUBLIC_KEY_PEM SIGNATURE]" >&2
  exit 2
fi

bundle=$1
if [[ ! -f $bundle ]]; then
  echo "release bundle does not exist" >&2
  exit 1
fi
bundle_size=$(stat -c %s "$bundle")
if (( bundle_size <= 0 || bundle_size > 1073741824 )); then
  echo "release bundle exceeds the compressed-size limit" >&2
  exit 1
fi
bundle_directory=$(cd "$(dirname "$bundle")" && pwd)
bundle_basename=$(basename "$bundle")
if [[ -f "$bundle.sha256" ]]; then
  (cd "$bundle_directory" && sha256sum -c "$bundle_basename.sha256")
fi

entry_list=$(mktemp)
verbose_list=$(mktemp)
verify_tmp=$(mktemp -d)
trap 'rm -f -- "$entry_list" "$verbose_list"; rm -rf -- "$verify_tmp"' EXIT
tar -tzf "$bundle" > "$entry_list"
tar -tvzf "$bundle" > "$verbose_list"
entry_count=$(wc -l < "$entry_list")
if (( entry_count <= 0 || entry_count > 4096 )); then
  echo "release bundle exceeds the entry-count limit" >&2
  exit 1
fi
if [[ ! -s $entry_list ]] || awk '
  /^\// { bad=1 }
  /(^|\/)\.\.($|\/)/ { bad=1 }
  END { exit bad ? 0 : 1 }
' "$entry_list"; then
  echo "release bundle contains an unsafe path" >&2
  exit 1
fi
if [[ -n $(sort "$entry_list" | uniq -d) ]]; then
  echo "release bundle contains duplicate paths" >&2
  exit 1
fi
if awk '
  substr($1,1,1) != "-" && substr($1,1,1) != "d" { found=1 }
  END { exit found ? 0 : 1 }
' "$verbose_list"; then
	echo "release bundle contains a special entry" >&2
	exit 1
fi
if ! awk '
  $3 !~ /^[0-9]+$/ || $3 > 4294967296 { bad=1 }
  { total += $3; if (total > 4294967296) bad=1 }
  END { exit bad ? 1 : 0 }
' "$verbose_list"; then
  echo "release bundle exceeds the uncompressed-size limit" >&2
  exit 1
fi
tar --no-same-owner --no-same-permissions -xzf "$bundle" -C "$verify_tmp"
mapfile -t release_roots < <(find "$verify_tmp" -mindepth 1 -maxdepth 1 -type d -print)
if [[ ${#release_roots[@]} -ne 1 ]] || find "$verify_tmp" -mindepth 1 -maxdepth 1 ! -type d -print -quit | grep -q .; then
  echo "release bundle must contain exactly one top-level directory" >&2
  exit 1
fi
mapfile -t checksum_files < <(find "$verify_tmp" -type f -name SHA256SUMS -print)
if [[ ${#checksum_files[@]} -ne 1 ]]; then
  echo "release bundle must contain exactly one SHA256SUMS" >&2
  exit 1
fi
release_root=$(dirname "${checksum_files[0]}")
if [[ $release_root != "${release_roots[0]}" ]]; then
  echo "SHA256SUMS is not at the release root" >&2
  exit 1
fi
if ! awk '
  !/^[0-9a-f]{64}  [A-Za-z0-9][A-Za-z0-9._\/-]*$/ { bad=1 }
  substr($0,67) ~ /(^|\/)\.\.($|\/)/ { bad=1 }
  END { exit bad ? 1 : 0 }
' "${checksum_files[0]}"; then
  echo "release checksum manifest is unsafe" >&2
  exit 1
fi
declared_files="$verify_tmp/declared-files"
actual_files="$verify_tmp/actual-files"
awk '{ print substr($0,67) }' "${checksum_files[0]}" | LC_ALL=C sort > "$declared_files"
if [[ -n $(uniq -d "$declared_files") ]]; then
  echo "release checksum manifest contains duplicate paths" >&2
  exit 1
fi
(cd "$release_root" && find . -type f ! -name SHA256SUMS -printf '%P\n' | LC_ALL=C sort) > "$actual_files"
if ! cmp -s "$declared_files" "$actual_files"; then
  echo "release bundle contains an unchecksummed or missing file" >&2
  exit 1
fi
(cd "$release_root" && sha256sum -c SHA256SUMS)

if [[ $# -eq 3 ]]; then
  public_key=$2
  signature=$3
  if [[ ! -f $public_key || ! -f $signature ]]; then
    echo "release signature material does not exist" >&2
    exit 1
  fi
  openssl pkeyutl -verify -pubin -inkey "$public_key" -rawin \
    -in "$bundle" -sigfile "$signature"
fi
