#!/usr/bin/env bash
set -euo pipefail

repository=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_tmp=$(mktemp -d)
trap 'rm -rf -- "$test_tmp"' EXIT
mkdir -p "$test_tmp/first" "$test_tmp/second"

SOURCE_DATE_EPOCH=1700000000 "$repository/scripts/build-release-bundle.sh" \
  "$test_tmp/first" local-gate >/dev/null
SOURCE_DATE_EPOCH=1700000000 "$repository/scripts/build-release-bundle.sh" \
  "$test_tmp/second" local-gate >/dev/null
first_bundle=$(find "$test_tmp/first" -maxdepth 1 -type f -name '*.tar.gz' -print)
second_bundle=$(find "$test_tmp/second" -maxdepth 1 -type f -name '*.tar.gz' -print)
cmp "$first_bundle" "$second_bundle"

openssl genpkey -algorithm ED25519 -out "$test_tmp/release-private.pem" >/dev/null 2>&1
openssl pkey -in "$test_tmp/release-private.pem" -pubout \
  -out "$test_tmp/release-public.pem" >/dev/null 2>&1
openssl pkeyutl -sign -inkey "$test_tmp/release-private.pem" -rawin \
  -in "$first_bundle" -out "$test_tmp/release.sig"
"$repository/scripts/verify-release-bundle.sh" "$first_bundle" \
  "$test_tmp/release-public.pem" "$test_tmp/release.sig" >/dev/null

cp "$test_tmp/release.sig" "$test_tmp/tampered.sig"
printf 'x' >> "$test_tmp/tampered.sig"
if "$repository/scripts/verify-release-bundle.sh" "$first_bundle" \
  "$test_tmp/release-public.pem" "$test_tmp/tampered.sig" >/dev/null 2>&1; then
  echo "tampered release signature was accepted" >&2
  exit 1
fi

mkdir -p "$test_tmp/unlisted"
tar -xzf "$first_bundle" -C "$test_tmp/unlisted"
release_root=$(find "$test_tmp/unlisted" -mindepth 1 -maxdepth 1 -type d -print)
printf 'not checksummed\n' > "$release_root/UNLISTED"
tar -czf "$test_tmp/unlisted.tar.gz" -C "$test_tmp/unlisted" "$(basename "$release_root")"
if "$repository/scripts/verify-release-bundle.sh" "$test_tmp/unlisted.tar.gz" >/dev/null 2>&1; then
  echo "unchecksummed release file was accepted" >&2
  exit 1
fi

mkdir -p "$test_tmp/symlink/root"
ln -s /etc/passwd "$test_tmp/symlink/root/unsafe-link"
tar -czf "$test_tmp/symlink.tar.gz" -C "$test_tmp/symlink" root
if "$repository/scripts/verify-release-bundle.sh" "$test_tmp/symlink.tar.gz" >/dev/null 2>&1; then
  echo "release symbolic link was accepted" >&2
  exit 1
fi
