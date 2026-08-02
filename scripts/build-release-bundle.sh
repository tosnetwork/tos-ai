#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 OUTPUT_DIRECTORY VERSION" >&2
  exit 2
fi

output_directory=$1
version=$2
if [[ ! $version =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]]; then
  echo "invalid release version" >&2
  exit 2
fi

repository=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
revision=$(git -C "$repository" rev-parse --verify HEAD)
source_epoch=${SOURCE_DATE_EPOCH:-$(git -C "$repository" show -s --format=%ct HEAD)}
if [[ ! $revision =~ ^[0-9a-f]{40}$ || ! $source_epoch =~ ^[0-9]{1,12}$ ]]; then
  echo "invalid release source metadata" >&2
  exit 1
fi

goos=$(GOWORK=off go env GOOS)
goarch=$(GOWORK=off go env GOARCH)
bundle_name="tos-ai-${version}-${goos}-${goarch}"
artifact="${bundle_name}.tar.gz"
mkdir -p "$output_directory"
if [[ -e "$output_directory/$artifact" || -e "$output_directory/$artifact.sha256" ]]; then
  echo "release output already exists" >&2
  exit 1
fi

release_tmp=$(mktemp -d)
trap 'rm -rf -- "$release_tmp"' EXIT
release_root="$release_tmp/$bundle_name"
mkdir -p "$release_root/bin" "$release_root/deploy/systemd" "$release_root/examples" "$release_root/docs"

targets=(tos-ai-cli tos-ai-edge tos-ai-worker)
for target in "${targets[@]}"; do
  (
    cd "$repository"
    GOWORK=off go build -mod=readonly -trimpath -buildvcs=false \
      -ldflags=-buildid= -o "$release_root/bin/$target" "./cmd/$target"
  )
done

cp "$repository/LICENSE" "$release_root/LICENSE"
cp "$repository/deploy/systemd/README.md" "$release_root/deploy/systemd/README.md"
cp "$repository/deploy/systemd/tos-ai-edge.service" "$release_root/deploy/systemd/tos-ai-edge.service"
cp "$repository/deploy/systemd/tos-ai-worker.service" "$release_root/deploy/systemd/tos-ai-worker.service"
for example in "$repository"/docs/*-config.example.json; do
  cp "$example" "$release_root/examples/$(basename "$example")"
done
cp "$repository/docs/production-edge-deployment.md" "$release_root/docs/production-edge-deployment.md"
cp "$repository/docs/software-update-operations.md" "$release_root/docs/software-update-operations.md"

printf '{"architecture":"%s","goos":"%s","repository":"tos-ai","revision":"%s","sourceDateEpoch":%s,"version":"%s"}\n' \
  "$goarch" "$goos" "$revision" "$source_epoch" "$version" \
  > "$release_root/RELEASE-METADATA.json"
(
  cd "$release_root"
	find . -type f ! -name SHA256SUMS -printf '%P\0' \
	  | LC_ALL=C sort -z \
	  | xargs -0 sha256sum > SHA256SUMS
)

tar --sort=name --mtime="@$source_epoch" --owner=0 --group=0 --numeric-owner \
  --mode='u+rwX,go+rX,go-w' -C "$release_tmp" -cf - "$bundle_name" \
  | gzip -n > "$output_directory/$artifact"
(
  cd "$output_directory"
  sha256sum "$artifact" > "$artifact.sha256"
)
printf '%s\n' "$output_directory/$artifact"
