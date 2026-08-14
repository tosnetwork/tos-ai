#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
	echo "usage: $0 TOOLCHAIN_ZIP OUTPUT_OCI_TAR" >&2
	exit 2
fi

toolchain_zip=$1
output_oci_tar=$2
expected_zip_sha256=fa1f92e41b70c6649bd78b1ab98b940f19adf3e71aeb0bcb5a177bcc25699df5
module_dir=golang.org/toolchain@v0.0.1-go1.26.5.linux-amd64
source_date_epoch=1785715200
expected_index_sha256=9624bca74096f810c5b24e489521dde124fadcfa1808581648b38bdc1ba1b105
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
build_dir=$(mktemp -d)
trap 'find "$build_dir" -depth -delete' EXIT HUP INT TERM

actual_zip_sha256=$(sha256sum "$toolchain_zip" | awk '{print $1}')
if [ "$actual_zip_sha256" != "$expected_zip_sha256" ]; then
	echo "toolchain ZIP SHA-256 mismatch" >&2
	exit 1
fi

mkdir "$build_dir/extracted" "$build_dir/context"
unzip -q "$toolchain_zip" -d "$build_dir/extracted"
mv "$build_dir/extracted/$module_dir" "$build_dir/context/go"
cp "$script_dir/Dockerfile" "$build_dir/context/Dockerfile"

SOURCE_DATE_EPOCH=$source_date_epoch docker buildx build \
	--platform linux/amd64 \
	--provenance=false \
	--sbom=false \
	--output "type=oci,dest=$output_oci_tar,rewrite-timestamp=true" \
	"$build_dir/context"

actual_index_sha256=$(tar -xOf "$output_oci_tar" index.json | sha256sum | awk '{print $1}')
if [ "$actual_index_sha256" != "$expected_index_sha256" ]; then
	echo "OCI index SHA-256 mismatch" >&2
	exit 1
fi
echo "sha256:$actual_index_sha256"
