#!/usr/bin/env bash

set -euo pipefail

hub_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -f "${hub_dir}/Cargo.toml" ]]; then
    echo "Cannot find the Hub Cargo workspace." >&2
    exit 1
fi

cd "${hub_dir}"
cargo run --locked --package gen-proxy-ffi
