#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd "$(dirname "$0")"

if ! command -v jsonnet >/dev/null 2>&1; then
  go install github.com/google/go-jsonnet/cmd/jsonnet@latest
fi

# Same fork as TiDB generate_json.sh (addOverride / table helpers).
# TODO: migrate to https://github.com/grafana/grafonnet when ready.
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

git clone --depth 1 https://github.com/nolouch/grafonnet-lib.git "$TMP_DIR/grafonnet-lib" >/dev/null 2>&1

export JSONNET_PATH="$TMP_DIR/grafonnet-lib"
# go-jsonnet manifests JSON with a fixed 3-space indent; reformat to 2 spaces.
jsonnet tiflash_summary.jsonnet | python3 -c '
import json, sys
json.dump(json.load(sys.stdin), sys.stdout, indent=2, ensure_ascii=False)
sys.stdout.write("\n")
' > tiflash_summary.json

echo "Generated $(pwd)/tiflash_summary.json"
