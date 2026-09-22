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

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install: https://docs.astral.sh/uv/" >&2
  exit 1
fi

uv sync

.venv/bin/isort --profile black *.py
.venv/bin/black *.py

.venv/bin/generate-dashboard \
  -o tiflash_summary.json \
  tiflash_summary.dashboard.py

# Checksum path prefix matches CSE style (repo-root relative).
(
  cd ../..
  sha256sum ./metrics/grafana/tiflash_summary.json > metrics/grafana/tiflash_summary.json.sha256
)

echo "Generated $(pwd)/tiflash_summary.json"
