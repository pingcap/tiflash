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

# TIFLASH_COLUMNAR unset or "false" -> tiflash under /tiflash.
# "true" -> columnar build under /tiflash-columnar when the binary exists.
tiflash_columnar="${TIFLASH_COLUMNAR:-false}"
tiflash_columnar_lower="$(echo "${tiflash_columnar}" | tr '[:upper:]' '[:lower:]')"

if [ "${tiflash_columnar_lower}" = "true" ] && [ -x /tiflash-columnar/tiflash ]; then
    export LD_LIBRARY_PATH=/tiflash-columnar
    # exec replaces this shell; lines below are not run on success.
    exec /tiflash-columnar/tiflash server "$@"
fi

# Reached only when columnar mode is off, or /tiflash-columnar/tiflash is missing.
export LD_LIBRARY_PATH=/tiflash
exec /tiflash/tiflash server "$@"
