#!/bin/bash
# Copyright 2025 PingCAP, Inc.
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
source ../docker/util.sh

function show_compose_help() {
  cat <<'EOF'
Usage: ./compose.sh [compose subcommand ...]

Wrapper for the next-gen fullstack test cluster. It selects compose yaml files
automatically, including Rocky Linux 9 and local binary overrides.

Examples:
  ./compose.sh up -d
  LOCAL_TIKV_BIN_DIR=/path/to/tikv/output ./compose.sh up -d
  ./compose.sh stop
  ./compose.sh down
  ./compose.sh ps
  ./compose.sh exec tiflash-cn0 bash

Defaults:
  Edit ./_env.sh using ${VAR:-default}. Command-line values take precedence.

Environment:
  LOCAL_PD_BIN_DIR    directory containing pd-server
  LOCAL_TIKV_BIN_DIR  directory containing tikv-server and tikv-worker
  LOCAL_TIDB_BIN_DIR  directory containing tidb-server
  PD_IMAGE, TIKV_IMAGE, TIDB_IMAGE, HUB_ADDR, PD_BRANCH, TIKV_BRANCH, TIDB_BRANCH
EOF
}

if [[ $# -eq 0 || "${1}" == "-h" || "${1}" == "--help" ]]; then
  show_compose_help
  exit 0
fi

check_docker_compose
# shellcheck source=/dev/null
source ./_env.sh

validate_binaries=true
case "${1}" in
  stop|down|ps|logs|exec|config|top|kill|pause|unpause|port|images|cp|events|attach)
    validate_binaries=false
    ;;
esac

setup_next_gen_compose_files COMPOSE_FILES "${validate_binaries}"

case "${1}" in
  up|start|create|run|restart)
    prepare_next_gen_data_dirs
    ;;
esac

echo "Running: ${COMPOSE} ${COMPOSE_FILES[*]} $*"
${COMPOSE} "${COMPOSE_FILES[@]}" "$@"
