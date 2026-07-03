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

# Local defaults for ./compose.sh and ./run.sh.
# Use ${VAR:-default}; command-line values take precedence over these defaults.

export NEXT_GEN_COLUMNAR_ONLY="${NEXT_GEN_COLUMNAR_ONLY:-true}"

export LOCAL_PD_BIN_DIR="${LOCAL_PD_BIN_DIR:-}"
export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-}"
export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-}"

# Examples:
# export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-/path/to/tikv/target/release}"
# export LOCAL_PD_BIN_DIR="${LOCAL_PD_BIN_DIR:-/path/to/pd/bin}"
# export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-/path/to/tidb/bin}"

export HUB_ADDR="${HUB_ADDR:-us-docker.pkg.dev/pingcap-testing-account/tidbx}"

export PD_BRANCH="${PD_BRANCH:-master-nextgen}"
if [[ "${PD_BRANCH}" == "master" ]]; then
  export PD_BRANCH="master-nextgen"
fi

export TIKV_BRANCH="${TIKV_BRANCH:-cloud-engine-nextgen}"
if [[ "${TIKV_BRANCH}" == "cloud-engine" ]]; then
  export TIKV_BRANCH="cloud-engine-nextgen"
fi

export TIDB_BRANCH="${TIDB_BRANCH:-master-nextgen}"
if [[ "${TIDB_BRANCH}" == "master" ]]; then
  export TIDB_BRANCH="master-nextgen"
fi

export PD_IMAGE="${PD_IMAGE:-${HUB_ADDR}/tikv/pd/image:${PD_BRANCH}}"
export TIKV_IMAGE="${TIKV_IMAGE:-${HUB_ADDR}/tikv/tikv/image:${TIKV_BRANCH}}"
export TIDB_IMAGE="${TIDB_IMAGE:-${HUB_ADDR}/pingcap/tidb/images/tidb-server:${TIDB_BRANCH}}"

# Expose tidb0 MySQL port on host. Empty means no host port mapping.
# export EXPOSE_TIDB_PORT="${EXPOSE_TIDB_PORT:-4000}"
export EXPOSE_TIDB_PORT="${EXPOSE_TIDB_PORT:-}"
