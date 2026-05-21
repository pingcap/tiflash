# Local defaults for ./compose.sh and ./run.sh.
# Use ${VAR:-default}; command-line values take precedence over these defaults.

export LOCAL_PD_BIN_DIR="${LOCAL_PD_BIN_DIR:-}"
export LOCAL_TIKV_BIN_DIR="${LOCAL_TIKV_BIN_DIR:-}"
export LOCAL_TIDB_BIN_DIR="${LOCAL_TIDB_BIN_DIR:-}"

export HUB_ADDR="${HUB_ADDR:-us-docker.pkg.dev/pingcap-testing-account/tidbx}"

export PD_BRANCH="${PD_BRANCH:-master-next-gen}"
if [[ "${PD_BRANCH}" == "master" ]]; then
  export PD_BRANCH="master-next-gen"
fi

export TIKV_BRANCH="${TIKV_BRANCH:-cloud-engine-next-gen}"
if [[ "${TIKV_BRANCH}" == "cloud-engine" ]]; then
  export TIKV_BRANCH="cloud-engine-next-gen"
fi

export TIDB_BRANCH="${TIDB_BRANCH:-master-next-gen}"
if [[ "${TIDB_BRANCH}" == "master" ]]; then
  export TIDB_BRANCH="master-next-gen"
fi

export PD_IMAGE="${PD_IMAGE:-${HUB_ADDR}/tikv/pd/image:${PD_BRANCH}}"
export TIKV_IMAGE="${TIKV_IMAGE:-${HUB_ADDR}/tikv/tikv/image:${TIKV_BRANCH}}"
export TIDB_IMAGE="${TIDB_IMAGE:-${HUB_ADDR}/pingcap/tidb/images/tidb-server:${TIDB_BRANCH}}"
