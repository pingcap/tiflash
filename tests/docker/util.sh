#!/bin/bash
# Copyright 2023 PingCAP, Inc.
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


function show_env() {
  set +e
  pwd
  df -h
  grep ^ /sys/block/*/queue/rotational

  cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c
  lscpu
  cat /proc/meminfo
  uname -a
  hostname
  lsmod
  dmidecode | grep 'Product Name'
  free -mh
  cat /proc/loadavg
  ldd --version

  set -e
}

function wait_env() {
  local timeout='200'
  local failed='true'

  echo "=> wait for env available"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if [[ -n $(cat ./log/tidb0/tidb.log | grep "server is running MySQL protocol") && \
          -n $(cat ./log/tiflash/tiflash.log | grep "Start to wait for terminal signal") ]]; then
        local failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

function wait_tiflash_env() {
  local timeout='200'
  local failed='true'

  echo "=> wait for env available"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if [[ -n $(cat ./log/tiflash/tiflash.log | grep "Start to wait for terminal signal") ]]; then
        local failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

function wait_next_gen_env() {
  local timeout='200'
  local failed='true'

  echo "=> wait for env available"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if [[ -n $(cat ./log/tidb0/tidb.log | grep "server is running MySQL protocol") && \
          -n $(cat ./log/tiflash-wn0/tiflash.log | grep "Start to wait for terminal signal") && \
          -n $(cat ./log/tiflash-cn0/tiflash.log | grep "Start to wait for terminal signal") ]]; then
        local failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

function set_branch() {
  # XYZ_BRANCH: pd/tikv/tidb hash, default to `master`
  # BRANCH:     hash short cut, default to `master`
  if [ -n "$BRANCH" ]; then
    [ -z "$PD_BRANCH" ] && export PD_BRANCH="$BRANCH"
    [ -z "$TIKV_BRANCH" ] && export TIKV_BRANCH="$BRANCH"
    [ -z "$TIDB_BRANCH" ] && export TIDB_BRANCH="$BRANCH"
  fi
  echo "use branch \`${BRANCH-master}\` for ci test"
}

function clean_data_log() {
  rm -rf ./data ./log
}

function prepare_next_gen_data_dirs() {
  mkdir -p \
    ./data/minio ./log/minio \
    ./data/pd0 ./log/pd0 \
    ./data/tikv0 ./log/tikv0 \
    ./data/tikv-worker0 ./log/tikv-worker0 \
    ./log/tidb0 \
    ./data/tiflash-wn0 ./log/tiflash-wn0 \
    ./data/tiflash-cn0 ./log/tiflash-cn0
}

function check_env() {
  local cur_dir=$(pwd)
  local prebuilt_bin_dir=$(realpath "${cur_dir}/../../tests/.build/tiflash")
  if [[ ! -d ${prebuilt_bin_dir} ]]; then
    echo "No pre-build tiflash binary directory: ${prebuilt_bin_dir}"
    exit -1
  else
    echo "Running tests with pre-built tiflash binary: ${prebuilt_bin_dir}/tiflash"
    ls -l ${prebuilt_bin_dir}
    ${prebuilt_bin_dir}/tiflash --version
  fi
}

function validate_local_binary() {
  local bin_path=$1
  local name=$2
  if [[ ! -f "${bin_path}" ]]; then
    echo "Error: ${name} not found: ${bin_path}" >&2
    exit 1
  fi
  if [[ ! -x "${bin_path}" ]]; then
    echo "Error: ${name} is not executable: ${bin_path}" >&2
    exit 1
  fi
}

function append_local_binary_overrides() {
  local -n compose_files=$1
  local override_dir="${2:-../docker/next-gen-yaml/override}"
  local validate_binaries="${3:-true}"

  if [[ -n "${LOCAL_PD_BIN_DIR:-}" ]]; then
    export LOCAL_PD_BIN_DIR="$(realpath "${LOCAL_PD_BIN_DIR}")"
    if [[ "${validate_binaries}" == "true" ]]; then
      validate_local_binary "${LOCAL_PD_BIN_DIR}/pd-server" "pd-server"
    fi
    compose_files+=(-f "${override_dir}/local_pd.yaml")
    echo "Using local PD binary: ${LOCAL_PD_BIN_DIR}/pd-server"
  fi
  if [[ -n "${LOCAL_TIKV_BIN_DIR:-}" ]]; then
    export LOCAL_TIKV_BIN_DIR="$(realpath "${LOCAL_TIKV_BIN_DIR}")"
    if [[ "${validate_binaries}" == "true" ]]; then
      validate_local_binary "${LOCAL_TIKV_BIN_DIR}/tikv-server" "tikv-server"
      validate_local_binary "${LOCAL_TIKV_BIN_DIR}/tikv-worker" "tikv-worker"
    fi
    compose_files+=(-f "${override_dir}/local_tikv.yaml")
    echo "Using local TiKV binary: ${LOCAL_TIKV_BIN_DIR}/tikv-server"
    echo "Using local TiKV worker binary: ${LOCAL_TIKV_BIN_DIR}/tikv-worker"
  fi
  if [[ -n "${LOCAL_TIDB_BIN_DIR:-}" ]]; then
    export LOCAL_TIDB_BIN_DIR="$(realpath "${LOCAL_TIDB_BIN_DIR}")"
    if [[ "${validate_binaries}" == "true" ]]; then
      validate_local_binary "${LOCAL_TIDB_BIN_DIR}/tidb-server" "tidb-server"
    fi
    compose_files+=(-f "${override_dir}/local_tidb.yaml")
    echo "Using local TiDB binary: ${LOCAL_TIDB_BIN_DIR}/tidb-server"
  fi
}

function setup_next_gen_compose_files() {
  local compose_files_name=$1
  local -n compose_files=${compose_files_name}
  local validate_binaries="${2:-true}"

  DISAGG_TIFLASH_YAML="disagg_tiflash.yaml"
  if [[ -f /etc/redhat-release ]] && grep -q "Rocky Linux release 9" /etc/redhat-release; then
    sed 's/tiflash-ci-base:rocky8-20241028/tiflash-ci-base:rocky9-20250529/g' \
        "${DISAGG_TIFLASH_YAML}" > "disagg_tiflash.rocky9.yaml"
    DISAGG_TIFLASH_YAML="disagg_tiflash.rocky9.yaml"
    echo "Using ${DISAGG_TIFLASH_YAML} for Rocky Linux 9"
  fi

  compose_files=(-f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}")
  append_local_binary_overrides "${compose_files_name}" "../docker/next-gen-yaml/override" "${validate_binaries}"
  if [[ -n "${EXPOSE_TIDB_PORT:-}" ]]; then
    compose_files+=(-f "../docker/next-gen-yaml/override/expose_tidb.yaml")
    echo "Exposing tidb0 on host port ${EXPOSE_TIDB_PORT}"
  fi
}

function check_docker_compose() {
  # Try to use these compose tools:
  # - `docker-compose`, the original compose tool on CI
  # - `podman compose`, the podman compose tool, which is compatible with docker compose,
  #   and supports rootless mode
  # - `docker compose`, the new docker provide compose command, which is compatible
  #   with `docker-compose`
  if command -v docker-compose &>/dev/null; then
    echo "docker-compose is installed."
    export COMPOSE="docker-compose"
  else
    if command -v podman &>/dev/null; then
      echo "podman is installed, using it as docker-compose."
      export COMPOSE="podman compose"
    else
      if command -v docker &>/dev/null; then
        echo "docker compose is installed."
        export COMPOSE="docker compose"
      else
        echo "Neither docker-compose nor docker noo podman could be found, please install one of them first."
        exit 1
      fi
    fi
  fi
}

export -f show_env
export -f wait_env
export -f wait_tiflash_env
export -f wait_next_gen_env
export -f set_branch
export -f clean_data_log
export -f prepare_next_gen_data_dirs
export -f check_env
export -f check_docker_compose
export -f validate_local_binary
export -f append_local_binary_overrides
export -f setup_next_gen_compose_files
