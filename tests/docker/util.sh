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

# feature/fts TiFlash is 8.5.x, while CI may pair it with master PD/TiKV/TiDB.
# Pin PD's compatibility gate before TiFlash registers itself to PD.
function set_pd_cluster_version_for_tiflash() {
  local cluster_version="${TIFLASH_TEST_PD_CLUSTER_VERSION:-8.5.6}"
  if [[ -z "${cluster_version}" || "${cluster_version}" == "0" || "${cluster_version}" == "false" ]]; then
    return
  fi

  cluster_version="${cluster_version#v}"
  if [[ ! "${cluster_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
    echo "Invalid TIFLASH_TEST_PD_CLUSTER_VERSION: ${TIFLASH_TEST_PD_CLUSTER_VERSION}" >&2
    exit 1
  fi

  local timeout="${TIFLASH_TEST_PD_CLUSTER_VERSION_TIMEOUT:-60}"
  local failed='true'

  echo "=> set PD cluster-version to ${cluster_version} for TiFlash test"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if ${COMPOSE} "$@" exec -T pd0 /pd-ctl -u http://127.0.0.1:2379 config set cluster-version "${cluster_version}"; then
        failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for PD to set cluster-version"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set PD cluster-version" >&2
    exit 1
  fi
}

function start_cluster_with_tiflash() {
  local compose_args=()
  for compose_file in "$@"; do
    compose_args+=("-f" "${compose_file}")
  done

  ${COMPOSE} "${compose_args[@]}" up -d pd0 tikv0 tidb0
  set_pd_cluster_version_for_tiflash "${compose_args[@]}"
  ${COMPOSE} "${compose_args[@]}" up -d tiflash0
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
export -f set_pd_cluster_version_for_tiflash
export -f start_cluster_with_tiflash
export -f check_env
export -f check_docker_compose
