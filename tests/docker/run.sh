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


set -x
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

function wait_env() {
  local engine="$1"
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

# TAG:        tiflash hash, default to `master`
# XYZ_BRANCH: pd/tikv/tidb hash, default to `master`
# BRANCH:     hash short cut, default to `master`
if [ -n "$BRANCH" ]; then
  [ -z "$PD_BRANCH" ] && export PD_BRANCH="$BRANCH"
  [ -z "$TIKV_BRANCH" ] && export TIKV_BRANCH="$BRANCH"
  [ -z "$TIDB_BRANCH" ] && export TIDB_BRANCH="$BRANCH"
fi


# Stop all docker instances if exist.
docker-compose      \
  -f cluster.yaml   \
  -f tiflash-tagged-image.yaml     \
  down

rm -rf ./data ./log

#################################### TIDB-CI ONLY ####################################
# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-tagged-image.yaml up -d
wait_env dt
docker-compose -f cluster.yaml -f tiflash-tagged-image.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/fullstack-test true && ./run-test.sh tidb-ci/fullstack-test-dt'
docker-compose -f cluster.yaml -f tiflash-tagged-image.yaml down
rm -rf ./data ./log

[[ "$TIDB_CI_ONLY" -eq 1 ]] && exit
#################################### TIDB-CI ONLY ####################################
