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


source ../docker/util.sh

set_branch

set -xe

check_env
check_docker_compose

if [[ -n "$ENABLE_NEXT_GEN" && "$ENABLE_NEXT_GEN" != "false" && "$ENABLE_NEXT_GEN" != "0" ]]; then
    echo "Running fullstack test on next-gen TiFlash"

    # clean up previous docker instances, data and log
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml down
    clean_data_log
    # create bucket "tiflash-test" on minio
    mkdir -pv data/minio/tiflash-test

    # run fullstack-tests
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml up -d
    wait_next_gen_env
    # FIXME: now only run the tiflash local index test
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml exec -T tiflash-wn0 bash -c 'cd /tests ; ./run-test.sh fullstack-test-index/vector'
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml down
    clean_data_log

    exit 0
fi

# classic TiFlash
echo "Running fullstack test on classic TiFlash"

# clean up previous docker instances, data and log
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

# FIXME: now vector does not support run with encryption-at-rest enabled
${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-encrypt.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-encrypt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test-index'
${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-encrypt.yaml down
clean_data_log


${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test2 true && ./run-test.sh fullstack-test-dt'
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/mpp'
${COMPOSE} -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml down
clean_data_log
