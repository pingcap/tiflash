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


source ../docker/util.sh

set_branch

set -xe

export verbose=${verbose:-"false"}

check_env
check_docker_compose

if [[ -n "$ENABLE_NEXT_GEN" && "$ENABLE_NEXT_GEN" != "false" && "$ENABLE_NEXT_GEN" != "0" ]]; then
    echo "Running fullstack test on next-gen TiFlash"

    # clean up previous docker instances, data and log
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml down
    clean_data_log

    # run fullstack-tests
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml up -d
    wait_next_gen_env
    # TODO: now only run a part of the test, add more tests later
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml exec -T tiflash-wn0 bash -c "cd /tests ; verbose=${verbose} ./run-test.sh fullstack-test/sample.test"
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml exec -T tiflash-wn0 bash -c "cd /tests ; verbose=${verbose} ./run-test.sh fullstack-test-index/vector"
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml exec -T tiflash-wn0 bash -c "cd /tests ; verbose=${verbose} ./run-test.sh fullstack-test-next-gen/placement"
    ${COMPOSE} -f next-gen-cluster.yaml -f disagg_tiflash.yaml down
    clean_data_log

    exit 0
fi

# classic TiFlash
echo "Should not run classic TiFlash on this test environment"
exit 1
