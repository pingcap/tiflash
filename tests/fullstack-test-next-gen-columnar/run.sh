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
# shellcheck source=/dev/null
source ./_env.sh

set_branch

set -xe

export verbose=${verbose:-"false"}

check_env
check_docker_compose

setup_next_gen_compose_files COMPOSE_FILES

if [[ -n "$ENABLE_NEXT_GEN" && "$ENABLE_NEXT_GEN" != "false" && "$ENABLE_NEXT_GEN" != "0" ]]; then
    echo "Running fullstack test on next-gen TiFlash"

    # clean up previous docker instances, data and log
    ${COMPOSE} "${COMPOSE_FILES[@]}" down
    clean_data_log
    prepare_next_gen_columnar_data_dirs

    ${COMPOSE} "${COMPOSE_FILES[@]}" up -d
    echo "PD version:"
    ${COMPOSE} "${COMPOSE_FILES[@]}" exec -T pd0 bash -c '/pd-server -V'
    echo "TiDB version:"
    ${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tidb0 bash -c '/tidb-server -V'
    echo "TiKV version:"
    ${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tikv0 bash -c '/tikv-server -V'
    echo "TiKV worker version:"
    ${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tikv-worker0 bash -c '/tikv-worker -V'

    # run fullstack-tests
    wait_next_gen_columnar_env
    ENV_ARGS="ENABLE_NEXT_GEN=true verbose=${verbose} "
    # most failpoints are expected to be set on the compute layer, use tiflash-cn0 to run tests
    ${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/sample.test"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test-index"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test-next-gen/placement"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/clustered_index"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/dml"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/variables"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/mpp"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/expr"
    #${COMPOSE} "${COMPOSE_FILES[@]}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/mpp"
    ${COMPOSE} "${COMPOSE_FILES[@]}" down
    clean_data_log

    exit 0
fi

# classic TiFlash
echo "Should not run classic TiFlash on this test environment"
exit 1
