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

DISAGG_TIFLASH_YAML="disagg_tiflash.yaml"
if grep -q "Rocky Linux release 9" /etc/redhat-release; then
    # replace the base image for running under Rocky Linux 9
    sed 's/tiflash-ci-base:rocky8-20241028/tiflash-ci-base:rocky9-20250529/g' \
        "${DISAGG_TIFLASH_YAML}" > "disagg_tiflash.rocky9.yaml"
    DISAGG_TIFLASH_YAML="disagg_tiflash.rocky9.yaml"
    echo "Using ${DISAGG_TIFLASH_YAML} for Rocky Linux 9"
fi

if [[ -n "$ENABLE_NEXT_GEN" && "$ENABLE_NEXT_GEN" != "false" && "$ENABLE_NEXT_GEN" != "0" ]]; then
    echo "Running fullstack test on next-gen TiFlash"

    # set images for next-gen TiFlash cluster
    HUB_ADDR="us-docker.pkg.dev/pingcap-testing-account/hub"
    if [[ -z "${PD_BRANCH}" || "${PD_BRANCH}" == "master" ]]; then
        PD_BRANCH="master-next-gen"
    fi
    if [[ -z "${TIKV_BRANCH}" || "${TIKV_BRANCH}" == "dedicated" ]]; then
        TIKV_BRANCH="dedicated-next-gen"
    fi
    if [[ -z "${TIDB_BRANCH}" || "${TIDB_BRANCH}" == "master" ]]; then
        TIDB_BRANCH="master-next-gen"
    fi
    export PD_IMAGE="${HUB_ADDR}/tikv/pd/image:${PD_BRANCH}"
    export TIKV_IMAGE="${HUB_ADDR}/tikv/tikv/image:${TIKV_BRANCH}"
    export TIDB_IMAGE="${HUB_ADDR}/pingcap/tidb/images/tidb-server:${TIDB_BRANCH}"

    # clean up previous docker instances, data and log
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" down
    clean_data_log

    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" up -d
    echo "PD version:"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T pd0 bash -c '/pd-server -V'
    echo "TiDB version:"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tidb0 bash -c '/tidb-server -V'
    echo "TiKV version:"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tikv0 bash -c '/tikv-server -V'

    # run fullstack-tests
    wait_next_gen_env
    ENV_ARGS="ENABLE_NEXT_GEN=true verbose=${verbose} "
    # most failpoints are expected to be set on the compute layer, use tiflash-cn0 to run tests
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/sample.test"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test-index/vector"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test-next-gen/placement"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/clustered_index"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/dml"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/variables"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test2/mpp"
    # maybe we need to split them into parallel pipelines because they take too long to run.
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/expr"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" exec -T tiflash-cn0 bash -c "cd /tests ; ${ENV_ARGS} ./run-test.sh fullstack-test/mpp"
    ${COMPOSE} -f next-gen-cluster.yaml -f "${DISAGG_TIFLASH_YAML}" down
    clean_data_log

    exit 0
fi

# classic TiFlash
echo "Should not run classic TiFlash on this test environment"
exit 1
