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
    echo "Skip fullstack test on next-gen TiFlash"
    exit 0
fi

# classic TiFlash
echo "Running fullstack test on classic TiFlash"

# run fullstack-tests (for engine DeltaTree)
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/fail-point-tests && ./run-test.sh tidb-ci/fullstack-test true && ./run-test.sh tidb-ci/fullstack-test-dt'

${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

# run new_collation_fullstack tests
${COMPOSE} -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

${COMPOSE} -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env
${COMPOSE} -f cluster_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/new_collation_fullstack'

${COMPOSE} -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

# run disable_new_collation_fullstack tests
${COMPOSE} -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

${COMPOSE} -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env
${COMPOSE} -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/disable_new_collation_fullstack'

${COMPOSE} -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

# run force_enable_lm tests
${COMPOSE} -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/force_enable_lm'

${COMPOSE} -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml down
clean_data_log

# run lightweight compression tests
${COMPOSE} -f cluster.yaml -f tiflash-dt-lightweight-compression.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt-lightweight-compression.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/lightweight_compression'

${COMPOSE} -f cluster.yaml -f tiflash-dt-lightweight-compression.yaml down
clean_data_log
