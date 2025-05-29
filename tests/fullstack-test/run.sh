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
    echo "Skip this fullstack test on next-gen TiFlash"
    exit 0
fi

# classic TiFlash
echo "Running fullstack test on classic TiFlash"

# clean up previous docker instances, data and log
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

# run fullstack-tests
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test'
${COMPOSE} -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

${COMPOSE} -f cluster.yaml -f tiflash-dt-sync-grpc.yaml up -d
wait_env
${COMPOSE} -f cluster.yaml -f tiflash-dt-sync-grpc.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/mpp'
${COMPOSE} -f cluster.yaml -f tiflash-dt-sync-grpc.yaml down
clean_data_log
