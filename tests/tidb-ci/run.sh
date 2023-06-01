#!/bin/bash
# Copyright 2022 PingCAP, Ltd.
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

# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/fail-point-tests && ./run-test.sh tidb-ci/fullstack-test true && ./run-test.sh tidb-ci/fullstack-test-dt'

docker-compose -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/async_grpc'

docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/disable_local_tunnel'

docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt-disable-planner.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-disable-planner.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/disable_planner'

docker-compose -f cluster.yaml -f tiflash-dt-disable-planner.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt-enable-pipeline.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-enable-pipeline.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/enable_pipeline'

docker-compose -f cluster.yaml -f tiflash-dt-enable-pipeline.yaml down
clean_data_log

# run new_collation_fullstack tests
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/new_collation_fullstack'

docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

# run disable_new_collation_fullstack tests
docker-compose -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/disable_new_collation_fullstack'

docker-compose -f cluster_disable_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

# run force_enable_lm tests
docker-compose -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/force_enable_lm'

docker-compose -f cluster.yaml -f tiflash-dt-force-enable-lm.yaml down
clean_data_log
