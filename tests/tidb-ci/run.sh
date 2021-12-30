#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/fail-point-tests && ./run-test.sh tidb-ci/fullstack-test true && ./run-test.sh tidb-ci/fullstack-test-dt'

docker-compose -f cluster.yaml -f tiflash-dt.yaml down

docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.toml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.toml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/async_grpc'

docker-compose -f cluster.yaml -f tiflash-dt-async-grpc.toml down

docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.toml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.toml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/disable_local_tunnel'

docker-compose -f cluster.yaml -f tiflash-dt-disable-local-tunnel.toml down

clean_data_log

# run new_collation_fullstack tests
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/new_collation_fullstack'

docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
clean_data_log
