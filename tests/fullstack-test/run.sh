#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log

docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test && ./run-test.sh fullstack-test-dt'

docker-compose -f cluster.yaml -f tiflash-dt.yaml down
clean_data_log
