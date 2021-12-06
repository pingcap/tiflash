#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

# (only tics0 up) (for engine DetlaTree)
docker-compose -f mock-test-dt.yaml down
clean_data_log

docker-compose -f mock-test-dt.yaml up -d
wait_tiflash_env
docker-compose -f mock-test-dt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh delta-merge-test'

docker-compose -f mock-test-dt.yaml down
clean_data_log
