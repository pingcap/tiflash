#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

# (only tics0 up) (for engine TxnMergeTree)
docker-compose -f mock-test-tmt.yaml down
clean_data_log

docker-compose -f mock-test-tmt.yaml up -d
docker-compose -f mock-test-tmt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'

docker-compose -f mock-test-tmt.yaml down
clean_data_log
