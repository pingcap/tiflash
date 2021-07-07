#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

# We need to separate mock-test for dt and tmt, since this behavior
# is different in some tests
# * "tmt" engine ONLY support disable_bg_flush = false.
# * "dt"  engine ONLY support disable_bg_flush = true.
# (only tics0 up) (for engine DetlaTree)
docker-compose -f mock-test-dt.yaml down
clean_data_log

docker-compose -f mock-test-dt.yaml up -d
docker-compose -f mock-test-dt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh delta-merge-test'

docker-compose -f mock-test-dt.yaml down
clean_data_log
