#!/bin/bash

set -xe

# TAG: tiflash hash, default to `master`
# XYZ_BRANCH: pd/tikv/tidb hash, default to `master`
# BRANCH: hash short cut, default to `master`

if [ -n "$BRANCH" ]; then
  [ -z "$PD_BRANCH" ] && PD_BRANCH="$BRANCH"
  [ -z "$TIKV_BRANCH" ] && TIKV_BRANCH="$BRANCH"
  [ -z "$TIDB_BRANCH" ] && TIDB_BRANCH="$BRANCH"
fi

# Stop all docker instances if exist.
# tiflash-dt && tiflash-tmt share the same name "tiflash0", we just need one here
docker-compose -f gtest.yaml -f cluster.yaml -f cluster_new_collation.yaml -f tiflash-dt.yaml -f mock-test-dt.yaml down

rm -rf ./data ./log
# run gtest cases. (only tics-gtest up)
docker-compose -f gtest.yaml up -d
docker-compose -f gtest.yaml exec -T tics-gtest bash -c 'cd /tests && ./run-gtest.sh'
docker-compose -f gtest.yaml down


rm -rf ./data ./log
# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
sleep 60
docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d --build
sleep 10
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true && ./run-test.sh fullstack-test-dt'
docker-compose -f cluster.yaml -f tiflash-dt.yaml down


# We need to separate mock-test for dt and tmt, since this behavior
# is different in some tests
# * "tmt" engine ONLY support disable_bg_flush = false.
# * "dt"  engine ONLY support disable_bg_flush = true.
rm -rf ./data ./log
# (only tics0 up) (for engine DetlaTree)
docker-compose -f mock-test-dt.yaml up -d
docker-compose -f mock-test-dt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh delta-merge-test'
docker-compose -f mock-test-dt.yaml down



rm -rf ./data ./log
# run fullstack-tests (for engine TxnMergeTree)
docker-compose -f cluster.yaml -f tiflash-tmt.yaml up -d
sleep 60
docker-compose -f cluster.yaml -f tiflash-tmt.yaml up -d --build
sleep 10
docker-compose -f cluster.yaml -f tiflash-tmt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true'
docker-compose -f cluster.yaml -f tiflash-tmt.yaml down


rm -rf ./data ./log
# (only tics0 up) (for engine TxnMergeTree)
docker-compose -f mock-test-tmt.yaml up -d
docker-compose -f mock-test-tmt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'
docker-compose -f mock-test-tmt.yaml down

rm -rf ./data ./log
# run new_collation_fullstack tests 
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d
sleep 60
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d --build
sleep 10
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh new_collation_fullstack'
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
