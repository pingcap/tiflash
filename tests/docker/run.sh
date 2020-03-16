#!/bin/bash

set -xe

# Stop all docker instances if exist.
# tiflash-dt && tiflash-tmt share the same name "tiflash0", we just need one here
docker-compose -f gtest.yaml -f cluster.yaml -f tiflash-dt.yaml -f mock-test.yaml down

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
# TODO: Enable fullstack-test/ddl for engine DeltaTree
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/dml true'
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/expr true'
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/fault-inject true'
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test/sample.test true'
docker-compose -f cluster.yaml -f tiflash-dt.yaml down


rm -rf ./data ./log
# run fullstack-tests (for engine TxnMergeTree)
docker-compose -f cluster.yaml -f tiflash-tmt.yaml up -d
sleep 60
docker-compose -f cluster.yaml -f tiflash-tmt.yaml up -d --build
sleep 10
docker-compose -f cluster.yaml -f tiflash-tmt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true'
docker-compose -f cluster.yaml -f tiflash-tmt.yaml down


rm -rf ./data ./log
# (only tics0 up)
docker-compose -f mock-test.yaml up -d
docker-compose -f mock-test.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh  delta-merge-test && ./run-test.sh mutable-test'
docker-compose -f mock-test.yaml down
