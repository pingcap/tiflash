#!/bin/bash

set -xe

docker-compose down

rm -rf ./data ./log

docker-compose up -d --scale tics0=0 --scale tics-gtest=0 --scale tiflash0=0

sleep 60

# run gtest cases. (only tics-gtest up)
docker-compose up -d --scale tics0=0 --scale tiflash0=0 --scale tikv0=0 --scale tidb0=0 --scale pd0=0
docker-compose exec -T tics-gtest bash -c 'cd /tests && ./run-gtest.sh'
docker-compose down

docker-compose up -d --scale tics0=0 --scale tics-gtest=0 --build
docker-compose exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true'
docker-compose down

# (only tics0 up)
docker-compose up -d --scale tics-gtest=0 --scale tiflash0=0 --scale tikv0=0 --scale tidb0=0 --scale pd0=0
docker-compose exec -T tics0 bash -c 'cd /tests ; ./run-test.sh  delta-merge-test && ./run-test.sh mutable-test'
docker-compose down
