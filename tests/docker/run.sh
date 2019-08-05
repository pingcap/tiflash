#!/bin/bash

set -xe

docker-compose down

rm -rf ./data ./log

./build_learner_config.sh

docker-compose up -d --scale tics0=0 --scale tiflash0=0 --scale tikv-learner0=0

sleep 60

docker-compose up -d --scale tics0=0 --scale tikv-learner0=0 --build

sleep 5

docker-compose up -d --scale tics0=0

docker-compose exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true'

docker-compose down

docker-compose up -d --scale tiflash0=0 --scale tikv-learner0=0 --scale tikv0=0 --scale tidb0=0 --scale pd0=0

docker-compose exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'

docker-compose down


