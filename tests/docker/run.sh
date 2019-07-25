#!/bin/bash

set -xe

docker-compose down

rm -rf ./data ./log

docker-compose up -d --scale tiflash0=0 --scale tikv-learner0=0 --scale tikv0=0 --scale tidb0=0 --scale pd0=0

docker-compose exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'

docker-compose down

docker-compose up -d --scale tics0=0

docker-compose exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test'

docker-compose down
