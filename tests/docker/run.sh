#!/bin/bash

set -xe

docker-compose down

rm -rf ./data ./log

docker-compose up -d

docker-compose exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test && ./run-gtest.sh '

docker-compose down
