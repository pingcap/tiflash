#!/bin/bash

docker-compose down

rm -rf ./data ./log

docker-compose up -d

docker-compose exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'

docker-compose down
