#!/bin/bash

docker-compose up -d

docker-compose exec tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'

docker-compose down
