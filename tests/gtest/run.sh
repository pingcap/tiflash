#!/bin/bash

source ../docker/util.sh

set_branch

set -xe

show_env

# run gtest cases. (only tics-gtest up)
docker-compose -f gtest.yaml down
clean_data_log

docker-compose -f gtest.yaml up -d
docker-compose -f gtest.yaml exec -T tics-gtest bash -c 'cd /tests && ./run-gtest.sh'

docker-compose -f gtest.yaml down
clean_data_log
