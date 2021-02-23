#!/bin/bash

set -x
set +e

pwd
df -h
grep ^ /sys/block/*/queue/rotational

cat /proc/cpuinfo | grep name | cut -f2 -d: | uniq -c
cat /proc/meminfo
uname -a
hostname
lsmod
dmidecode | grep 'Product Name'
free -mh
cat /proc/loadavg

set -e

function wait_env() {
  local engine="$1"
  local timeout='60'
  local failed='true'

  echo "=> wait for env available"

  for (( i = 0; i < "${timeout}"; i++ )); do
    if [[ -n $(cat ./log/tidb0/tidb.log | grep "server is running MySQL protocol") && \
          -n $(cat ./log/tiflash_${engine}/server.log | grep "Ready for connections") ]]; then
        local failed='false'
        break
    fi

    if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
      echo "   #${i} waiting for env available"
    fi

    sleep 1
  done

  if [ "${failed}" == 'true' ]; then
    echo "   can not set up env" >&2
    exit 1
  else
    echo "   available"
  fi
}

# TAG:        tiflash hash, default to `master`
# XYZ_BRANCH: pd/tikv/tidb hash, default to `master`
# BRANCH:     hash short cut, default to `master`
if [ -n "$BRANCH" ]; then
  [ -z "$PD_BRANCH" ] && export PD_BRANCH="$BRANCH"
  [ -z "$TIKV_BRANCH" ] && export TIKV_BRANCH="$BRANCH"
  [ -z "$TIDB_BRANCH" ] && export TIDB_BRANCH="$BRANCH"
fi


# Stop all docker instances if exist.
docker-compose -f gtest.yaml -f cluster.yaml -f cluster_new_collation.yaml -f cluster_clustered_index.yaml -f tiflash-dt.yaml -f mock-test-dt.yaml \
  -f cluster_tidb_fail_point.yaml \
  down

rm -rf ./data ./log

# run async-commit tests under tidb with failpoint
docker-compose -f cluster_tidb_fail_point.yaml -f tiflash-dt.yaml up -d
wait_env dt
docker-compose -f cluster_tidb_fail_point.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh async-commit'
docker-compose -f cluster_tidb_fail_point.yaml -f tiflash-dt.yaml down
rm -rf ./data ./log

#################################### TIDB-CI ONLY ####################################
# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env dt
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh tidb-ci/fullstack-test true && ./run-test.sh tidb-ci/fullstack-test-dt'
docker-compose -f cluster.yaml -f tiflash-dt.yaml down
rm -rf ./data ./log

[[ "$TIDB_CI_ONLY" -eq 1 ]] && exit
#################################### TIDB-CI ONLY ####################################


# run gtest cases. (only tics-gtest up)
docker-compose -f gtest.yaml up -d
docker-compose -f gtest.yaml exec -T tics-gtest bash -c 'cd /tests && ./run-gtest.sh'
docker-compose -f gtest.yaml down
rm -rf ./data ./log


# run fullstack-tests (for engine DeltaTree)
docker-compose -f cluster.yaml -f tiflash-dt.yaml up -d
wait_env dt
docker-compose -f cluster.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh fullstack-test true && ./run-test.sh fullstack-test-dt && ./run-test.sh clustered_index_fullstack'
docker-compose -f cluster.yaml -f tiflash-dt.yaml down
rm -rf ./data ./log


# We need to separate mock-test for dt and tmt, since this behavior
# is different in some tests
# * "tmt" engine ONLY support disable_bg_flush = false.
# * "dt"  engine ONLY support disable_bg_flush = true.
# (only tics0 up) (for engine DetlaTree)
docker-compose -f mock-test-dt.yaml up -d
docker-compose -f mock-test-dt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh delta-merge-test'
docker-compose -f mock-test-dt.yaml down
rm -rf ./data ./log


# (only tics0 up) (for engine TxnMergeTree)
docker-compose -f mock-test-tmt.yaml up -d
docker-compose -f mock-test-tmt.yaml exec -T tics0 bash -c 'cd /tests ; ./run-test.sh mutable-test'
docker-compose -f mock-test-tmt.yaml down
rm -rf ./data ./log


# run new_collation_fullstack tests
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml up -d
wait_env dt
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh new_collation_fullstack'
docker-compose -f cluster_new_collation.yaml -f tiflash-dt.yaml down
rm -rf ./data ./log

# run clustered index tests
docker-compose -f cluster_clustered_index.yaml -f tiflash-dt.yaml up -d
wait_env dt
docker-compose -f cluster_clustered_index.yaml -f tiflash-dt.yaml exec -T tiflash0 bash -c 'cd /tests ; ./run-test.sh clustered_index_fullstack'
docker-compose -f cluster_clustered_index.yaml -f tiflash-dt.yaml down
rm -rf ./data ./log
