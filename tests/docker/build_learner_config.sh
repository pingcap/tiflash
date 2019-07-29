#!/bin/bash

set -ue
set -o pipefail

source ./_helper.sh

host_ip=`get_host_ip`

echo "[readpool.storage]" > ./config/tikv-learner.toml
echo "[readpool.coprocessor]" >> ./config/tikv-learner.toml
echo "[server]" >> ./config/tikv-learner.toml
echo "labels = { zone = 'engine' }" >> ./config/tikv-learner.toml
echo "engine-addr = '${host_ip}:3930'" >> ./config/tikv-learner.toml
echo "[storage]" >> ./config/tikv-learner.toml
echo "[pd]" >> ./config/tikv-learner.toml
echo "[metric]" >> ./config/tikv-learner.toml
echo "[raftstore]" >> ./config/tikv-learner.toml
echo "raftdb-path = """ >> ./config/tikv-learner.toml
echo "sync-log = true" >> ./config/tikv-learner.toml
echo "max-leader-missing-duration = '22s'" >> ./config/tikv-learner.toml
echo "abnormal-leader-missing-duration = '21s'" >> ./config/tikv-learner.toml
echo "peer-stale-state-check-interval = '20s'" >> ./config/tikv-learner.toml
echo "[coprocessor]" >> ./config/tikv-learner.toml
echo "[rocksdb]" >> ./config/tikv-learner.toml
echo "wal-dir = ''" >> ./config/tikv-learner.toml
echo "[rocksdb.defaultcf]" >> ./config/tikv-learner.toml
echo "block-cache-size = '10GB'" >> ./config/tikv-learner.toml
echo "[rocksdb.lockcf]" >> ./config/tikv-learner.toml
echo "block-cache-size = '4GB'" >> ./config/tikv-learner.toml
echo "[rocksdb.writecf]" >> ./config/tikv-learner.toml
echo "block-cache-size = '4GB'" >> ./config/tikv-learner.toml
echo "[raftdb]" >> ./config/tikv-learner.toml
echo "[raftdb.defaultcf]" >> ./config/tikv-learner.toml
echo "block-cache-size = '1GB'" >> ./config/tikv-learner.toml
echo "[security]" >> ./config/tikv-learner.toml
echo "ca-path = ''" >> ./config/tikv-learner.toml
echo "cert-path = ''" >> ./config/tikv-learner.toml
echo "key-path = ''" >> ./config/tikv-learner.toml
echo "[import]" >> ./config/tikv-learner.toml

