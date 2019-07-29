#!/bin/bash

set -ue
set -o pipefail

source ./_helper.sh

host_ip=`get_host_ip`

echo "[readpool.storage]" > ./config/tiflash-learner.toml
echo "[readpool.coprocessor]" >> ./config/tiflash-learner.toml
echo "[server]" >> ./config/tiflash-learner.toml
echo "labels = { zone = 'engine' }" >> ./config/tiflash-learner.toml
echo "engine-addr = '${host_ip}:3930'" >> ./config/tiflash-learner.toml
echo "[storage]" >> ./config/tiflash-learner.toml
echo "[pd]" >> ./config/tiflash-learner.toml
echo "[metric]" >> ./config/tiflash-learner.toml
echo "[raftstore]" >> ./config/tiflash-learner.toml
echo "raftdb-path = """ >> ./config/tiflash-learner.toml
echo "sync-log = true" >> ./config/tiflash-learner.toml
echo "max-leader-missing-duration = '22s'" >> ./config/tiflash-learner.toml
echo "abnormal-leader-missing-duration = '21s'" >> ./config/tiflash-learner.toml
echo "peer-stale-state-check-interval = '20s'" >> ./config/tiflash-learner.toml
echo "[coprocessor]" >> ./config/tiflash-learner.toml
echo "[rocksdb]" >> ./config/tiflash-learner.toml
echo "wal-dir = ''" >> ./config/tiflash-learner.toml
echo "[rocksdb.defaultcf]" >> ./config/tiflash-learner.toml
echo "block-cache-size = '10GB'" >> ./config/tiflash-learner.toml
echo "[rocksdb.lockcf]" >> ./config/tiflash-learner.toml
echo "block-cache-size = '4GB'" >> ./config/tiflash-learner.toml
echo "[rocksdb.writecf]" >> ./config/tiflash-learner.toml
echo "block-cache-size = '4GB'" >> ./config/tiflash-learner.toml
echo "[raftdb]" >> ./config/tiflash-learner.toml
echo "[raftdb.defaultcf]" >> ./config/tiflash-learner.toml
echo "block-cache-size = '1GB'" >> ./config/tiflash-learner.toml
echo "[security]" >> ./config/tiflash-learner.toml
echo "ca-path = ''" >> ./config/tiflash-learner.toml
echo "cert-path = ''" >> ./config/tiflash-learner.toml
echo "key-path = ''" >> ./config/tiflash-learner.toml
echo "[import]" >> ./config/tiflash-learner.toml

