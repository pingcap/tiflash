## How to run test cases in local

For running intergration test cases (defined in `./fullstack-test`, `./fullstack-test-dt`, `./new_collation_fullstack`), you should define a TiDB cluster with TiFlash node (1 PD, 1 TiKV, 1 TiDB, 1 TiFlash at least).

1. Install [tiup](https://tiup.io/)
2. Use `tiup playground` (recommanded) or `tiup cluster` to start a tidb cluster

```bash
export TIDB_PORT=4000
export TIFLASH_PORT=9000

# Prepare a config file with `tcp_port` enabled
cat << EOF > tiflash.toml
tcp_port = ${TIFLASH_PORT}
# If you're deploying a testing cluster where the disk is not exclusively dedicated to TiFlash,
# it is necessary to set "capacity". Otherwise, PD will not add peers to TiFlash, making the
# tiflash replica can not be available.
capacity = 1000000000

[logger]
level = "debug"
EOF

# Start a tidb cluster with tiup playground
tiup playground nightly --tiflash 1 --tiflash.binpath /path/to/build/tiflash --db.port ${TIDB_PORT} --tiflash.config ./tiflash.toml

# Run tests with `./run-test.sh`, for example
tidb_port=${TIDB_PORT} storage_port=${TIFLASH_PORT} verbose=true ./run-test.sh fullstack-test/ddl
```

## How to run test cases in `delta-merge-test`

For running mock test cases (defined in `./delta-merge-test`), you should start a standalone tiflash that is not connected to tidb cluster

```bash
export TIFLASH_PORT=9000
./tiflash server -- --path /tmp/tiflash/data --tcp_port ${TIFLASH_PORT}
storage_port=${TIFLASH_PORT} verbose=true ./run-test.sh delta-merge-test
```
