## How to run test cases in local

For running intergration test cases (defined in `./fullstack-test`, `./fullstack-test-dt`, `./new_collation_fullstack`), you should define a TiDB cluster with TiFlash node (1 PD, 1 TiKV, 1 TiDB, 1 TiFlash at least).

1. Deploy a cluster using [tiup](https://tiup.io/)
2. Use `ti.sh x.ti prop` to ensure the ports of your cluster
3. Change the "storage_port", "tidb_port" in `./env.sh` to let it connect to your cluster
4. Run tests with `./run-test.sh fullstack-test/ddl`

## How to run test cases in `delta-merge-test`

For running mock test cases (defined in `./delta-merge-test`), you should start a standalone tiflash that is not connected to tidb cluster
```
export TIFLASH_PORT=9000
./tiflash server -- --path /tmp/tiflash/data --tcp_port ${TIFLASH_PORT}
storage_port=${TIFLASH_PORT} verbose=true ./run-test.sh delta-merge-test
```
