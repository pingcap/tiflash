## How to run test cases in local

1. Deploy a cluster using [ti.sh](https://github.com/pingcap/tiflash/blob/master/integrated/docs/ti.sh.md)
2. Use `ti.sh x.ti prop` to ensure the ports of your cluster
3. Change the "storage_port", "tidb_port" in `./env.sh` to let it connect to your cluster
4. Run tests with `./run-test.sh fullstack-test/ddl`

Instead of ti.sh, you can deploy a cluster with [TiUP](https://tiup.io/). But ti.sh is more easy to replace the binary for TiFlash/TiDB/PD, which is convenient for debugging.

* For running mock test cases (defined in `./delta-merge-test`), you should define a cluster with only one TiFlash node, and set standalone property for that node.

* For running intergration test cases (defined in `./fullstack-test`, `./fullstack-test-dt`, `./new_collation_fullstack`), you should define a TiDB cluster with TiFlash node (1 PD, 1 TiKV, 1 TiDB, 1 TiFlash at least).

