## How to run test cases in local

For running integration test cases (defined in `./fullstack-test`, `./new_collation_fullstack`), you should define a TiDB cluster with TiFlash node (1 PD, 1 TiKV, 1 TiDB, 1 TiFlash at least).

1. Build your own TiFlash binary using debug profile:

    ```bash
    # In the TiFlash repository root:
    cmake --workflow --preset dev
    ```

2. Install [tiup](https://tiup.io/)
3. Use `tiup playground` (recommended) or `tiup cluster` to start a tidb cluster

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
    ```

4. Run test cases

    ```bash
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

## How to run test cases in `fullstack-test-next-gen`

For running next-gen integration test cases, you should define a TiDB cluster with disaggregated arch TiFlash node

* 1 minio-server for providing object storage API
* 1 PD
* 1 next-gen TiKV
* 1 TiDB
* 1 TiFlash compute node
* 1 TiFlash write node

Notice: The pd, next-gen TiKV, tidb now is only available from pingcap internal dockerhub. Make sure you can access the dockerhub before running the tests.

1. Build your own TiFlash binary using debug profile:

    ```bash
    # In the TiFlash repository root:
    mkdir cmake-build-ng
    cd cmake-build-ng
    cmake -GNinja -DCMAKE_BUILD_TYPE=DEBUG -DENABLE_NEXT_GEN=ON ..
    ninja tiflash -j32

    # Copy the TiFlash binary to test directory
    cmake --install . --component=tiflash-release --prefix "/path/to/your/tiflash/tests/.build/tiflash"
    ```

2. Run the tests using `docker compose`

    ```bash
    # In the TiFlash repository root:
    cd tests/fullstack-test-next-gen

    # Ensure `docker` or `podman` or `docker-compose` is installed in your env.
    # This script will start a minimal next-gen cluster for running the tests.
    # The next-gen TiKV, TiDB, PD, minio image will be pulled from pingcap internal
    # dockerhub. Make sure you can access the dockerhub before running the tests.
    ENABLE_NEXT_GEN=true ./run.sh

    # If you want a more verbose running log
    verbose=true ENABLE_NEXT_GEN=true ./run.sh
    ```

    If you want to do some manual testing, you can start a cluster as follow

    ```bash
    # start a cluster
    docker compose -f next-gen-cluster.yaml -f disagg_tiflash.yaml up -d

    # enter the tiflash wn pod
    docker exec -ti fullstack-test-next-gen-tiflash-wn0-1 bash
    # connect to the mysql from tiflash wn pod
    mysql --host tidb0 --port 4000 -u root -D test

    # Do NOT forget to release the cluster when you finish the debugging
    docker compose -f next-gen-cluster.yaml -f disagg_tiflash.yaml down
    ```
