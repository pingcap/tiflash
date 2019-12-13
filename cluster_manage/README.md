# TiFlash Cluster Manage

- It works as `Cluster Manage` for `TiFlash` to interact with `TiDB` / `PD` / `TiKV`, run with each flash node.
A master will be elected by using `etcd` to manage the whole flash cluster. Periodically scan tables with column 
replicas configuration in tidb and try to build flash engine peers or monitor their status.
- language: `Python` `C/C++`

## Prepare

* install `python3.7`, `pybind11`, `pyinstaller`, `clang-format`, `dnspython`, `uri`, `requests`, `urllib3`
, `toml`, `C++17`
, `setuptools`

* use `release.sh` and get dir `dist/flash_cluster_manager/`.

## Run

* show version
```
./dist/flash_cluster_manager/flash_cluster_manager -v
```

* run cmd:
```
./dist/flash_cluster_manager/flash_cluster_manager --config [path-to-flash-config.toml]
```
