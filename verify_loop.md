# 编译

## tiflash 编译

```bash
# 代码位置
# /DATA/disk1/jaysonhuang/tiflash/

# 编译(已经带 -DENABLE_NEXT_GEN=1 -DENABLE_NEXT_GEN_COLUMNAR=1)
cd /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng
# 加载工具链
source /DATA/disk1/ra_common/.tiflash_env_17_basic
# 使用 ninja 编译
ninja -j 16 tiflash gtests_dbms

# 编译后的 binary 位置
# /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng/dbms/src/Server/tiflash
# libtiflash_proxy.so 同目录，启动时需 LD_LIBRARY_PATH 指向该目录
```

修改 `contrib/tiflash-proxy-columnar` 后需重新编译 tiflash（会连带 proxy）。仅改 C++ 时 `ninja tiflash` 即可。

## tikv / tikv-worker 编译

```bash
cd /DATA/disk1/jaysonhuang/cloud-storage-engine/
make release

# 集群启动使用 release binary：
# /DATA/disk1/jaysonhuang/cloud-storage-engine/target/release/tikv-server
# /DATA/disk1/jaysonhuang/cloud-storage-engine/target/release/tikv-worker

# debug（可选，本地调试）：
# make build
# target/debug/tikv-server
# target/debug/tikv-worker
```

# 启动

前提：PD（`:6530`）和 TiDB（`:8031`）已运行。存储组件按 **TiKV → tikv-worker → TiFlash** 顺序启动。

| 组件 | 地址 |
|------|------|
| PD | `10.2.12.81:6530` |
| TiDB | `10.2.12.81:8031` |
| TiKV | `10.2.12.81:7530` |
| tikv-worker | `10.2.12.81:19030` |
| TiFlash compute | `10.2.12.81:5035` |

## 启动 TiKV

```bash
/DATA/disk1/jaysonhuang/cloud-storage-engine/target/release/tikv-server \
    --addr "0.0.0.0:7530" \
    --advertise-addr "10.2.12.81:7530" \
    --status-addr "0.0.0.0:16530" \
    --advertise-status-addr "10.2.12.81:16530" \
    --pd "10.2.12.81:6530" \
    --data-dir "/DATA/disk3/jaysonhuang/clusters/tikv-7530/data" \
    --config /DATA/disk3/jaysonhuang/clusters/tikv-7530/conf/tikv.toml \
    --log-file "/DATA/disk3/jaysonhuang/clusters/tikv-7530/log/tikv.log" \
    >> /DATA/disk3/jaysonhuang/clusters/tikv-7530/log/tikv.stdout 2>&1 &

# tikv 日志目录
/DATA/disk3/jaysonhuang/clusters/tikv-7530/log
```

## 启动 tikv-worker

```bash
/DATA/disk1/jaysonhuang/cloud-storage-engine/target/release/tikv-worker \
    --addr "0.0.0.0:19030" \
    --pd-endpoints "10.2.12.81:6530" \
    --data-dir "/DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/data" \
    --config /DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/tikv-worker.toml \
    --log-file "/DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/log/tikv.log" \
    >> /DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/log/tikv.stdout 2>&1 &
```

## 启动 tiflash-compute（tiflash-proxy-columnar）

必须使用 `server` 子命令，并在 `Server` 目录下设置 `LD_LIBRARY_PATH`（`libtiflash_proxy.so`、`libc++.so` 在同目录）。

```bash
cd /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng/dbms/src/Server
export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH
./tiflash server --config-file /DATA/disk3/jaysonhuang/clusters/tiflash-5035/conf/tiflash.toml \
    >> /DATA/disk3/jaysonhuang/clusters/tiflash-5035/log/tiflash.stdout 2>&1 &

# tiflash 日志目录
/DATA/disk3/jaysonhuang/clusters/tiflash-5035/log
```

## tidb 连接方式

```bash
mysql -h 10.2.12.81 -P 8031 -u root -D test
# 或
mycli -h 10.2.12.81 --port 8031 -D test -u root
```

检查 replica：

```sql
SELECT TABLE_SCHEMA, TABLE_NAME, AVAILABLE, TABLE_ID
FROM information_schema.tiflash_replica
WHERE TABLE_SCHEMA = 'test';
```

---

# 验证用例

清理表请只用 SQL（`drop table`），不要手动删除 `data/` 目录下的文件。

事务内分区表扫描必须在**同一个 mysql 会话**里执行（`begin` … `commit` 不能拆成多次 `mysql -e`）。

| Case | 表 | 状态 | 修复位置 |
|------|-----|------|----------|
| 2 | `test.t_enum`（enum 聚簇主键） | 已验证 | proxy：`columnar.rs` 读路径 `serialize_for_tiflash` 将 Enum 按 Enum16 窄化 |
| 4 | `test.employees`（RANGE 分区） | 已验证 | TiFlash：`StorageDisaggregatedColumnar.h` RNProxy 使用 `action.getHeader()` 补齐 `_tidb_tid` |
| 5 | `test.t_1`（BIGINT 聚簇主键） | **未修复** | 待查（columnar 读路径缺 `INT64_MAX` 行） |

集成测试对照：

- case 4：`tests/fullstack-test/mpp/extra_physical_table_column.test`
- case 5：`tests/fullstack-test2/clustered_index/query.test` 第 37 行

---

## Case 2：`test.t_enum`（enum PK + columnar）

```sql
drop table if exists test.t_enum;
create table test.t_enum (pk enum('tidb','pd','tikv','tiflash') primary key clustered);
insert into test.t_enum values('tidb'),('tiflash');
alter table test.t_enum set tiflash replica 1;
-- 等待 AVAILABLE=1（约 30s）
```

```sql
set tidb_isolation_read_engines=tiflash;
set tidb_enforce_mpp=1;
select * from test.t_enum;
select pk, pk+0 from test.t_enum order by pk+0;
```

预期：

```
pk: tidb, tiflash
pk+0: tidb/1, tiflash/4
```

修复前错误现象：`select *` 第二行 `pk` 为空；`pk+0` 为 `''/0, tidb/1`（缺 `tiflash/4`）。

---

## Case 4：`test.employees`（分区表 + 事务内 MPP）

建表（与 fullstack 测试一致）：

```sql
drop table if exists test.employees;
create table test.employees (
  id int(11) not null,
  fname varchar(30) default null,
  lname varchar(30) default null,
  hired date not null default '1970-01-01',
  separated date default '9999-12-31',
  job_code int(11) default null,
  store_id int(11) not null
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_bin
partition by range (store_id) (
  partition p0 values less than (6),
  partition p1 values less than (11),
  partition p2 values less than (16),
  partition p3 values less than (21),
  partition p4 values less than (maxvalue)
);
alter table test.employees set tiflash replica 1;
-- 等待 AVAILABLE=1
```

**单会话**验证（必须一次提交整段 SQL）：

```bash
mysql -h 10.2.12.81 -P 8031 -u root -D test <<'SQL'
set tidb_isolation_read_engines=tikv;
delete from test.employees;
set tidb_isolation_read_engines=tiflash;
set tidb_enforce_mpp=1;
set tidb_partition_prune_mode=dynamic;
begin;
insert into test.employees values(100,'aa','aa','2020-01-01',null,10,10);
select count(*) from test.employees;
insert into test.employees values(100,'aa','aa','2020-01-01',null,10,23);
select * from test.employees where store_id > 10;
set session tidb_allow_batch_cop=2;
select count(*) from test.employees;
select * from test.employees where store_id > 10;
commit;
SQL
```

预期：

| 步骤 | 结果 |
|------|------|
| 第一次 `count(*)` | 1 |
| `store_id > 10` | 一行 `store_id=23` |
| `batch_cop=2` 后 `count(*)` | 2 |
| 再次 `store_id > 10` | 仍为一行 `store_id=23` |

修复前错误（事务内第一次 `count(*)`）：

```text
DB::TiFlashException: The tidb table scan schema size 2 is different from
the tiflash storage schema size 1
schema=[_tidb_rowid, _tidb_tid]
storage_header=[exchange_receiver_0 ...]
```

根因：`genColumnDefinesForDisaggregatedRead` 不把 `_tidb_tid` 放进 `columns_to_read`，应由 `AddExtraTableIDColumnTransformAction` 注入；columnar 的 `RNProxySourceOp` / `RNProxyInputStream` 误用 `toEmptyBlock(columns_to_read)` 作 header，少一列。非 columnar 的 `RNSegmentSourceOp` 已正确使用 `action.getHeader()`。

事务外 `count(*)` 在修复前可通过（TiDB 常只下推 `_tidb_rowid` 一列），不能代替 case 4 验证。

---

## Case 5：`test.t_1`（BIGINT 聚簇主键，缺 INT64_MAX 行）

来源：`tests/fullstack-test2/clustered_index/query.test` **第 37 行**（2026-05-20 在当前 next-gen columnar 集群复现）。

建表与数据（测试文件第 17–19 行）：

```sql
drop table if exists test.t_1;
create table test.t_1(a bigint primary key clustered, col int);
insert into test.t_1 values(-9223372036854775808,1),(9223372036854775807,2),(0,3);
alter table test.t_1 set tiflash replica 1;
-- 等待 AVAILABLE=1
```

第 37 行验证 SQL：

```sql
set session tidb_isolation_read_engines='tiflash';
select * from test.t_1 where a > -9223372036854775808;
```

测试文件预期（2 行）：

```
| a                   | col  |
|                   0 |    3 |
| 9223372036854775807 |    2 |
```

当前集群实测（TiFlash，replica=1，无 SQL 报错）：

| 引擎 | `a > INT64_MIN` | `select *` 全表 | `count(*)` |
|------|-----------------|-----------------|------------|
| TiKV | 2 行（含 MAX） | 3 行 | 3 |
| TiFlash | **1 行**（仅 `0/3`） | **2 行**（缺 `9223372036854775807/2`） | **2** |

相关边界（同一表，TiFlash 均缺 MAX 主键行）：

```sql
-- 第 44 行：应有 3 行，TiFlash 仅 MIN + 0
set session tidb_isolation_read_engines='tiflash';
select * from test.t_1 where a >= -9223372036854775808;

-- 第 67 行：应有 1 行 MAX，TiFlash 为 0 行
select * from test.t_1 where a >= 9223372036854775807;
```

现象归纳：不是单纯谓词过滤错误，而是 TiFlash/columnar **读不到** `a = 9223372036854775807` 这条记录（MPP `TableFullScan` 也只返回 2 行）。与 case 2（enum PK）、case 4（`_tidb_tid` schema）无关。

```bash
# 快速对比 TiKV / TiFlash
mysql -h 10.2.12.81 -P 8031 -u root -D test -e "
set session tidb_isolation_read_engines='tikv';
select 'tikv' as eng, a, col from test.t_1 order by a;
set session tidb_isolation_read_engines='tiflash';
select 'tiflash' as eng, a, col from test.t_1 order by a;
set session tidb_isolation_read_engines='tiflash';
select * from test.t_1 where a > -9223372036854775808;
"
```

---

## 一键复现（表已存在且 replica=1）

```bash
# case 2
mysql -h 10.2.12.81 -P 8031 -u root -D test -e "
set tidb_isolation_read_engines=tiflash; set tidb_enforce_mpp=1;
select * from test.t_enum order by pk;
select pk, pk+0 from test.t_enum order by pk+0;
"

# case 4（见上方 heredoc 单会话脚本）

# case 5（query.test:37，需先建 t_1 并 wait replica）
mysql -h 10.2.12.81 -P 8031 -u root -D test -e "
set session tidb_isolation_read_engines='tiflash';
select * from test.t_1 where a > -9223372036854775808;
"
```
