# TiFlash Summary grafanalib 迁移差异说明

本文档由 `scripts/gen_migration_diff_md.py` 自动生成，对比：

- **修改前**：`jsonnet_legacy/scripts/tiflash_summary.original.json`（已在后续清理中删除；本文档保留当时对比结果）
- **修改后**：`tiflash_summary.json`

## 1. 总览

| | old | new |
|---|---:|---:|
| rows | 24 | 24 |
| panels | 229 | 230 |

### Panel 状态统计

| status | count |
|---|---:|
| `unchanged` | 22 |
| `changed` | 206 |
| `added` | 2 |
| `removed` | 1 |

### 差异标签统计（panel 可多标签）

| tag | count |
|---|---:|
| `rate_interval` | 150 |
| `hidden_right_axis` | 131 |
| `layout_repack` | 100 |
| `duration_quantiles` | 36 |
| `style_default` | 33 |
| `proxy_instance_selector` | 15 |
| `yaxis_visible` | 3 |
| `panel_split` | 3 |
| `legend_or_hide` | 3 |
| `other` | 1 |

## 2. Dashboard 级定义

- `title`: unchanged (`'Test-Cluster-TiFlash-Summary'`)
- `uid`: unchanged (`'SVbh2xUWk'`)
- `refresh`: unchanged (`'1m'`)
- `timezone`: `''` → `'browser'`
- `graphTooltip`: unchanged (`1`)
- `schemaVersion`: `27` → `14`
- `tags`: unchanged (`[]`)
- `time`: unchanged (`{'from': 'now-1h', 'to': 'now'}`)
- top-level keys only in **old**: `__requires`, `iteration`
- top-level keys only in **new**: `description`, `hideControls`, `rows`, `sharedCrosshair`
- templating names: old=['additional_groupby', 'instance', 'k8s_cluster', 'proxy_instance', 'tidb_cluster', 'tiflash_role'] new=['additional_groupby', 'instance', 'k8s_cluster', 'proxy_instance', 'tidb_cluster', 'tiflash_role']
  - `template[additional_groupby].query`: unchanged
  - `template[instance].query`: unchanged
  - `template[k8s_cluster].query`: unchanged
  - `template[proxy_instance].query`: unchanged
  - `template[tidb_cluster].query`: unchanged
  - `template[tiflash_role].query`: unchanged

## 3. Intentional 变更目录

下列差异为迁移中的预期行为，验收时一般可视为非回归：

- **`rate_interval`**（150 panels）：固定 scrape range（`[1m]` / `[30s]` / `[5m]` 等）改为 Grafana `[$__rate_interval]`，随 dashboard 刷新间隔自适应。
- **`proxy_instance_selector`**（15 panels）：Threads CPU 等 proxy 指标在 selector 中补上 `instance=~"$proxy_instance"`，与其它 proxy 面板一致。
- **`duration_quantiles`**（36 panels）：Duration 直方图面板收敛为 S3-style quantile 集（max/9999/999/99/80/avg）及默认 hide 可见性。
- **`hidden_right_axis`**（131 panels）：单轴面板隐藏右 Y 轴（`showY2: false`）；隐藏轴上的 `formatY2`/`minY2` 漂移不影响显示。
- **`panel_split`**（3 panels）：`Columnar Meta Cache Gauge` 拆成 `Entries` + `Weighted Size`，最后一行三等分展示。
- **`layout_repack`**（100 panels）：Layout 由 `Layout.row` 均分宽度/相对 y 重排；部分 panel 的 `gridPos` 变化。
- **`style_default`**（33 panels）：样式收敛到 `graph_panel` 默认（如 `nullPointMode`、legend 表头、Threads 去掉 points/decimals 等）。
- **`legend_or_hide`**（3 panels）：仅 legendFormat 或 hide 标志变化（PromQL 表达式不变）。

另外：Threads IO 单位 `Bps`→`binBps`（IEC）；`yaxis()` 强制禁止 SI 字节单位。

## 4. 逐 Row 摘要

| Row | unchanged | changed | added | removed |
|---|---:|---:|---:|---:|
| Server | 1 | 8 | 0 | 0 |
| Threads CPU | 0 | 16 | 0 | 0 |
| Threads | 0 | 4 | 0 | 0 |
| Coprocessor | 0 | 18 | 0 | 0 |
| Task Scheduler | 0 | 6 | 0 | 0 |
| DDL | 0 | 3 | 0 | 0 |
| Imbalance read/write | 0 | 6 | 0 | 0 |
| Memory trace | 7 | 5 | 0 | 0 |
| Columnar Storage | 0 | 10 | 2 | 1 |
| Storage | 1 | 17 | 0 | 0 |
| Storage Read Pool & Data Sharing | 2 | 7 | 0 | 0 |
| PageStorage | 2 | 10 | 0 | 0 |
| Rate Limiter | 1 | 4 | 0 | 0 |
| Storage Write Stall | 1 | 4 | 0 | 0 |
| Raft | 0 | 25 | 0 | 0 |
| Raft Snapshot / IngestSST | 0 | 11 | 0 | 0 |
| Rough Set Filter Rate Histogram | 0 | 2 | 0 | 0 |
| Disaggregated-Write | 1 | 16 | 0 | 0 |
| Disaggregated-Compute | 2 | 13 | 0 | 0 |
| S3 | 0 | 8 | 0 | 0 |
| Pipeline Model | 0 | 9 | 0 | 0 |
| TiFlash Resource Control | 0 | 2 | 0 | 0 |
| Status Server | 0 | 2 | 0 | 0 |
| Vector Search | 4 | 0 | 0 | 0 |

## 5. 逐 Row / Panel 明细

### Server

| Panel | status | tags |
|---|---|---|
| Store size | `changed` | `hidden_right_axis` |
| Available size | `changed` | `hidden_right_axis` |
| Capacity size | `changed` | `hidden_right_axis` |
| Uptime | `changed` | `hidden_right_axis` |
| Region | `unchanged` | — |
| CPU Usage | `changed` | `rate_interval` |
| Memory | `changed` | `other` |
| IO Throughput | `changed` | `rate_interval` |
| Remote Store Summary (Disagg arch) | `changed` | `hidden_right_axis` |

#### Store size

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Available size

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Capacity size

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Uptime

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### CPU Usage

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `rate(tiflash_proxy_process_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster="...` → `rate(tiflash_proxy_process_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster="...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Memory

- **status**: `changed`
- **tags**: `other`
- **field diffs**:
  - `targets`: `'12 series'` → `'12 series'`
- **target notes**:
  - t0: `tiflash_proxy_process_resident_memory_bytes{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster="$...` → `sum(tiflash_proxy_process_resident_memory_bytes{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluste...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### IO Throughput

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(irate(tiflash_proxy_threads_io_bytes_total{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster...` → `sum(irate(tiflash_proxy_threads_io_bytes_total{instance=~"$proxy_instance",instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Remote Store Summary (Disagg arch)

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

### Threads CPU

| Panel | status | tags |
|---|---|---|
| SST Import Service | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| SST Apply | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Region Task | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Region Worker | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Raft Store | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Apply Worker | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Storage Background (Small Tasks) | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Storage Background (Large Tasks) | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Manual Compaction | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| GRPC Async Server | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| GRPC Async Client | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| FAP builder | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Snapshot Sender | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Segment Scheduler | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Local Index Pool | `changed` | `rate_interval`, `proxy_instance_selector`, `layout_repack` |
| Segment Reader | `changed` | `rate_interval`, `layout_repack` |

#### SST Import Service

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"sst_importer.*",tidb_cluster="$tidb_cluster"}[1m]...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"sst_importer.*",tidb_...` (legend `{{instance}}`→`{{instance}}`, hide False→False)
  - +1 extra target(s) in new

#### SST Apply

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_low_.*",tidb_cluster="$tidb_cluster"}[1m]))...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_low_.*",tidb_cl...` (legend `{{instance}}`→`{{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_low_.*",tidb_cluster="$tidb_cluster"})by(insta...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_low_.*",tidb_clust...` (legend `Limit`→`Limit`, hide False→False)

#### Region Task

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_task.*",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_task.*",tidb_c...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_task.*",tidb_cluster="$tidb_cluster"})by(inst...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_task.*",tidb_clus...` (legend `Limit`→`Limit`, hide False→False)

#### Region Worker

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_worker.*",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_worker.*",tidb...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_worker.*",tidb_cluster="$tidb_cluster"})by(in...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"region_worker.*",tidb_cl...` (legend `Limit`→`Limit`, hide False→False)

#### Raft Store

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"raftstore_.*",tidb_cluster="$tidb_cluster"}[1m]))...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"raftstore_.*",tidb_cl...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"raftstore_.*",tidb_cluster="$tidb_cluster"})by(insta...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"raftstore_.*",tidb_clust...` (legend `Limit`→`Limit`, hide False→False)

#### Apply Worker

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name!~"apply_low_.*",name=~"apply_.*",tidb_cluster="$tid...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_.*",tidb_cluste...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name!~"apply_low_.*",name=~"apply_.*",tidb_cluster="$tidb_c...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"apply_.*",tidb_cluster="...` (legend `Limit`→`Limit`, hide False→False)

#### Storage Background (Small Tasks)

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_\\d+",tidb_cluster="$tidb_cluster"}[1m]))by(in...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_\\d+",tidb_cluster...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_\\d+",tidb_cluster="$tidb_cluster"})by(instance)` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_\\d+",tidb_cluster="$...` (legend `Limit`→`Limit`, hide False→False)

#### Storage Background (Large Tasks)

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_block_\\d+",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_block_\\d+",tidb_c...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_block_\\d+",tidb_cluster="$tidb_cluster"})by(inst...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"bg_block_\\d+",tidb_clus...` (legend `Limit`→`Limit`, hide False→False)

#### Manual Compaction

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"m_compact_pool",tidb_cluster="$tidb_cluster"}[1m]...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"m_compact_pool",tidb_...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"m_compact_pool",tidb_cluster="$tidb_cluster"})by(ins...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"m_compact_pool",tidb_clu...` (legend `Limit`→`Limit`, hide False→False)

#### GRPC Async Server

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"async_poller.*",tidb_cluster="$tidb_cluster"}[1m]...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"async_poller.*",tidb_...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"async_poller.*",tidb_cluster="$tidb_cluster"})by(ins...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"async_poller.*",tidb_clu...` (legend `Limit`→`Limit`, hide False→False)

#### GRPC Async Client

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"GRPCComp.*",tidb_cluster="$tidb_cluster"}[1m]))by...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"GRPCComp.*",tidb_clus...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"GRPCComp.*",tidb_cluster="$tidb_cluster"})by(instanc...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"GRPCComp.*",tidb_cluster...` (legend `Limit`→`Limit`, hide False→False)

#### FAP builder

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"fap_builder.*",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"fap_builder.*",tidb_c...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"GRPCComp.*",tidb_cluster="$tidb_cluster"})by(instanc...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"fap_builder.*",tidb_clus...` (legend `Limit`→`Limit`, hide False→False)

#### Snapshot Sender

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"snap_sender.*",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"snap_sender.*",tidb_c...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"snap_sender.*",tidb_cluster="$tidb_cluster"})by(inst...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"snap_sender.*",tidb_clus...` (legend `Limit`→`Limit`, hide False→False)

#### Segment Scheduler

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"segment_sched.*",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"segment_sched.*",tidb...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"segment_sched.*",tidb_cluster="$tidb_cluster"})by(in...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"segment_sched.*",tidb_cl...` (legend `Limit`→`Limit`, hide False→False)

#### Local Index Pool

- **status**: `changed`
- **tags**: `rate_interval`, `proxy_instance_selector`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"LocalIndexPool*",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"LocalIndexPool*",tidb...` (legend `pool-{{instance}}`→`pool-{{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"LocalIndexPool*",tidb_cluster="$tidb_cluster"})by(in...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"LocalIndexPool*",tidb_cl...` (legend `Limit`→`Limit`, hide True→False)
  - -1 target(s) removed in new

#### Segment Reader

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"SegmentReader.*",tidb...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"SegmentReader.*",tidb...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)

### Threads

| Panel | status | tags |
|---|---|---|
| Threads state | `changed` | `style_default`, `hidden_right_axis`, `layout_repack` |
| Threads IO | `changed` | `rate_interval`, `style_default`, `yaxis_visible`, `hidden_right_axis`, `layout_repack` |
| Thread Voluntary Context Switches | `changed` | `rate_interval`, `style_default`, `hidden_right_axis`, `layout_repack` |
| Thread Nonvoluntary Context Switches | `changed` | `rate_interval`, `style_default`, `hidden_right_axis`, `layout_repack` |

#### Threads state

- **status**: `changed`
- **tags**: `style_default`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`

#### Threads IO

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `yaxis_visible`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `formatY1`: `'Bps'` → `'binBps'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_threads_io_bytes_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `sum(rate(tiflash_proxy_threads_io_bytes_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` (legend `{{name}}-{{io}} {{$additional_groupby}}`→`{{name}}-{{io}} {{$additional_groupby}}`, hide False→False)

#### Thread Voluntary Context Switches

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_voluntary_context_switches{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` → `sum(rate(tiflash_proxy_thread_voluntary_context_switches{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` (legend `{{instance}} - {{name}}`→`{{instance}} - {{name}}`, hide False→False)

#### Thread Nonvoluntary Context Switches

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_nonvoluntary_context_switches{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$t...` → `sum(rate(tiflash_proxy_thread_nonvoluntary_context_switches{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$t...` (legend `{{instance}} - {{name}}`→`{{instance}} - {{name}}`, hide False→False)

### Coprocessor

| Panel | status | tags |
|---|---|---|
| Request QPS | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Executor QPS | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Request Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Error QPS | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Request Handle Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Response Bytes/Seconds | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Cop task memory usage | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Exchange Bytes/Seconds | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Threads of Rpc | `changed` | `hidden_right_axis`, `layout_repack` |
| Handling Request Number | `changed` | `hidden_right_axis`, `layout_repack` |
| Threads | `changed` | `hidden_right_axis`, `layout_repack` |
| Max Threads of Rpc | `changed` | `hidden_right_axis`, `layout_repack` |
| MPP Query count | `changed` | `hidden_right_axis`, `layout_repack` |
| Max Threads | `changed` | `hidden_right_axis`, `layout_repack` |
| Time of the Longest Live MPP Task | `changed` | `hidden_right_axis`, `layout_repack` |
| Data size in send and receive queue | `changed` | `hidden_right_axis`, `layout_repack` |
| Network Transmission | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Establish calldata details | `changed` | `hidden_right_axis`, `layout_repack` |

#### Request QPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_coprocessor_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(t...` → `sum(rate(tiflash_coprocessor_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Executor QPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_coprocessor_executor_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(...` → `sum(rate(tiflash_coprocessor_executor_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Request Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clust...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clu...` (legend `999-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` → `histogram_quantile(0.9999,sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clus...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide True→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` → `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clust...` (legend `95-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### Error QPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_coprocessor_request_error{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(r...` → `sum(rate(tiflash_coprocessor_request_error{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{reason}}`→`{{reason}}`, hide False→False)

#### Request Handle Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clust...` (legend `999-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` → `histogram_quantile(0.9999,sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` → `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster...` (legend `95-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +2 extra target(s) in new

#### Response Bytes/Seconds

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `sum(rate(tiflash_coprocessor_response_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(...` → `sum(rate(tiflash_coprocessor_response_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Cop task memory usage

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'4 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` → `histogram_quantile(0.999,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `999-{{type}}`→`999-{{type}}`, hide False→False)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.99,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` (legend `99-{{type}}`→`99-{{type}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.95,sum(rate(tiflash_coprocessor_request_memory_usage_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` (legend `95-{{type}}`→`95-{{type}}`, hide False→False)

#### Exchange Bytes/Seconds

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `sum(rate(tiflash_exchange_data_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(type)` → `sum(rate(tiflash_exchange_data_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interva...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Threads of Rpc

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`

#### Handling Request Number

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`

#### Threads

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`

#### Max Threads of Rpc

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`

#### MPP Query count

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`

#### Max Threads

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`

#### Time of the Longest Live MPP Task

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`

#### Data size in send and receive queue

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`

#### Network Transmission

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `56` → `64`
- **target notes**:
  - t0: `sum(rate(tiflash_network_transmission_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(...` → `sum(rate(tiflash_network_transmission_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Establish calldata details

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `56` → `64`

### Task Scheduler

| Panel | status | tags |
|---|---|---|
| Min TSO | `changed` | `hidden_right_axis` |
| Estimated Thread Usage and Limit | `changed` | `hidden_right_axis` |
| Active and Waiting Queries Count | `changed` | `hidden_right_axis` |
| Active and Waiting Tasks Count | `changed` | `hidden_right_axis` |
| Hard Limit Exceeded Count | `changed` | `hidden_right_axis` |
| Task Waiting Duration | `changed` | `rate_interval`, `duration_quantiles`, `yaxis_visible`, `hidden_right_axis`, `style_default` |

#### Min TSO

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Estimated Thread Usage and Limit

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Active and Waiting Queries Count

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Active and Waiting Tasks Count

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Hard Limit Exceeded Count

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Task Waiting Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `yaxis_visible`, `hidden_right_axis`, `style_default`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `labelY1`: `'Time'` → `None`
  - `showY2`: `True` → `False`
  - `minY1`: `None` → `'0'`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.80,max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clu...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_...` (legend `{{instance}}-{{resource_group}}-80`→`{{instance}}-{{resource_group}}-max`, hide True→True)
  - t1: `histogram_quantile(0.90,max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clu...` → `histogram_quantile(0.9999,sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_c...` (legend `{{instance}}-{{resource_group}}-90`→`{{instance}}-{{resource_group}}-9999`, hide True→False)
  - t2: `histogram_quantile(1.00,max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clu...` → `histogram_quantile(0.999,sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cl...` (legend `{{instance}}-{{resource_group}}-100`→`{{instance}}-{{resource_group}}-999`, hide False→True)
  - +3 extra target(s) in new

### DDL

| Panel | status | tags |
|---|---|---|
| Schema Internal DDL OPM | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Schema Apply OPM | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Schema Apply Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default`, `layout_repack` |

#### Schema Internal DDL OPM

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'4 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `avg(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))...` → `avg(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__ra...` (legend `{{type}}`→`{{type}}`, hide False→False)
  - t1: `sum(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))` → `sum(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__ra...` (legend `total`→`total`, hide False→False)
  - t2: `sum(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))...` → `sum(increase(tiflash_schema_internal_ddl_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__ra...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide True→True)

#### Schema Apply OPM

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `avg(increase(tiflash_schema_trigger_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(type)` → `avg(increase(tiflash_schema_trigger_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_in...` (legend `triggle-by-{{type}}`→`triggle-by-{{type}}`, hide False→False)

#### Schema Apply Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'5 series'` → `'7 series'`
  - `minY2`: `'0'` → `None`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$...` (legend `999-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` → `histogram_quantile(0.9999,sum(rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` → `histogram_quantile(0.999,sum(rate(tiflash_schema_apply_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` (legend `95-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

### Imbalance read/write

| Panel | status | tags |
|---|---|---|
| CPU Usage (irate) | `changed` | `rate_interval` |
| Segment Reader | `changed` | `rate_interval` |
| Request QPS by instance | `changed` | `rate_interval`, `hidden_right_axis` |
| Read Throughput by instance | `changed` | `rate_interval`, `hidden_right_axis` |
| Write Command OPS By Instance | `changed` | `rate_interval` |
| Write Throughput By Instance | `changed` | `rate_interval`, `hidden_right_axis` |

#### CPU Usage (irate)

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `irate(tiflash_proxy_process_cpu_seconds_total{instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])` → `irate(tiflash_proxy_process_cpu_seconds_total{instance=~"$tiflash_role",job=~".*tiflash",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_int...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Segment Reader

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"SegmentReader.*",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"SegmentReader.*",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)

#### Request QPS by instance

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_coprocessor_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(i...` → `sum(rate(tiflash_coprocessor_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

#### Read Throughput by instance

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` → `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` (legend `File Descriptor-{{instance}}`→`File Descriptor-{{instance}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_PSMReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_PSMReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `Page-{{instance}}`→`Page-{{instance}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_c...` → `sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_c...` (legend `PageBackGround-{{instance}}`→`PageBackGround-{{instance}}`, hide False→False)

#### Write Command OPS By Instance

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `write block-{{instance}}`→`write block-{{instance}}`, hide False→False)
  - t1: `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(i...` → `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

#### Write Throughput By Instance

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `showY2`: `False` → `True`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` (legend `write-{{instance}}`→`write-{{instance}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"inge...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"inge...` (legend `ingest-{{instance}}`→`ingest-{{instance}}`, hide False→False)

### Memory trace

| Panel | status | tags |
|---|---|---|
| Number of Keyspaces | `unchanged` | — |
| Number of Physical Tables | `unchanged` | — |
| Number of Segments | `unchanged` | — |
| Bytes of MemTables | `unchanged` | — |
| Mark Cache and Minmax Index Cache Memory Usage | `unchanged` | — |
| Effectiveness of Mark Cache | `unchanged` | — |
| Schema of Column File | `changed` | `rate_interval`, `hidden_right_axis` |
| Read Snapshots | `unchanged` | — |
| Memory by thread | `changed` | `hidden_right_axis` |
| Memory by thread (proxy) | `changed` | `hidden_right_axis` |
| Memory by class | `changed` | `hidden_right_axis` |
| KVStore memory | `changed` | `hidden_right_axis` |

#### Schema of Column File

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'4 series'` → `'4 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t1: `sum(rate(tiflash_shared_block_schemas{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"hit_coun...` → `sum(rate(tiflash_shared_block_schemas{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"hit_coun...` (legend `hit_count_ops-{{instance}}`→`hit_count_ops-{{instance}}`, hide False→False)
  - t3: `sum(rate(tiflash_shared_block_schemas{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"miss_cou...` → `sum(rate(tiflash_shared_block_schemas{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"miss_cou...` (legend `miss_count_ops-{{instance}}`→`miss_count_ops-{{instance}}`, hide False→False)

#### Memory by thread

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Memory by thread (proxy)

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Memory by class

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### KVStore memory

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

### Columnar Storage

| Panel | status | tags |
|---|---|---|
| IA usage | `changed` | `style_default` |
| IA Segments Memory Wait | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| IA Segment Remote Read Cache | `changed` | `rate_interval`, `hidden_right_axis` |
| IA Segments Remote Read Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| ColumnarFile Cache | `changed` | `rate_interval`, `hidden_right_axis` |
| Columnar Prefetch Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Columnar Prefetch Cache Hit Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Columnar Fetch Snapshot Retry | `changed` | `rate_interval`, `hidden_right_axis` |
| Columnar Fetch Snapshot Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Columnar Meta Cache | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Columnar Meta Cache Entries | `added` | `panel_split` |
| Columnar Meta Cache Weighted Size | `added` | `panel_split` |
| Columnar Meta Cache Gauge | `removed` | `panel_split` |

#### IA usage

- **status**: `changed`
- **tags**: `style_default`
- **field diffs**:
  - `nullPointMode`: `'null'` → `'null as zero'`

#### IA Segments Memory Wait

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance"...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance"...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$ti...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$ti...` (legend `9999 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tifl...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tif...` (legend `99 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### IA Segment Remote Read Cache

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` → `sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` (legend `cache-hit {{$additional_groupby}}`→`cache-hit {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` → `sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` (legend `cache-miss {{$additional_groupby}}`→`cache-miss {{$additional_groupby}}`, hide False→False)

#### IA Segments Remote Read Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_rol...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_rol...` (legend `9999 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role"...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role...` (legend `99 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### ColumnarFile Cache

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` → `sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` (legend `file-cache-hit {{$additional_groupby}}`→`file-cache-hit {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` → `sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` (legend `file-cache-miss {{$additional_groupby}}`→`file-cache-miss {{$additional_groupby}}`, hide False→False)

#### Columnar Prefetch Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$t...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$t...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8...` (legend `9999 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s...` (legend `99 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Columnar Prefetch Cache Hit Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_clust...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_clust...` (legend `9999 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluste...` (legend `99 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Columnar Fetch Snapshot Retry

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_retry_count{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cl...` → `sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_retry_count{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cl...` (legend `retry {{$additional_groupby}}`→`retry {{$additional_groupby}}`, hide False→False)

#### Columnar Fetch Snapshot Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instanc...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instanc...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_ro...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_ro...` (legend `9999 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_rol...` (legend `99 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Columnar Meta Cache

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.w`: `12` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` → `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_hit{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` (legend `hit {{$additional_groupby}}`→`hit {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` → `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_miss{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` (legend `miss {{$additional_groupby}}`→`miss {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_parse{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$ti...` → `sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_parse{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$ti...` (legend `parse {{$additional_groupby}}`→`parse {{$additional_groupby}}`, hide False→False)

#### Columnar Meta Cache Entries

- **status**: `added`
- **tags**: `panel_split`

#### Columnar Meta Cache Weighted Size

- **status**: `added`
- **tags**: `panel_split`

#### Columnar Meta Cache Gauge

- **status**: `removed`
- **tags**: `panel_split`

### Storage

| Panel | status | tags |
|---|---|---|
| Write Command OPS | `changed` | `rate_interval` |
| Write Amplification | `unchanged` | — |
| SubTasks Write Throughput (bytes) | `changed` | `rate_interval`, `hidden_right_axis` |
| SubTasks Write Throughput (rows) | `changed` | `rate_interval`, `hidden_right_axis` |
| Small Internal Tasks OPS | `changed` | `style_default` |
| Small Internal Tasks Duration | `changed` | `duration_quantiles` |
| Large Internal Tasks OPS | `changed` | `style_default` |
| Large Internal Tasks Duration | `changed` | `duration_quantiles` |
| Current Data Management Tasks | `changed` | `hidden_right_axis`, `layout_repack` |
| Opened File Count | `changed` | `layout_repack` |
| File Open OPS | `changed` | `rate_interval`, `layout_repack` |
| FSync Status | `changed` | `rate_interval`, `layout_repack` |
| Disk Write OPS | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Disk Read OPS | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Write flow | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Read flow | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Compression Ratio | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Compression Algorithm Count | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |

#### Write Command OPS

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(t...` → `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{type}}`→`{{type}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `write block`→`write block`, hide False→False)

#### SubTasks Write Throughput (bytes)

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `False` → `True`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_subtask_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_storage_subtask_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### SubTasks Write Throughput (rows)

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `False` → `True`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_subtask_throughput_rows{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]...` → `sum(rate(tiflash_storage_subtask_throughput_rows{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Small Internal Tasks OPS

- **status**: `changed`
- **tags**: `style_default`
- **field diffs**:
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`

#### Small Internal Tasks Duration

- **status**: `changed`
- **tags**: `duration_quantiles`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
- **target notes**:
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `99-{{type}} {{$additional_groupby}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Large Internal Tasks OPS

- **status**: `changed`
- **tags**: `style_default`
- **field diffs**:
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`

#### Large Internal Tasks Duration

- **status**: `changed`
- **tags**: `duration_quantiles`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
- **target notes**:
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `99-{{type}} {{$additional_groupby}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Current Data Management Tasks

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`

#### Opened File Count

- **status**: `changed`
- **tags**: `layout_repack`
- **field diffs**:
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `33` → `34`

#### File Open OPS

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `33` → `34`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_FileOpen{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))...` → `sum(rate(tiflash_system_profile_event_FileOpen{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__ra...` (legend `Open-{{instance}}`→`Open-{{instance}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_FileOpenFailed{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` → `sum(rate(tiflash_system_profile_event_FileOpenFailed{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` (legend `OpenFail-{{instance}}`→`OpenFail-{{instance}}`, hide False→False)

#### FSync Status

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `33` → `34`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_FileFSync{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_system_profile_event_FileFSync{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__r...` (legend `ops-fsync-{{instance}}`→`ops-fsync-{{instance}}`, hide False→False)

#### Disk Write OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `40` → `42`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_PSMWriteIOCalls{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"...` → `sum(rate(tiflash_system_profile_event_PSMWriteIOCalls{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"...` (legend `Page`→`Page`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_PSMWritePages{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `sum(rate(tiflash_system_profile_event_PSMWritePages{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` (legend `PageFile`→`PageFile`, hide True→True)
  - t2: `sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` → `sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` (legend `File Descriptor`→`File Descriptor`, hide False→False)

#### Disk Read OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `40` → `42`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_PSMReadIOCalls{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` → `sum(rate(tiflash_system_profile_event_PSMReadIOCalls{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` (legend `Page`→`Page`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_PSMReadPages{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_PSMReadPages{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `PageFile`→`PageFile`, hide True→True)
  - t2: `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorRead{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluste...` → `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorRead{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluste...` (legend `File Descriptor`→`File Descriptor`, hide False→False)

#### Write flow

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.y(relative)`: `47` → `50`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` → `sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` (legend `File Descriptor`→`File Descriptor`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_PSMWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `sum(rate(tiflash_system_profile_event_PSMWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` (legend `Page`→`Page`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_PSMBackgroundWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_...` → `sum(rate(tiflash_system_profile_event_PSMBackgroundWriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_...` (legend `PageBackGround`→`PageBackGround`, hide False→False)

#### Read flow

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.y(relative)`: `47` → `50`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` → `sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` (legend `File Descriptor`→`File Descriptor`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_PSMReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_PSMReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `Page`→`Page`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_c...` → `sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_c...` (legend `PageBackGround`→`PageBackGround`, hide False→False)

#### Compression Ratio

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `55` → `58`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_pack_compression_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=...` → `sum(rate(tiflash_storage_pack_compression_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=...` (legend `lz4`→`lz4`, hide False→False)
  - t1: `sum(rate(tiflash_storage_pack_compression_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=...` → `sum(rate(tiflash_storage_pack_compression_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=...` (legend `lightweight`→`lightweight`, hide False→False)

#### Compression Algorithm Count

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `55` → `58`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_pack_compression_algorithm_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clus...` → `sum(rate(tiflash_storage_pack_compression_algorithm_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clus...` (legend `{{type}}`→`{{type}}`, hide False→False)

### Storage Read Pool & Data Sharing

| Panel | status | tags |
|---|---|---|
| Read Tasks OPS | `changed` | `rate_interval`, `hidden_right_axis` |
| Read Snapshots | `unchanged` | — |
| Read Thread Internal Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default` |
| Read Thread Scheduling | `changed` | `rate_interval` |
| Data Sharing | `changed` | `rate_interval`, `hidden_right_axis`, `style_default` |
| Segment MergedTask | `unchanged` | — |
| Segment MergedTask Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default` |
| VersionChain | `changed` | `rate_interval`, `duration_quantiles`, `style_default` |
| DeltaIndexError | `changed` | `rate_interval` |

#### Read Tasks OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_read_tasks_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(in...` → `sum(rate(tiflash_storage_read_tasks_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_in...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Read Thread Internal Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cl...` (legend `999-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` → `histogram_quantile(0.9999,sum(rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clu...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` → `histogram_quantile(0.999,sum(rate(tiflash_read_thread_internal_us_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` (legend `95-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### Read Thread Scheduling

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_read_thread_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"r...` → `sum(rate(tiflash_storage_read_thread_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"r...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Data Sharing

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `minY2`: `'0'` → `None`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_read_thread_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"a...` → `sum(rate(tiflash_storage_read_thread_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"a...` (legend `{{type}}`→`{{type}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_column_cache_packs{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"da...` → `sum(rate(tiflash_storage_column_cache_packs{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"da...` (legend `data_sharing_cache_hit_ratio`→`data_sharing_cache_hit_ratio`, hide False→False)
  - t2: `sum(rate(tiflash_storage_column_cache_packs{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"ex...` → `sum(rate(tiflash_storage_column_cache_packs{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"ex...` (legend `extra_column_cache_hit_ratio`→`extra_column_cache_hit_ratio`, hide True→True)

#### Segment MergedTask Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8...` (legend `999-{{type}} {{$additional_groupby}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` (legend `99-{{type}} {{$additional_groupby}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.80,sum(rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_read_thread_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_...` (legend `80-{{type}} {{$additional_groupby}}`→`999-{{type}} {{$additional_groupby}}`, hide True→True)
  - +3 extra target(s) in new

#### VersionChain

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clu...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_c...` (legend `999-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cl...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide True→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clus...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_version_chain_ms_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clu...` (legend `95-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### DeltaIndexError

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_DTDeltaIndexError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluste...` → `sum(rate(tiflash_system_profile_event_DTDeltaIndexError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluste...` (legend `DeltaIndexError-{{instance}}`→`DeltaIndexError-{{instance}}`, hide False→False)

### PageStorage

| Panel | status | tags |
|---|---|---|
| PageStorage Disk Usage | `unchanged` | — |
| PageStorage File Num | `changed` | `hidden_right_axis` |
| PageStorage WriteBatch Size | `changed` | `rate_interval` |
| Page write Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Page GC Tasks OPM | `changed` | `hidden_right_axis` |
| Page GC Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Numer of Pages | `changed` | `hidden_right_axis` |
| PageStorage Pending Writers Num | `unchanged` | — |
| PageStorage stored bytes by type | `changed` | `hidden_right_axis` |
| Number of Tables | `changed` | `hidden_right_axis` |
| PS Command OPS By Instance | `changed` | `rate_interval`, `layout_repack` |
| PS Apply edits OPS By Instance | `changed` | `rate_interval`, `layout_repack` |

#### PageStorage File Num

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `formatY2`: `'percentunit'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`

#### PageStorage WriteBatch Size

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(delta(tiflash_storage_page_write_batch_size_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_storage_page_write_batch_size_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Page write Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'5 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clus...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clus...` (legend `{{type}}-max`→`{{type}}-max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.999,sum(rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clust...` (legend `{{type}}-999`→`{{type}}-9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_page_write_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` (legend `{{type}}-99`→`{{type}}-999 {{$additional_groupby}}`, hide True→True)
  - +1 extra target(s) in new

#### Page GC Tasks OPM

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Page GC Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_page_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_page_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` (legend `{{type}}-max`→`{{type}}-max {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` (legend `{{type}}-99`→`{{type}}-9999 {{$additional_groupby}}`, hide True→False)
  - +4 extra target(s) in new

#### Numer of Pages

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### PageStorage stored bytes by type

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`

#### Number of Tables

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### PS Command OPS By Instance

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `9` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_page_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(...` → `sum(rate(tiflash_storage_page_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

#### PS Apply edits OPS By Instance

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `9` → `8`
  - `gridPos.y(relative)`: `49` → `48`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_page_apply_edit_type{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))b...` → `sum(rate(tiflash_storage_page_apply_edit_type{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rat...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

### Rate Limiter

| Panel | status | tags |
|---|---|---|
| I/O Limiter Throughput | `changed` | `rate_interval`, `hidden_right_axis` |
| I/O Limiter Threshold | `changed` | `hidden_right_axis` |
| I/O Limiter Current Pending Gauge | `unchanged` | — |
| I/O Limiter Pending OPS | `changed` | `rate_interval` |
| I/O Limiter Pending Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`, `yaxis_visible` |

#### I/O Limiter Throughput

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_io_limiter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(instance...` → `sum(rate(tiflash_storage_io_limiter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

#### I/O Limiter Threshold

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### I/O Limiter Pending OPS

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_io_limiter_pending_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_storage_io_limiter_pending_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

#### I/O Limiter Pending Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`, `yaxis_visible`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `formatY2`: `'s'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY1`: `None` → `'0'`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clust...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clust...` (legend `{{type}}-pending-max`→`{{type}}-pending-max`, hide True→True)
  - t1: `histogram_quantile(0.999,sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` (legend `{{type}}-pending-P999`→`{{type}}-pending-9999`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster...` (legend `{{type}}-pending-P99`→`{{type}}-pending-999`, hide False→True)
  - +3 extra target(s) in new

### Storage Write Stall

| Panel | status | tags |
|---|---|---|
| Write Stall Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Write & Delta Management Throughput | `changed` | `rate_interval` |
| Write & Delta Management Total | `unchanged` | — |
| Write Throughput By Instance | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Write Command OPS By Instance | `changed` | `rate_interval`, `layout_repack` |

#### Write Stall Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'6 series'`
  - `formatY2`: `'s'` → `'short'`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `histogram_quantile(0.99,sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluste...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_write_stall_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clu...` (legend `99-{{type}}-{{instance}}`→`max-{{type}}-{{instance}}`, hide True→True)
  - t1: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_write_stall_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_clu...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clus...` (legend `max-{{type}}-{{instance}}`→`9999-{{type}}-{{instance}}`, hide False→False)
  - +4 extra target(s) in new

#### Write & Delta Management Throughput

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` (legend `write+ingest`→`write+ingest`, hide False→False)
  - t1: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type!~"writ...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type!~"writ...` (legend `ManageDelta`→`ManageDelta`, hide False→False)

#### Write Throughput By Instance

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `showY2`: `False` → `True`
  - `gridPos.h`: `9` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"writ...` (legend `write-{{instance}}`→`write-{{instance}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"inge...` → `sum(rate(tiflash_storage_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"inge...` (legend `ingest-{{instance}}`→`ingest-{{instance}}`, hide False→False)

#### Write Command OPS By Instance

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `9` → `8`
  - `gridPos.y(relative)`: `25` → `24`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_DMWriteBlock{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `write block-{{instance}}`→`write block-{{instance}}`, hide False→False)
  - t1: `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(i...` → `sum(increase(tiflash_storage_command_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)

### Raft

| Panel | status | tags |
|---|---|---|
| Stale Read OPS | `changed` | `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Raft Read Index OPS | `changed` | `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Learner Read Failures | `changed` | `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Read Index Events | `changed` | `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Raft Wait Index Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Raft Batch Read Index Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Apply Raft write logs Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Region write Duration (decode) | `changed` | `rate_interval`, `layout_repack` |
| Region write Duration (write blocks) | `changed` | `rate_interval`, `layout_repack` |
| Apply Raft write logs Duration [Heatmap] | `changed` | `rate_interval`, `layout_repack` |
| Apply Raft admin logs Duration [Heatmap] | `changed` | `rate_interval`, `layout_repack` |
| Raft Events QPS | `changed` | `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Raft Frequent Events QPS | `changed` | `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Raft Log Gap Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Raft Entry Batch Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Region Size (by event) Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Big Write To Region Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Write Committed Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Raft Eager GC OPS | `changed` | `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack` |
| Raft Eager GC Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Keys flow | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Raft throughput | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |
| Upstream Latency [Heatmap] | `changed` | `rate_interval`, `layout_repack` |
| Upstream Latency | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Log Replication Rejected | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |

#### Stale Read OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_stale_read_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(instance)` → `sum(rate(tiflash_stale_read_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval])...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Raft Read Index OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_read_index_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(insta...` → `sum(rate(tiflash_raft_read_index_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_inter...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

#### Learner Read Failures

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_learner_read_failures_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_raft_learner_read_failures_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Read Index Events

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_read_index_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))b...` → `sum(rate(tiflash_raft_read_index_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rat...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Raft Wait Index Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'5 series'` → `'7 series'`
  - `minY2`: `'0'` → `None`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.9999,sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` (legend `99 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.999,sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `95 {{$additional_groupby}}`→`999 {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### Raft Batch Read Index Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.9999,sum(rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster"...` (legend `99 {{$additional_groupby}}`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.999,sum(rate(tiflash_raft_read_index_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `95`→`999 {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### Apply Raft write logs Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'6 series'` → `'10 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8...` (legend ` 100%-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_c...` → `histogram_quantile(0.9999,sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` (legend ` 99%-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb...` → `histogram_quantile(0.999,sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_...` (legend `avg-write`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +4 extra target(s) in new

#### Region write Duration (decode)

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster=...` → `sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster=...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Region write Duration (write blocks)

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster=...` → `sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster=...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Apply Raft write logs Duration [Heatmap]

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` → `sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Apply Raft admin logs Duration [Heatmap]

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` → `sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Raft Events QPS

- **status**: `changed`
- **tags**: `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_raft_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_inte...` → `sum(rate(tiflash_raft_raft_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_inte...` (legend `{{instance}}`→`{{type}}`, hide False→False)

#### Raft Frequent Events QPS

- **status**: `changed`
- **tags**: `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_raft_frequent_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__...` → `sum(rate(tiflash_raft_raft_frequent_events_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__...` (legend `{{instance}}`→`{{type}}`, hide False→False)

#### Raft Log Gap Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_raft_log_gap_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` → `sum(delta(tiflash_raft_raft_log_gap_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` (legend `{{le}}`→`{{le}}`, hide False→False)
  - t1: `sum(delta(tiflash_raft_raft_log_gap_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` → `sum(delta(tiflash_raft_raft_log_gap_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Raft Entry Batch Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `49` → `56`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_entry_size_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"norma...` → `sum(delta(tiflash_raft_entry_size_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"norma...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Region Size (by event) Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `56` → `64`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_region_flush_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` → `sum(delta(tiflash_raft_region_flush_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` (legend `{{le}}`→`{{le}}`, hide False→False)
  - t1: `sum(delta(tiflash_raft_region_flush_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` → `sum(delta(tiflash_raft_region_flush_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type...` (legend `{{le}}`→`{{le}}`, hide True→True)

#### Big Write To Region Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `56` → `64`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` → `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Write Committed Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `63` → `72`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` → `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Raft Eager GC OPS

- **status**: `changed`
- **tags**: `legend_or_hide`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `legend_alignAsTable`: `False` → `True`
  - `legend_rightSide`: `False` → `True`
  - `legend_values`: `False` → `True`
  - `legend_current`: `False` → `True`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `70` → `80`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_eager_gc_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interva...` → `sum(rate(tiflash_raft_eager_gc_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interva...` (legend `{{instance}}`→`{{type}}`, hide False→False)

#### Raft Eager GC Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `70` → `80`
- **target notes**:
  - t0: `histogram_quantile(0.99,sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_eager_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="...` (legend ` 99%-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.95,sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(0.9999,sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` (legend `95%-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide True→False)
  - t2: `sum(rate(tiflash_raft_eager_gc_duration_seconds_sum{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `histogram_quantile(0.999,sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` (legend `avg-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +2 extra target(s) in new

#### Keys flow

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `77` → `88`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_process_keys{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(type)` → `sum(rate(tiflash_raft_process_keys{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval]...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Raft throughput

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `84` → `96`
- **target notes**:
  - t0: `sum(rate(tiflash_raft_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(type)` → `sum(rate(tiflash_raft_throughput_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_inter...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Upstream Latency [Heatmap]

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `91` → `104`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))...` → `sum(delta(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__ra...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Upstream Latency

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'4 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `91` → `104`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clus...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clus...` (legend ` 100%`→`max {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster...` → `histogram_quantile(0.9999,sum(rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` (legend ` 99%`→`9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.95,sum(rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster...` → `histogram_quantile(0.999,sum(rate(tiflash_raft_upstream_latency_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluste...` (legend `95%`→`999 {{$additional_groupby}}`, hide True→True)
  - +2 extra target(s) in new

#### Log Replication Rejected

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `98` → `112`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_tikv_server_raft_append_rejects{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(instance)` → `sum(rate(tiflash_proxy_tikv_server_raft_append_rejects{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval]))...` (legend `{{instance}}`→`{{instance}}`, hide False→False)

### Raft Snapshot / IngestSST

| Panel | status | tags |
|---|---|---|
| Heavy Raft Apply Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack` |
| Applying snapshots Count | `changed` | `hidden_right_axis`, `layout_repack` |
| Snapshot Uncommitted Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Ongoing raft snapshot | `changed` | `hidden_right_axis`, `layout_repack` |
| Snapshot Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Snapshot Predecode Duration | `changed` | `rate_interval`, `layout_repack` |
| Snapshot Prehandle Throughput Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Snapshot Flush Duration | `changed` | `rate_interval`, `layout_repack` |
| Ingest Uncommitted Size Heatmap | `changed` | `rate_interval`, `layout_repack` |
| Snapshot Predecode SST to DT Duration | `changed` | `rate_interval`, `layout_repack` |
| Ingest SST Duration | `changed` | `rate_interval`, `layout_repack` |

#### Heavy Raft Apply Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'6 series'`
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `histogram_quantile(0.99,sum(rate(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$...` (legend `99%-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - +5 extra target(s) in new

#### Applying snapshots Count

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `7` → `8`

#### Snapshot Uncommitted Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` → `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Ongoing raft snapshot

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `14` → `16`

#### Snapshot Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_snapshot_total_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",ty...` → `sum(delta(tiflash_raft_snapshot_total_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",ty...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Snapshot Predecode Duration

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `21` → `24`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Snapshot Prehandle Throughput Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_command_throughput_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` → `sum(delta(tiflash_raft_command_throughput_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Snapshot Flush Duration

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `28` → `32`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Ingest Uncommitted Size Heatmap

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` → `sum(delta(tiflash_raft_write_flow_bytes_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Snapshot Predecode SST to DT Duration

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `35` → `40`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

#### Ingest SST Duration

- **status**: `changed`
- **tags**: `rate_interval`, `layout_repack`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `42` → `48`
- **target notes**:
  - t0: `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_raft_command_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

### Rough Set Filter Rate Histogram

| Panel | status | tags |
|---|---|---|
| Rough Set Filter Rate | `changed` | `rate_interval` |
| Rough Set Filter Rate Histogram | `changed` | `rate_interval` |

#### Rough Set Filter Rate

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'5 series'` → `'5 series'`
- **target notes**:
  - t0: `avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` → `avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` (legend `1min-{{instance}}`→`1min-{{instance}}`, hide False→False)
  - t1: `avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` → `avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$...` (legend `5min-{{instance}}`→`5min-{{instance}}`, hide True→True)
  - t2: `sum(rate(tiflash_system_profile_event_DMFileFilterNoFilter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` → `sum(rate(tiflash_system_profile_event_DMFileFilterNoFilter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` (legend `No Filter-{{instance}}`→`No Filter-{{instance}}`, hide True→True)

#### Rough Set Filter Rate Histogram

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(delta(tiflash_storage_rough_set_filter_rate_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(delta(tiflash_storage_rough_set_filter_rate_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `{{le}}`→`{{le}}`, hide False→False)

### Disaggregated-Write

| Panel | status | tags |
|---|---|---|
| Checkpoint Upload Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Checkpoint Upload flow | `changed` | `rate_interval`, `hidden_right_axis` |
| Checkpoint Upload keys speed by type (all) | `changed` | `rate_interval`, `style_default`, `hidden_right_axis` |
| Checkpoint Upload flow by type (incremental+compaction) | `changed` | `rate_interval`, `hidden_right_axis` |
| Remote File Num | `changed` | `hidden_right_axis` |
| Remote Store Usage | `unchanged` | — |
| Remote Object Lock Request QPS | `changed` | `rate_interval`, `style_default`, `hidden_right_axis` |
| Remote Object Lock Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis` |
| Remote Store Summary | `changed` | `hidden_right_axis` |
| Remote GC Duration Breakdown | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`, `layout_repack` |
| Remote GC Status | `changed` | `hidden_right_axis`, `layout_repack` |
| Local Lock Manager status | `changed` | `hidden_right_axis` |
| Local Lock Manager QPS | `changed` | `rate_interval`, `style_default`, `hidden_right_axis` |
| FAP result | `changed` | `rate_interval` |
| FAP state | `changed` | `rate_interval` |
| FAP time by stage | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default` |
| FAP no match reason | `changed` | `rate_interval` |

#### Checkpoint Upload Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` (legend `{{type}} {{$additional_groupby}}`→`{{type}}-max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.999,sum(rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_...` (legend `{{type}}-999 {{$additional_groupby}}`→`{{type}}-9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cl...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_checkpoint_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` (legend `{{type}}-99 {{$additional_groupby}}`→`{{type}}-999 {{$additional_groupby}}`, hide True→True)
  - +3 extra target(s) in new

#### Checkpoint Upload flow

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_checkpoint_flow{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="increm...` → `sum(rate(tiflash_storage_checkpoint_flow{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="increm...` (legend `incremental {{$additional_groupby}}`→`incremental {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_checkpoint_flow{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="compac...` → `sum(rate(tiflash_storage_checkpoint_flow{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="compac...` (legend `compaction {{$additional_groupby}}`→`compaction {{$additional_groupby}}`, hide False→False)

#### Checkpoint Upload keys speed by type (all)

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_checkpoint_keys_by_types{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_storage_checkpoint_keys_by_types{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### Checkpoint Upload flow by type (incremental+compaction)

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_checkpoint_flow_by_types{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_storage_checkpoint_flow_by_types{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### Remote File Num

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `formatY2`: `'percentunit'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`

#### Remote Object Lock Request QPS

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_disaggregated_object_lock_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` → `sum(rate(tiflash_disaggregated_object_lock_request_count{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### Remote Object Lock Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `legend_current`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.99,sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluste...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash...` (legend `99%-{{type}} {{$additional_groupby}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - +5 extra target(s) in new

#### Remote Store Summary

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Remote GC Duration Breakdown

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `formatY2`: `'s'` → `'short'`
  - `legend_current`: `False` → `True`
  - `gridPos.w`: `9` → `8`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_storage_s3_gc_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluste...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_s3_gc_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clus...` (legend `99%-{{type}} {{$additional_groupby}}`→`max-{{type}} {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_s3_gc_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_s3_gc_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_clust...` (legend `90%-{{type}} {{$additional_groupby}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - +4 extra target(s) in new

#### Remote GC Status

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.x`: `17` → `16`
  - `gridPos.w`: `7` → `8`

#### Local Lock Manager status

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Local Lock Manager QPS

- **status**: `changed`
- **tags**: `rate_interval`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `formatY2`: `'none'` → `'short'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_s3_lock_mgr_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by...` → `sum(rate(tiflash_storage_s3_lock_mgr_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### FAP result

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_fap_task_result{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by($additional...` → `sum(rate(tiflash_fap_task_result{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval]))...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### FAP state

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_fap_task_state{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by($additional_...` → `sum(rate(tiflash_fap_task_state{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval]))b...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### FAP time by stage

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`, `style_default`
- **field diffs**:
  - `targets`: `'1 series'` → `'6 series'`
  - `formatY2`: `'percentunit'` → `'short'`
  - `minY2`: `'0'` → `None`
  - `legend_max`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(round(1000000000*rate(tiflash_fap_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_fap_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_...` (legend `{{type}} {{$additional_groupby}}`→`{{type}}-max {{$additional_groupby}}`, hide False→True)
  - +5 extra target(s) in new

#### FAP no match reason

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_fap_nomatch_reason{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by($additio...` → `sum(rate(tiflash_fap_nomatch_reason{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interval...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

### Disaggregated-Compute

| Panel | status | tags |
|---|---|---|
| Read Duration Breakdown | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis` |
| Remote Cache Operations | `changed` | `rate_interval`, `hidden_right_axis` |
| Remote Cache Flow | `changed` | `rate_interval` |
| Remote Cache BG Download Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis` |
| Remote Cache Wait on Downloading Duration | `changed` | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis` |
| Remote Cache Wait on Downloading OPS | `changed` | `rate_interval` |
| Remote Cache Wait on Downloading Flow | `changed` | `rate_interval` |
| Remote Cache Gauge | `changed` | `hidden_right_axis` |
| Remote Cache Reject Download Type OPS | `changed` | `rate_interval` |
| Remote Cache Usage | `unchanged` | — |
| Memory Usage of Storage Tasks | `unchanged` | — |
| MVCCIndexCache | `changed` | `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack` |
| PlaceIndex Tasks Duration | `changed` | `duration_quantiles`, `layout_repack` |
| PlaceIndexTask/Reuse OPS | `changed` | `layout_repack` |
| PlaceIndex update rows/deletes | `changed` | `rate_interval`, `hidden_right_axis`, `layout_repack` |

#### Read Duration Breakdown

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `legend_current`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.99,sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cl...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s...` (legend `99%-{{type}} {{$additional_groupby}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - +5 extra target(s) in new

#### Remote Cache Operations

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by($addit...` → `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_interv...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"dtfile_h...` → `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"dtfile_h...` (legend `dtfile_cache_hit_ratio`→`dtfile_cache_hit_ratio`, hide False→False)
  - t2: `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"page_hit...` → `sum(rate(tiflash_storage_remote_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"page_hit...` (legend `page_cache_hit_ratio`→`page_cache_hit_ratio`, hide False→False)

#### Remote Cache Flow

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_remote_cache_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(...` → `sum(rate(tiflash_storage_remote_cache_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_...` (legend `{{type}} {{$additional_groupby}}`→`{{type}} {{$additional_groupby}}`, hide False→False)

#### Remote Cache BG Download Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{instance=~"$instance",instance=~"$tiflash_rol...` (legend `999%-{{stage}}-{{file_type}} {{$additional_groupby}}`→`max-{{stage}}-{{file_type}} {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `99%-{{stage}}-{{file_type}} {{$additional_groupby}}`→`9999-{{stage}}-{{file_type}} {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_sum{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="...` (legend `avg-{{stage}}-{{file_type}} {{$additional_groupby}}`→`999-{{stage}}-{{file_type}} {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Remote Cache Wait on Downloading Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'2 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `legend_current`: `False` → `True`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{instance=~"$instance",instance=~"$tiflash_r...` (legend `999%-{{result}}-{{file_type}} {{$additional_groupby}}`→`max-{{result}}-{{file_type}} {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluste...` (legend `99%-{{result}}-{{file_type}} {{$additional_groupby}}`→`9999-{{result}}-{{file_type}} {{$additional_groupby}}`, hide False→False)
  - +4 extra target(s) in new

#### Remote Cache Wait on Downloading OPS

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_remote_cache_wait_on_downloading_result{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$ti...` → `sum(rate(tiflash_storage_remote_cache_wait_on_downloading_result{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$ti...` (legend `{{result}}-{{file_type}} {{$additional_groupby}}`→`{{result}}-{{file_type}} {{$additional_groupby}}`, hide False→False)

#### Remote Cache Wait on Downloading Flow

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_remote_cache_wait_on_downloading_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` → `sum(rate(tiflash_storage_remote_cache_wait_on_downloading_bytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tid...` (legend `{{result}}-{{file_type}} {{$additional_groupby}}`→`{{result}}-{{file_type}} {{$additional_groupby}}`, hide False→False)

#### Remote Cache Gauge

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Remote Cache Reject Download Type OPS

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_remote_cache_reject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by...` → `sum(rate(tiflash_storage_remote_cache_reject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate...` (legend `{{reason}}-{{file_type}} {{$additional_groupby}}`→`{{reason}}-{{file_type}} {{$additional_groupby}}`, hide False→False)

#### MVCCIndexCache

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `style_default`, `layout_repack`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
  - `minY2`: `'0'` → `None`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_mvcc_index_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m]))by(in...` → `sum(rate(tiflash_storage_mvcc_index_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__rate_in...` (legend `{{type}}-{{instance}}`→`{{type}}-{{instance}}`, hide False→False)
  - t1: `sum(rate(tiflash_storage_mvcc_index_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"hit"...` → `sum(rate(tiflash_storage_mvcc_index_cache{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type=~"hit"...` (legend `hit_ratio-{{instance}}`→`hit_ratio-{{instance}}`, hide False→False)

#### PlaceIndex Tasks Duration

- **status**: `changed`
- **tags**: `duration_quantiles`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_subtask_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `99-{{type}} {{$additional_groupby}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### PlaceIndexTask/Reuse OPS

- **status**: `changed`
- **tags**: `layout_repack`
- **field diffs**:
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `55` → `56`

#### PlaceIndex update rows/deletes

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
  - `gridPos.h`: `7` → `8`
  - `gridPos.y(relative)`: `55` → `56`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `max {{$additional_groupby}}`→`max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_storage_place_index_stats_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` → `histogram_quantile(0.99,sum(rate(tiflash_storage_place_index_stats_count_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` (legend `99-{{type}} {{$additional_groupby}}`→`99-{{type}} {{$additional_groupby}}`, hide True→True)
  - t2: `sum(rate(tiflash_storage_place_index_stats_count_sum{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` → `sum(rate(tiflash_storage_place_index_stats_count_sum{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}...` (legend `avg-{{type}} {{$additional_groupby}}`→`avg-{{type}} {{$additional_groupby}}`, hide False→False)

### S3

| Panel | status | tags |
|---|---|---|
| S3 Bytes | `changed` | `rate_interval`, `hidden_right_axis` |
| S3 OPS | `changed` | `rate_interval`, `hidden_right_axis` |
| S3 Retry OPS | `changed` | `rate_interval`, `hidden_right_axis` |
| S3 Request Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| S3 HTTP OPS | `changed` | `rate_interval`, `hidden_right_axis` |
| S3 HTTP Request Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| S3 on-going instances | `changed` | `hidden_right_axis` |
| S3RandomAccessFile OPS | `changed` | `rate_interval`, `hidden_right_axis` |

#### S3 Bytes

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'3 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_S3WriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_S3WriteBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `S3WriteBytes {{$additional_groupby}}`→`S3WriteBytes {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_S3ReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_system_profile_event_S3ReadBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `S3ReadBytes {{$additional_groupby}}`→`S3ReadBytes {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_S3WriteDMFileBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` → `sum(rate(tiflash_system_profile_event_S3WriteDMFileBytes{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clust...` (legend `S3WriteDMFileBytes {{$additional_groupby}}`→`S3WriteDMFileBytes {{$additional_groupby}}`, hide False→False)

#### S3 OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'12 series'` → `'12 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_S3PutObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_system_profile_event_S3PutObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `S3PutObject {{$additional_groupby}}`→`S3PutObject {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_S3GetObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m...` → `sum(rate(tiflash_system_profile_event_S3GetObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$_...` (legend `S3GetObject {{$additional_groupby}}`→`S3GetObject {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_S3HeadObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1...` → `sum(rate(tiflash_system_profile_event_S3HeadObject{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$...` (legend `S3HeadObject {{$additional_groupby}}`→`S3HeadObject {{$additional_groupby}}`, hide False→False)

#### S3 Retry OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'6 series'` → `'6 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_S3GetObjectRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(rate(tiflash_system_profile_event_S3GetObjectRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `S3GetObjectRetry {{$additional_groupby}}`→`S3GetObjectRetry {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_S3PutObjectRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(rate(tiflash_system_profile_event_S3PutObjectRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `S3PutObjectRetry {{$additional_groupby}}`→`S3PutObjectRetry {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_S3PutDMFileRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(rate(tiflash_system_profile_event_S3PutDMFileRetry{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `S3PutDMFileRetry {{$additional_groupby}}`→`S3PutDMFileRetry {{$additional_groupby}}`, hide False→False)

#### S3 Request Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s...` (legend `{{type}}-max {{$additional_groupby}}`→`{{type}}-max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_...` (legend `{{type}}-9999 {{$additional_groupby}}`→`{{type}}-9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cl...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_s3_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_c...` (legend `{{type}}-99 {{$additional_groupby}}`→`{{type}}-999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### S3 HTTP OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'10 series'` → `'10 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_S3ReadRequestsCount{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clus...` → `sum(rate(tiflash_system_profile_event_S3ReadRequestsCount{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clus...` (legend `read-count {{$additional_groupby}}`→`read-count {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_S3WriteRequestsCount{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` → `sum(rate(tiflash_system_profile_event_S3WriteRequestsCount{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` (legend `write-count {{$additional_groupby}}`→`write-count {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_S3ReadRequestsErrors{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` → `sum(rate(tiflash_system_profile_event_S3ReadRequestsErrors{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_clu...` (legend `read-error {{$additional_groupby}}`→`read-error {{$additional_groupby}}`, hide False→False)

#### S3 HTTP Request Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `{{type}}-max {{$additional_groupby}}`→`{{type}}-max {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.9999,sum(rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` → `histogram_quantile(0.9999,sum(rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",...` (legend `{{type}}-9999 {{$additional_groupby}}`→`{{type}}-9999 {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` → `histogram_quantile(0.999,sum(rate(tiflash_storage_s3_http_request_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` (legend `{{type}}-99 {{$additional_groupby}}`→`{{type}}-999 {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### S3 on-going instances

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`

#### S3RandomAccessFile OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'5 series'` → `'5 series'`
  - `formatY2`: `'opm'` → `'short'`
  - `showY2`: `True` → `False`
  - `minY2`: `'0'` → `None`
- **target notes**:
  - t0: `sum(rate(tiflash_system_profile_event_S3IOReadError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `sum(rate(tiflash_system_profile_event_S3IOReadError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` (legend `S3IOReadError {{$additional_groupby}}`→`S3IOReadError {{$additional_groupby}}`, hide False→False)
  - t1: `sum(rate(tiflash_system_profile_event_S3IOSeekError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` → `sum(rate(tiflash_system_profile_event_S3IOSeekError{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[...` (legend `S3IOSeekError {{$additional_groupby}}`→`S3IOSeekError {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_system_profile_event_S3IOSeekBackward{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` → `sum(rate(tiflash_system_profile_event_S3IOSeekBackward{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster...` (legend `S3IOSeekBackward {{$additional_groupby}}`→`S3IOSeekBackward {{$additional_groupby}}`, hide False→False)

### Pipeline Model

| Panel | status | tags |
|---|---|---|
| Task Thread Pool Size | `changed` | `hidden_right_axis` |
| Task Count | `changed` | `hidden_right_axis` |
| Task Status Change OPS | `changed` | `rate_interval`, `hidden_right_axis` |
| Task Duration | `changed` | `rate_interval`, `duration_quantiles`, `hidden_right_axis` |
| Task Max Execute Time Per Round | `changed` | `rate_interval`, `hidden_right_axis` |
| Threads CPU of CPU Task Thread Pool | `changed` | `rate_interval` |
| Threads CPU of IO Task Thread Pool | `changed` | `rate_interval` |
| Threads CPU of Wait Reactor | `changed` | `rate_interval` |
| Wait notify task details | `changed` | `hidden_right_axis` |

#### Task Thread Pool Size

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Task Count

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

#### Task Status Change OPS

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'1 series'` → `'1 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `sum(rate(tiflash_pipeline_task_change_to_status{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[1m])...` → `sum(rate(tiflash_pipeline_task_change_to_status{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster"}[$__r...` (legend `{{type}}`→`{{type}}`, hide False→False)

#### Task Duration

- **status**: `changed`
- **tags**: `rate_interval`, `duration_quantiles`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'8 series'` → `'11 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="...` (legend `95-{{type}}`→`max-{{type}} {{$additional_groupby}}`, hide False→True)
  - t1: `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(0.9999,sum(rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",t...` (legend `99-{{type}}`→`9999-{{type}} {{$additional_groupby}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tid...` → `histogram_quantile(0.999,sum(rate(tiflash_pipeline_task_duration_seconds_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",ti...` (legend `999-{{type}}`→`999-{{type}} {{$additional_groupby}}`, hide False→True)
  - +3 extra target(s) in new

#### Task Max Execute Time Per Round

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'6 series'` → `'6 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t0: `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `95-{{type}}`→`95-{{type}}`, hide False→False)
  - t1: `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(0.95,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `99-{{type}}`→`99-{{type}}`, hide False→False)
  - t2: `histogram_quantile(0.99,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` → `histogram_quantile(0.99,sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster=...` (legend `999-{{type}}`→`999-{{type}}`, hide False→False)

#### Threads CPU of CPU Task Thread Pool

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"cpu_pool",tidb_cluste...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"cpu_pool",tidb_cluster="$tidb_cluster"}[$__rate_i...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"cpu_pool",tidb_cluster="...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"cpu_pool",tidb_cluster="$tidb_cluster"})by(instance)` (legend `Limit`→`Limit`, hide False→False)

#### Threads CPU of IO Task Thread Pool

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"io_pool",tidb_cluster...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"io_pool",tidb_cluster="$tidb_cluster"}[$__rate_in...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"io_pool",tidb_cluster="$...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"io_pool",tidb_cluster="$tidb_cluster"})by(instance)` (legend `Limit`→`Limit`, hide False→False)

#### Threads CPU of Wait Reactor

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'2 series'` → `'2 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"WaitReactor",tidb_clu...` → `sum(rate(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"WaitReactor",tidb_cluster="$tidb_cluster"}[$__rat...` (legend `{{name}} {{instance}}`→`{{name}} {{instance}}`, hide False→False)
  - t1: `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"WaitReactor",tidb_cluste...` → `count(tiflash_proxy_thread_cpu_seconds_total{instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",name=~"WaitReactor",tidb_cluster="$tidb_cluster"})by(instance)` (legend `Limit`→`Limit`, hide False→False)

#### Wait notify task details

- **status**: `changed`
- **tags**: `hidden_right_axis`
- **field diffs**:
  - `showY2`: `True` → `False`

### TiFlash Resource Control

| Panel | status | tags |
|---|---|---|
| TiFlash Resource Group | `changed` | `rate_interval`, `hidden_right_axis` |
| Request Unit | `changed` | `rate_interval` |

#### TiFlash Resource Group

- **status**: `changed`
- **tags**: `rate_interval`, `hidden_right_axis`
- **field diffs**:
  - `targets`: `'9 series'` → `'9 series'`
  - `showY2`: `True` → `False`
- **target notes**:
  - t2: `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="total_c...` → `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="total_c...` (legend `total_consumption-{{instance}}-{{resource_group}}`→`total_consumption-{{instance}}-{{resource_group}}`, hide True→True)
  - t5: `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="request...` → `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="request...` (legend `request_gac_count-{{instance}}-{{resource_group}}`→`request_gac_count-{{instance}}-{{resource_group}}`, hide True→True)
  - t6: `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="gac_req...` → `sum(rate(tiflash_resource_group_counter{instance=~"$instance",instance=~"$tiflash_role",k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",type="gac_req...` (legend `gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}`→`gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}`, hide True→True)

#### Request Unit

- **status**: `changed`
- **tags**: `rate_interval`
- **field diffs**:
  - `targets`: `'5 series'` → `'5 series'`
- **target notes**:
  - t0: `sum(rate(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[1m]))by($additional_groupby,keyspace_id)` → `sum(rate(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[$__rate_interval]))by($additional_groupby,keyspace_id)` (legend `replica-sync-rate-{{keyspace_id}}`→`replica-sync-rate-{{keyspace_id}}`, hide False→False)
  - t2: `sum(rate(tiflash_compute_request_unit{instance=~"$tiflash_role"}[1m]))by($additional_groupby,cluster_id)` → `sum(rate(tiflash_compute_request_unit{instance=~"$tiflash_role"}[$__rate_interval]))by($additional_groupby,cluster_id)` (legend `query-rate-{{cluster_id}} {{$additional_groupby}}`→`query-rate-{{cluster_id}} {{$additional_groupby}}`, hide False→False)
  - t4: `sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[1m]))by($additional_groupby,keyspace,resource_group,type)/(64*1024)` → `sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[$__rate_interval]))by($additional_groupby,keyspace,resource_group,type)/(64*1024)` (legend `storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}`→`storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}`, hide False→False)

### Status Server

| Panel | status | tags |
|---|---|---|
| Status API Request Duration | `changed` | `duration_quantiles`, `style_default`, `hidden_right_axis`, `layout_repack` |
| Status API Request (op/s) | `changed` | `hidden_right_axis`, `layout_repack` |

#### Status API Request Duration

- **status**: `changed`
- **tags**: `duration_quantiles`, `style_default`, `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `targets`: `'3 series'` → `'6 series'`
  - `nullPointMode`: `'null'` → `'null as zero'`
  - `showY2`: `True` → `False`
  - `legend_max`: `False` → `True`
  - `gridPos.h`: `7` → `8`
- **target notes**:
  - t0: `histogram_quantile(0.999,sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role...` → `histogram_quantile(1.00,sum(round(1000000000*rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance...` (legend `999-{{path}} {{$additional_groupby}}`→`max-{{path}} {{$additional_groupby}}`, hide True→True)
  - t1: `histogram_quantile(0.99,sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role"...` → `histogram_quantile(0.9999,sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_rol...` (legend `99-{{path}} {{$additional_groupby}}`→`9999-{{path}} {{$additional_groupby}}`, hide False→False)
  - t2: `sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role",k8s_cluster="$k8s_clust...` → `histogram_quantile(0.999,sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{instance=~"$proxy_instance",instance=~"$tiflash_role...` (legend `avg-{{path}} {{$additional_groupby}}`→`999-{{path}} {{$additional_groupby}}`, hide True→True)
  - +3 extra target(s) in new

#### Status API Request (op/s)

- **status**: `changed`
- **tags**: `hidden_right_axis`, `layout_repack`
- **field diffs**:
  - `showY2`: `True` → `False`
  - `gridPos.h`: `7` → `8`

### Vector Search

| Panel | status | tags |
|---|---|---|
| In-Memory Vector Index Instances | `unchanged` | — |
| Vector Index Estimated Memory Usage | `unchanged` | — |
| 99.9% Vector Search Duration (Per Request) | `unchanged` | — |
| 99.9% Vector Index Build Duration (Per DMFile Column) | `unchanged` | — |

_本 row 全部 panel 语义对齐（或仅被忽略的隐藏右轴字段）。_

## 6. 附录：含 `other` 标签或未充分归类的 panel

共 4 个 panel 带有 `other` / `yaxis_visible`，建议人工确认：

| Row | Panel | tags | field keys |
|---|---|---|---|
| Server | Memory | `other` | targets |
| Threads | Threads IO | `rate_interval`, `style_default`, `yaxis_visible`, `hidden_right_axis`, `layout_repack` | targets, nullPointMode, formatY1, showY2, gridPos.h |
| Task Scheduler | Task Waiting Duration | `rate_interval`, `duration_quantiles`, `yaxis_visible`, `hidden_right_axis`, `style_default` | targets, labelY1, showY2, minY1, legend_max |
| Rate Limiter | I/O Limiter Pending Duration | `rate_interval`, `duration_quantiles`, `style_default`, `hidden_right_axis`, `yaxis_visible` | targets, nullPointMode, formatY2, showY2, minY1 |

---

_Generated by `scripts/gen_migration_diff_md.py`. changed=206, added=2, removed=1, appendix=4._
