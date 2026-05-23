# TiCI RssFile 内存流控设计

## 背景

TiCI reader 启用后会通过 Tantivy 大量使用 mmap 读取索引文件。Linux 会把这些文件映射页计入进程 RSS，并体现在 `/proc/self/status` 的 `RssFile` 中。由于这些页主要是 file-backed page cache，不应和匿名内存一样触发 TiFlash 的内存流控。

当前 TiFlash 有两类受影响的流控：

- TiFlash 计算层的 `MemoryTracker` 会读取进程 RSS，超过配置阈值时拒绝查询请求。
- `tiflash-proxy` 会通过 `memory-usage-limit`、`memory-usage-high-water` 和 `reject-messages-on-memory-ratio` 控制 Raft append message 和 snapshot 接收。

目标是在 TiCI reader 启用时，让这两处流控使用排除 `RssFile` 后的 RSS，同时保留原始 RSS 和 `RssFile` 用于观测与排障。

## 配置语义

新增 TiCI 相关配置项：

```toml
[tici]
exclude-rss-file-from-memory-control = true
```

有效开关定义为：

```text
effective_exclude_rss_file =
    tici_reader_enabled && tici.exclude-rss-file-from-memory-control
```

其中 `tici_reader_enabled` 沿用现有启动条件：

```text
tici.reader-node.addr 非空 || tici.reader-node.port > 0
```

因此：

- 未启用 TiCI reader 时，不改变现有内存流控行为。
- 启用 TiCI reader 时，默认从流控 RSS 中排除 `RssFile`。
- 如果需要排障或灰度回退，可以显式设置 `exclude-rss-file-from-memory-control = false`。

## TiFlash 侧内存采集

TiFlash C++ 侧维护 2 个进程内存采样值：

- `raw_rss`：原始进程 RSS。
- `rss_file`：`/proc/self/status` 中的 `RssFile`。

`MemoryTracker` 在执行内存流控检查时根据配置计算实际用于流控的 RSS：

```text
if effective_exclude_rss_file:
    memory_control_rss = max(raw_rss - rss_file, 0)
else:
    memory_control_rss = raw_rss
```

实现位置：

- 复用 `libs/libprocess_metrics` 已有的 `ProcessMetricsInfo.rss` 和 `ProcessMetricsInfo.rss_file`。
- `get_process_mem_usage()` 只返回原始 RSS 和 `RssFile` 等进程采样值，不接收流控配置。
- 在 `CollectProcInfoBackgroundTask::memCheckJob()` 中定期更新原始 RSS 和 `RssFile`，更新频率沿用当前 `MemTrackThread` 的 100 ms 周期。
- `MemoryTracker` 根据 `effective_exclude_rss_file` 选择使用 `raw_rss` 或 `max(raw_rss - rss_file, 0)`。

失败策略：

- 如果读取 `/proc/self/status` 失败，按现有逻辑回退到整体 RSS。
- 如果 `rss_file > raw_rss`，`memory_control_rss` 取 0，避免无符号下溢。

## Proxy 侧内存采集

`tiflash-proxy` 不修改 `tikv_util::sys` 的全局 memory usage，避免影响 proxy 内部其他依赖 `GLOBAL_MEMORY_USAGE` 的逻辑。

proxy 内部新增独立的 `MemoryControlRssState`，用于缓存 raw RSS、`RssFile`、`memory_control_rss` 和有效性标记：

```rust
struct MemoryControlInfo
{
    raw_rss: u64,
    rss_file: u64,
    memory_control_rss: u64,
    valid: bool,
}
```

实现位置：

- 在 `contrib/tiflash-proxy/proxy_components/proxy_server/src/run.rs` 中新增状态结构和刷新 helper。
- 在 `TiKvServer` 中保存 `Arc<MemoryControlRssState>`。
- 新增 `TiKvServer::init_memory_control_rss_refresher(effective_exclude_rss_file)`，用 `self.core.background_worker.spawn_interval_task(...)` 启动周期刷新任务。
- `run_impl` 在 `register_memory_usage_high_water(high_water)` 之后调用该初始化方法。

刷新规则：

- 刷新周期使用 1 s，与 TiKV 现有全局 memory usage 刷新频率一致。
- 启动周期任务前先同步刷新一次，避免 proxy 刚开始接收消息时缓存为空。
- 开关关闭时仍可刷新 raw RSS，并令 `memory_control_rss = raw_rss`，这样拒绝路径不需要按开关分支处理。
- Linux 上从 `/proc/self/status` 读取 `VmRSS` 和 `RssFile`；非 Linux 或读取失败时标记 invalid。

`TiFlashGrpcMessageFilter` 构造时接收 `Arc<MemoryControlRssState>` 和 `high_water`，在拒绝路径中只读取缓存值，不解析 `/proc`。

## Proxy 拒绝逻辑

修改范围限制在：

- `contrib/tiflash-proxy/proxy_components/proxy_server/src/run.rs` 中的 `should_reject_raft_message`
- `contrib/tiflash-proxy/proxy_components/proxy_server/src/run.rs` 中的 `should_reject_snapshot`

逻辑为：

```text
if reject_messages_on_memory_ratio <= 0:
    return false

if this is raft message and message is not MsgAppend:
    return false

info = memory_control_rss_state.load()
if info.valid:
    return info.memory_control_rss >= high_water

return memory_usage_reaches_high_water(...)
```

说明：

- `should_reject_snapshot` 没有 message type 判断，其余逻辑一致。
- `MemoryControlRssState` invalid 时，fallback 到原有 `memory_usage_reaches_high_water`，避免采集异常导致流控失效。
- `tikv_server_memory_usage` 等 proxy 全局指标保持原语义，不改成 adjusted RSS。

## 观测与日志

需要保留原始 RSS 和 adjusted RSS 的可解释性：

- TiFlash 侧继续暴露 `tiflash_process_rss_by_type_bytes{type="file"}`。
- 在启动日志中打印 `effective_exclude_rss_file` 的最终值。
- 在内存拒绝日志中区分 raw RSS、`RssFile` 和 `memory_control_rss`。
- proxy 在 high-water 判断相关 debug 日志中包含 `memory_control_rss`、raw RSS、`RssFile` 和 high-water。

## 测试与验证

本变更不新增单元测试。实现完成后按仓库约束运行：

```bash
time (make format && make check)
```

手工验证场景：

- TiCI 未启用：`memory_control_rss == raw_rss`，计算层和 proxy 行为保持原样。
- TiCI 启用且使用默认配置：`memory_control_rss == max(raw_rss - rss_file, 0)`。
- TiCI 启用但显式关闭开关：`memory_control_rss == raw_rss`。
- proxy 侧采集返回 invalid：fallback 到原有 `memory_usage_reaches_high_water`。
- 日志或指标可以解释 raw RSS、`RssFile` 和流控 RSS 的差异。

## 非目标

- 不改变 `tikv_util::sys::record_global_memory_usage()` 和 `memory_usage_reaches_high_water()` 的全局语义。
- 不尝试区分 TiCI mmap 文件和其他 file-backed RSS，当前策略是在 TiCI reader 启用时排除全部 `RssFile`。
- 不为监控或该流控调整新增单元测试。
