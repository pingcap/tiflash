# TiFlash Disaggregated Read Path: Node-Level S3 Backpressure and FileCache Same-Key Deduplication

Purpose: define a production-oriented design with deliberately constrained implementation scope to solve uncontrolled S3 traffic on compute nodes in disaggregated read workloads caused by cold reads and amplified cache misses. The target risk scenario is: a large portion of one table is cold on the current node and therefore absent from local `FileCache`; the system receives concurrent queries in a short time window; multiple queries scan roughly the same set of files on TiFlash at about the same time; S3 access latency keeps rising; node network becomes congested; queries queue for a long time; and the situation eventually turns into query failures or obvious timeouts.

Date: 2026-03-24

## Summary

This proposal recommends rolling out in two incremental phases:

1. Introduce a node-level `S3ReadLimiter` to cap total remote-read bandwidth on one node.
2. In `FileCache::get(...)`, add one bounded wait only when the same physical key already has an in-flight download, so followers prefer to reuse the existing download result instead of immediately falling back to `S3RandomAccessFile` direct reads.

The first version deliberately does **not** do the following:

- no query-level async read scheduler
- no new global miss coordination table
- no sleep-retry on `RejectTooManyDownloading`
- no complicated logic-file-type-specific branching inside `FileCache`

The reason is straightforward: the only stable, reusable, and easily rollbackable seams in the current code are `S3RandomAccessFile`, `FileCache`, `ClientFactory/TiFlashS3Client`, and `IORateLimitConfig` / `IORateLimiter`. Once we put a hard node-level traffic guardrail in place and deduplicate same-key miss fan-out, we are already able to turn “an occasional network blow-up that makes queries fail” into “a bounded, observable, tunable problem”.

## Context

### 1. Current direct S3 reads issue `GetObject` immediately

The current S3 open path is:

```text
FileProvider::newRandomAccessFile
  -> S3RandomAccessFile::create
     -> FileCache::getRandomAccessFile
        -> cache hit: return local file
        -> cache miss: return nullptr
     -> fallback: construct S3RandomAccessFile
        -> initialize() immediately in constructor
           -> client->GetObject(req)
```

Verified code locations:

- `FileProvider::newRandomAccessFile` goes directly to `S3RandomAccessFile::create(...)` on the S3 path and does not use the incoming `read_limiter`:
  `dbms/src/IO/FileProvider/FileProvider.cpp:35`
- `S3RandomAccessFile::create(...)` first tries `FileCache::getRandomAccessFile(...)`, then immediately constructs a remote reader on miss:
  `dbms/src/Storages/S3/S3RandomAccessFile.cpp:345`
- `S3RandomAccessFile` calls `initialize("init file")` in its constructor:
  `dbms/src/Storages/S3/S3RandomAccessFile.cpp:65`
- `initialize(...)` directly calls `client_ptr->GetObject(req)`:
  `dbms/src/Storages/S3/S3RandomAccessFile.cpp:278`

This means:

- once `FileCache` misses, the caller immediately consumes a new S3 stream
- there is currently no node-level hard cap on total bandwidth or total remote-read concurrency

### 2. Current `FileCache::get(...)` only inserts a background download and returns immediately on miss

`FileCache::get(...)` currently has three key branches:

- hit an existing `Complete` segment and return it directly:
  `dbms/src/Storages/S3/FileCache.cpp:342`
- hit an existing segment that is not ready yet, record a miss, return `nullptr` immediately:
  `dbms/src/Storages/S3/FileCache.cpp:351`
- key does not exist and caching is allowed: insert an `Empty` segment, schedule background download, return `nullptr` immediately:
  `dbms/src/Storages/S3/FileCache.cpp:366`

Current behavior can be summarized as:

```text
The same key is already being downloaded
  -> get() still returns nullptr
  -> caller keeps direct-falling back to S3

First miss of one key
  -> get() only inserts Empty + submits bg download
  -> caller keeps direct-falling back to S3
```

That is, today `FileCache` does not help query hot paths reuse an already running download.

### 3. A wait primitive already exists, but only for the foreground-download path

`FileSegment::waitForNotEmpty()` already exists, but it is a 30-second-granularity loop and is used only by `getOrWait(...)`:

- `dbms/src/Storages/S3/FileCache.cpp:76`
- `dbms/src/Storages/S3/FileCache.cpp:411`

`getOrWait(...)` means:

- if key already exists: wait for the download result
- if key does not exist: perform a foreground download directly

This API is suitable for vector index / inverted index / full-text index paths that must land on disk and then `mmap`, but not suitable for ordinary query read paths, because it turns the very first miss on the current thread into a synchronous download.

### 4. Existing `IORateLimiter` cannot directly limit remote S3 reads

Current read-side `IORateLimiter` implementation depends on `/proc/<pid>/io` and `/proc/<pid>/task/<tid>/io` to sample real local-disk bytes:

- `dbms/src/IO/BaseFile/RateLimiter.cpp:523`
- `dbms/src/IO/BaseFile/RateLimiter.cpp:601`
- `dbms/src/IO/BaseFile/RateLimiter.cpp:644`

This works for local disks, not remote S3 traffic, for two reasons:

1. S3 bytes never show up in `/proc/.../io` disk statistics.
2. For S3, the application already knows how many bytes it plans to read and how many bytes it actually read, so `/proc` sampling is unnecessary.

Therefore remote reads need a dedicated limiter implementation instead of reusing the existing local-disk `ReadLimiter`.

### 5. `S3RandomAccessFile` does not have access to `Context`

Today `S3RandomAccessFile` only holds:

- `TiFlashS3Client`
- `remote_fname`
- `ScanContext`

Code location:

- `dbms/src/Storages/S3/S3RandomAccessFile.h:43`

It does not have a `Context` or `IORateLimiter` reference. Therefore, if we want a node-level remote limiter, we must first solve how the runtime object becomes reachable from the S3 read path.

### 6. Under MetaV2, many logical small files are mapped to the physical `.merged` key

This is the key fact that shapes the phase-2 design.

Under MetaV2 / DMFile V3:

- min-max index is written into the merged file:
  `dbms/src/Storages/DeltaMerge/File/DMFileWriter.cpp:312`
- mark is written into the merged file:
  `dbms/src/Storages/DeltaMerge/File/DMFileWriter.cpp:345`
- some small data files are also merged into the merged file:
  `dbms/src/Storages/DeltaMerge/File/DMFileMetaV2.cpp:321`

Correspondingly, `getReadFileSize(...)` returns the size of the entire merged object, not the logical sub-file size:

- `dbms/src/Storages/DeltaMerge/File/DMFileMetaV2.cpp:395`

So the real access chain becomes:

```text
Logically read mark / index / small data
  -> resolve to merged_sub_file_infos
  -> actually open N.merged
  -> FileCache only sees the physical key: N.merged
```

The direct conclusion is:

- `FileCache::get(...)` cannot reliably tell, from the physical key alone, whether this is a small mark/index read or a large merged-data read
- therefore the first version should not introduce complicated wait policies at the `FileCache` layer based on logical file categories

## Goals

- Prevent one TiFlash compute node from saturating its NIC because of cold S3 reads and thereby causing query failures.
- Put direct S3 reads and FileCache downloads under the same node-level backpressure instead of allowing the two paths to amplify independently.
- Reduce miss fan-out for the same physical key without changing the current synchronous read model.
- Keep the cache-hit fast path almost unchanged, especially with no new heap allocations or major CPU overhead when the limiter is disabled.
- Make every new behavior switchable, observable, and rollbackable online.

## Non-Goals

- No query-level async read scheduler.
- No cross-node coordination or global download registry.
- No sleep-retry for `RejectTooManyDownloading` in the first version.
- Do not turn the first miss into a foreground download in the first version; only deduplicate followers.
- Do not separately optimize metadata-burst during read-task build; the node-level limiter in phase 1 automatically covers these S3 reads.
- Do not add a separate timeout/cancel semantic for limiter waits in the first version.

## Design

### Phase 1: node-level S3 remote-read limiter

#### Design decision

Add a dedicated `S3ReadLimiter` that constrains:

- total remote-read bytes per second on one node

It covers two paths:

- `S3RandomAccessFile` direct reads
- `FileCache::downloadImpl(...)` foreground/background downloads

#### Why the first version keeps only byte-rate limiting

We experimented with modeling the `GetObject` body lifetime as a `StreamToken`, and with using `s3_max_get_object_streams` to cap concurrent active streams. However, under real workloads and replay validation, this turned out to be an unsafe upper bound for the number of `S3RandomAccessFile` objects:

- one `S3RandomAccessFile` may keep a remote body stream open across pipeline / scheduler gaps while not continuously transferring bytes
- for semantic consistency, the token must cover the entire body lifetime; once the limit is set too tightly, readers that hold a stream but are temporarily idle are still counted under the hard cap
- the result is that active streams stay pinned near the cap while real S3 throughput has already dropped, and subsequent readers remain blocked, which manifests as permanently stuck queries or no forward progress

Therefore the first version keeps `s3_max_read_bytes_per_sec` as the deterministic node-level guardrail and removes `s3_max_get_object_streams` from both config and metrics. If we want to reintroduce a stream-dimension cap in the future, we first need a model that is closer to real network occupancy than a reader-lifetime token.

#### Configuration surface

Continue to use `[storage.io_rate_limit]` and add one new field:

- `s3_max_read_bytes_per_sec`
  - node-level upper bound on total remote-read bandwidth
  - uniformly covers `S3RandomAccessFile` read/forward seek and `FileCache` download
  - `0` means disabled

Suggested TOML form:

```toml
[storage.io_rate_limit]
# Existing disk IO limiter configs...
# max_bytes_per_sec = 0

# Limit total S3 remote-read bandwidth on one compute node.
# 0 means disabled.
# s3_max_read_bytes_per_sec = 0
```

#### Runtime ownership and integration points

Recommended runtime object relationship:

```text
storage.io_rate_limit
  -> IORateLimitConfig
     -> IORateLimiter handles parse / reload
        -> owns a dedicated S3ReadLimiter
           -> Server publishes it to ClientFactory during initialization
              -> TiFlashS3Client holds shared_ptr<S3ReadLimiter>
                 -> used directly by S3RandomAccessFile / FileCache::downloadImpl
```

Key constraints:

- `S3ReadLimiter` is not a subclass of the current `ReadLimiter`
  - it does not depend on `/proc/.../io`
  - it throttles directly from application-known byte counts
- `IORateLimiter` still owns config reload entry
  - but the remote limiter does not participate in current disk IO auto tune
- `ClientFactory` is responsible for publishing the current limiter to all `TiFlashS3Client` instances
  - including the client created immediately at startup
  - and clients lazily initialized later by compute nodes through write nodes

This avoids introducing an unnatural `Context` dependency into `S3RandomAccessFile`.

#### Behavioral semantics

##### 1. Semantics of byte budget

`s3_max_read_bytes_per_sec` accounts and limits real remote traffic:

- bytes read by `S3RandomAccessFile::readImpl(...)`
- bytes discarded by forward skip in `S3RandomAccessFile::seekImpl(...)`
- bytes downloaded to local storage by `FileCache::downloadImpl(...)`

It is a throttling mechanism, not fail-fast:

- wait when the budget is exhausted
- continue once budget becomes available again

#### Key implementation details

##### 1. `S3RandomAccessFile` switches to chunked mode only when the limiter is enabled

Current implementation:

- `readImpl(...)` performs one `istream.read(buf, size)`:
  `dbms/src/Storages/S3/S3RandomAccessFile.cpp:121`
- `seekImpl(...)` performs one `istream.ignore(delta)` on forward skip:
  `dbms/src/Storages/S3/S3RandomAccessFile.cpp:195`

If we simply add one `request(size)` / `request(delta)` before these calls, there are three problems:

1. one read can accumulate an overly large burst
2. retry / EOF / stream error can make reserved budget diverge from actual bytes
3. forward seek can burst a large amount of traffic after obtaining one large budget chunk

Therefore the first version should:

- keep the current one-shot `read/ignore` fast path when the limiter is disabled
- switch to fixed-size chunk loops when the limiter is enabled
  - for example 64 KiB or 128 KiB
  - wait for budget before each chunk advances the stream
  - no new heap allocation

This requires:

- `readImpl(...)` to become chunked `istream.read(...)` when the limiter is enabled
- `seekImpl(...)` forward skip to become chunked `istream.ignore(...)` when the limiter is enabled
- backward seek to keep the current semantics: re-`GetObject` plus a new body stream

##### 2. `FileCache::downloadImpl(...)` reuses the same limiter without introducing a new abstraction layer

Current download flow:

```text
downloadImpl
  -> client->GetObject(req)
  -> downloadToLocal(result.GetBody(), ...)
     -> ReadBufferFromIStream + copyData(...)
```

Code locations:

- `dbms/src/Storages/S3/FileCache.cpp:1013`
- `dbms/src/Storages/S3/FileCache.cpp:989`

The first version does not need a new IO framework here. Keep it simple:

- if limiter is disabled, continue to use the current download implementation
- if limiter is enabled, change `downloadToLocal(...)` into a fixed-buffer local loop
  - read a chunk
  - write to local file
  - no extra heap allocations throughout the path

This path is not the cache-hit hot path of a query, so one additional tiny branch relative to current `copyData(...)` is acceptable, but it should not introduce a new object graph.

#### Compatibility and invariants

- cache-hit read path stays unchanged
- when the limiter is disabled, behavior must remain identical to today
- existing `S3RandomAccessFile::initialize(...)` retry logic must be preserved
- remote limiter does not depend on query context
- remote limiter does not participate in current disk IO auto tune

#### Observability

At minimum, add these node-level metrics:

- count and duration of waiting for byte budget
- charged bytes of direct reads
- charged bytes of FileCache downloads

Keep and continue to use existing metrics:

- `tiflash_storage_s3_request_seconds`
- `ProfileEvents::S3GetObject`
- `ProfileEvents::S3ReadBytes`
- existing disaggregated S3 statistics in `ScanContext`
- `CurrentMetrics::S3RandomAccessFile`
  - this can continue to serve as an approximate guardrail for direct-read pressure

### Phase 2: same-key deduplication in `FileCache::get(...)`

#### Design decision

The first version does exactly one thing:

- when `FileCache::get(...)` finds that the same physical key already has an `Empty` segment, meaning someone is downloading it, allow the current thread to perform one very short bounded wait and try to reuse that result

The first version intentionally does not do two things:

- no wait strategy by logical file type
- no sleep-retry on `RejectTooManyDownloading`

This is an intentional narrowing of scope:

- `FileCache` can reliably see only physical keys
- under MetaV2, mark/index/small data often map to `.merged`
- queue-congestion retry is not very helpful for wide-table scenarios with many unique keys missing together
- phase 1 already solves the root cause of “the node network gets blown up”, so phase 2 can focus on the most reliable same-key deduplication only

#### Configuration surface

Add one dynamic setting near the existing `dt_filecache_*` settings:

- `dt_filecache_wait_on_downloading_ms`
  - `0` means disabled
  - when non-zero, `FileCache::get(...)` waits up to this duration when it hits an existing `Empty` segment

The recommended code default for the first version is `0`; when enabling in canary, start from a very small value such as `1~2ms`.

#### Behavioral semantics

Current behavior:

```text
get(key)
  -> found Empty segment
  -> return nullptr
  -> caller directly falls back to S3
```

Behavior after the change:

```text
get(key)
  -> found Empty segment
  -> release FileCache::mtx
  -> waitForNotEmptyFor(dt_filecache_wait_on_downloading_ms)
     -> Complete: return cached segment
     -> Failed / timeout: return nullptr
```

Note two points:

1. wait only when the same key already has an in-flight download
2. for the first miss where the current thread just inserted the `Empty` segment, keep the current behavior in the first version and return `nullptr` immediately

In other words, the first version deduplicates followers, not leaders.

#### Local primitive to add

Add on `FileSegment`:

- `waitForNotEmptyFor(timeout_ms)`

Code locations:

- `dbms/src/Storages/S3/FileCache.h:90`
- `dbms/src/Storages/S3/FileCache.cpp:76`

Requirements:

- return the state observed at timeout end
- hold no `FileCache::mtx` during the entire wait
- allow existing `waitForNotEmpty()` to share internal logic with it and avoid duplication

#### Why the first version does not add sleep-retry on `RejectTooManyDownloading`

We do not adopt this because:

1. it mainly helps when the queue is only briefly jittering
2. it is not very helpful for the main problem of wide-table cold reads where many unique keys miss together
3. it expands `FileCache::get(...)` from “same-key reuse” to “congestion guess + retry”, increasing complexity and tuning surface

Therefore the first version keeps only the most robust layer: when there is already an owner downloading one key, let followers wait briefly.

#### Compatibility and invariants

- `dt_filecache_wait_on_downloading_ms = 0` must preserve current behavior
- no `FileCache::mtx` is held while waiting
- only `Complete` is treated as hit; both `Failed` and timeout are treated as miss
- existing `getOrWait(...)`, vector-index / inverted-index / full-text-index foreground download paths are out of scope for this change

#### Observability

At least add the following counters:

- `wait_on_downloading`
- `wait_on_downloading_hit`
- `wait_on_downloading_timeout`
- `wait_on_downloading_failed`

To support finer-grained benefit analysis and tuning, also add a low-cardinality metric set:

- `wait_on_downloading_result{result, file_type}`
- `wait_on_downloading_bytes{result, file_type}`
- `wait_on_downloading_wait_seconds{result, file_type}`
- `bg_download_stage_seconds{stage, file_type}`
- `bg_downloading_count`

Suggested fixed labels:

- `result`: `hit` / `timeout` / `failed`
- `file_type`: `merged` / `coldata` / `other`
- `stage`: `queue_wait` / `download`

Among them:

- `wait_on_downloading_result{...}` is used for count-based hit ratio
- `wait_on_downloading_bytes{...}` is used for bytes-weighted hit ratio, to avoid overestimating benefit from many tiny merged hits
- `wait_on_downloading_wait_seconds{...}` is used to measure actual follower waiting cost
- `bg_download_stage_seconds{stage="queue_wait"}` is the time from `bgDownload(...)` submission until `bgDownloadExecutor(...)` actually starts running
- `bg_download_stage_seconds{stage="download"}` is the time from executor start until download completes or fails
- `bg_downloading_count` is the number of currently active background downloads

This metric set must strictly avoid high cardinality:

- no labels by key, table name, or DMFile id
- keep only coarse-grained categories such as `Merged vs ColData vs Other`, enough to support tuning

Current code already has natural hook points and state for these metrics:

- the queue-full decision that causes `RejectTooManyDownloading`:
  `dbms/src/Storages/S3/FileCache.cpp:887`
- current background download count `bg_downloading_count`:
  `dbms/src/Storages/S3/FileCache.h:471`
  `dbms/src/Storages/S3/FileCache.cpp:1106`
- stopwatch around one download:
  `dbms/src/Storages/S3/FileCache.cpp:1010`

This metric set is sufficient to answer four questions:

- whether bounded wait really absorbs direct fallback
- whether the benefit mainly comes from `Merged` or from `ColData`
- when count hit ratio looks promising, whether bytes-weighted hit ratio still holds
- whether timeout mainly comes from queueing too long or from the object download itself being too slow

## Rejected Directions

### 1. Distinguish wait policy at the `FileCache` layer by `Meta / Index / Mark / Merged / ColData`

Not adopted. Under MetaV2, this mapping does not reliably hold at the `FileCache` layer:

- mark/index/small data often map to the physical `.merged`
- `FileCache` only sees the physical key

If we force this into the first version, the document would become more precise than the implementation, but the implementation still would not have accurate-enough information, and the final policy and metrics would both become misleading.

### 2. Sleep-retry on `RejectTooManyDownloading`

Not adopted because:

- it cannot replace a node-level traffic cap
- it is not very useful for cold reads with many unique keys
- it adds hot-path branches and new tuning knobs

If phase 2 later shows that same-key follower dedup already works well but queue jitter is still obvious, we can evaluate a third incremental change separately.

## Incremental Plan

1. Add S3 remote-read config into `IORateLimitConfig`, disabled by default.
2. Add `S3ReadLimiter`; let `IORateLimiter` handle reload and `ClientFactory` publish it to `TiFlashS3Client`.
3. Wire it into `S3RandomAccessFile` and `FileCache::downloadImpl(...)`, together with phase-1 metrics and unit tests.
4. Canary phase 1 on compute nodes first, with real machine-specific limits.
5. Add bounded wait to `FileSegment` / `FileCache::get(...)`, disabled by default.
6. After phase 1 metrics become stable, enable a very small `dt_filecache_wait_on_downloading_ms` in canary.

### Recommended initial parameters

For the first canary, the first goal is to make sure a node cannot saturate its network because of cold-read fan-out, rather than to maximize throughput from day one.

- `s3_max_read_bytes_per_sec`
  - start from `30%~50%` of the node's sustainable outbound bandwidth budget rather than the theoretical NIC peak
  - for `10GbE` nodes, start from `300~500 MiB/s`, i.e. `314572800~524288000`
  - for `25GbE` nodes, start from `800~1200 MiB/s`, i.e. `838860800~1258291200`
  - if the same node also serves obvious MPP exchange, page-storage, S3 background tasks, or other outbound traffic, prefer the lower end of the range
  - when increasing, change by only `10%~20%` each step

- `dt_filecache_wait_on_downloading_ms`
  - keep `0` in phase 1
  - for the first phase-2 canary, start from `1`
  - increase to `2` only after `wait_on_downloading_hit / wait_on_downloading` is clearly non-zero and timeout ratio is still low
  - do not exceed `5` in the first version

At phase 1, there is only one knob to stabilize: `s3_max_read_bytes_per_sec`. It directly determines how much sustained pressure a compute node can apply to S3 during a cold-read burst.

### Rollout sequence

Recommended four-step rollout, changing only one main variable each time:

1. Single-node canary, phase 1 only
   - suggested starting point:
     - `s3_max_read_bytes_per_sec =` `30%~50%` of node budget
     - `dt_filecache_wait_on_downloading_ms = 0`
   - must cover at least one real cold-read peak or one reproducible wide-table cold-cache pressure test

2. Small-batch canary, limited to one AZ, one tenant group, or no more than `5%` of compute nodes
   - keep config identical to the canary
   - if tuning is required, change only one parameter each time and observe at least one complete business peak window before the next change

3. Expand to `20%~30%` of compute nodes
   - only expand when phase 1 has already shown clear error reduction, node outbound traffic no longer sticks to the line, and S3 latency no longer keeps worsening

4. Enable phase 2 on the same canary
   - keep phase-1 config unchanged and add only `dt_filecache_wait_on_downloading_ms = 1`
   - if phase-2 hit ratio is not obvious or timeout ratio is high, roll back to `0` without blocking phase-1 full rollout

### Rollout criteria and rollback thresholds

The following thresholds are recommendations for the first rollout, not contractual guarantees; their goal is to turn “whether to continue rollout” into an actionable decision.

Before expanding phase 1, all of the following are recommended:

- `CurrentMetrics::S3RandomAccessFile` no longer grows almost linearly with query concurrency
  - “continuously above `900` for several minutes” is recommended as an alert line
  - this is a trend/guardrail signal, not an exact token count
- P95/P99 of `tiflash_storage_s3_request_seconds{type="get_object"}` and `tiflash_storage_s3_request_seconds{type="read_stream"}` no longer keep worsening during bursts
  - some slowdown relative to baseline is acceptable
  - but if P99 degrades by more than `30%` at similar throughput and query errors do not clearly improve, rollout should not continue
- 1-minute peak node outbound traffic should stay below `85%` of the node budget
  - if it still sticks near the link limit or shows obvious saw-tooth oscillation, the byte budget is still too high
- query errors caused by node network saturation should drop to `0` or near `0`
  - if the error type does not clearly improve, the limiter likely did not hit the real bottleneck
- `tiflash_storage_remote_cache{type="dtfile_download_failed"}` should not rise significantly after enabling

Phase 1 should be rolled back immediately or at least stop further rollout under any of the following conditions:

- query error rate rises obviously, or new timeout/cancel errors increase significantly
- `CurrentMetrics::S3RandomAccessFile` still frequently approaches `1000`, indicating that direct-read pressure is still too high or there are unprotected paths
- P99 of `tiflash_storage_s3_request_seconds{type="get_object"}` or `{type="read_stream"}` keeps rising to around `2x` baseline and does not recover
- node outbound traffic still sticks to the line or turns into a new severe oscillation

Before expanding phase 2, all of the following are recommended:

- `wait_on_downloading_hit / wait_on_downloading >= 10%`
  - if it stays below `5%` for a long time, the same-key follower dedup benefit is weak and keeping `0` is likely better
- bytes-weighted hit ratio should be stably non-zero; start with `5%` as an empirical threshold
  - can be computed as `sum(wait_on_downloading_bytes{result="hit"}) / sum(wait_on_downloading_bytes)`
  - if count hit ratio is high but bytes-weighted hit ratio stays below `5%`, the benefit mainly comes from small metadata / merged files and should not be expected to significantly reduce the main data traffic
- `wait_on_downloading_timeout / wait_on_downloading <= 20%`
  - if timeout ratio is high, the wait window is larger than the real reusable window
- `wait_on_downloading_failed` remains at a very low level
- after enabling `1ms` wait, query P99 does not worsen by more than `10%`
- `tiflash_storage_remote_cache{type="dtfile_too_many_download"}` does not continue to rise, or direct-fallback spikes show signs of dropping
- split results between `Merged` and `ColData` match workload expectations
  - if `merged` hit is high but `coldata` is low, phase 2 mainly absorbs MetaV2 small-file fallback; this still has value, but network relief is usually smaller than the count ratio suggests
  - if the goal is to further reduce main data traffic, focus on `coldata` hit/timeout/bytes rather than only total hit ratio
- `bg_download_stage_seconds{stage="queue_wait"}` should not stay significantly above `stage="download"}` for a long time
  - if queue wait dominates timeout, first inspect download queue size, concurrent-download config, and `RejectTooManyDownloading` pressure instead of increasing `dt_filecache_wait_on_downloading_ms`
  - if actual download dominates timeout and timeout is concentrated on `coldata`, the benefit ceiling of phase 2 is inherently limited and the wait window should not keep growing in order to force more benefit

Existing code locations for the current observability basis:

- `CurrentMetrics::S3RandomAccessFile`: `dbms/src/Common/CurrentMetrics.cpp:88`
- `tiflash_storage_s3_request_seconds`: `dbms/src/Common/TiFlashMetrics.h:779`
- `tiflash_storage_remote_cache`: `dbms/src/Common/TiFlashMetrics.h:875`

## Validation Strategy

### Unit tests

- Phase 1
  - `s3_max_read_bytes_per_sec` limits both direct reads and downloads
  - behavior remains unchanged when the limiter is disabled
  - lazily created S3 clients on compute nodes still pick up the current limiter

- Phase 2
  - timeout / success / failed semantics of `waitForNotEmptyFor(...)`
  - `FileCache::get(...)` can directly return the cached file via bounded wait when the key already has an `Empty` segment
  - behavior remains unchanged when `dt_filecache_wait_on_downloading_ms = 0`
  - no `FileCache::mtx` is held during waiting
  - leader behavior on the first miss stays unchanged
  - observability classification for `Merged` / `ColData` / `Other` is correct
  - `wait_on_downloading_bytes{...}` matches actual waited file size accounting
  - `bg_download_stage_seconds{stage="queue_wait|download"}` is accounted correctly on both success and failure paths

### Integration tests

- cold cache, wide table, many columns read simultaneously
- multiple threads reading the same physical `.merged` key concurrently
- node-level limiter still caps peak traffic when direct read and FileCache download coexist
- under MetaV2, mark / index / data reads all map to the `.merged` key path

### Production success criteria

- after phase 1 rollout, node peak outbound traffic stays stably below the budget and no longer continuously sticks near the link limit
- query failures caused by node network saturation drop to `0` or near `0`
- P95 / P99 of `tiflash_storage_s3_request_seconds{type="get_object"}` and `{type="read_stream"}` no longer keep worsening during bursts
- if phase 2 is enabled, `wait_on_downloading_hit / wait_on_downloading` should at least reach a two-digit percentage and timeout ratio should remain low
- if phase 2 is enabled, bytes-weighted hit ratio should stay stably non-zero, and the `Merged vs ColData` split should explain where the benefit comes from rather than leaving only one total hit ratio
- if timeout mainly comes from queue wait, optimize download queueing first; if timeout mainly comes from actual download, accept the benefit ceiling of phase 2 on large `ColData` files instead of treating it as a replacement for phase 1

## Risks and Mitigations

- limit set too low and query latency rises
  - mitigation: disabled by default; size per node; roll out in phases

- direct read and FileCache download share the same limit and may squeeze each other
  - mitigation: share one budget in the first version and do not add more layering yet; decide whether to split only after observing `source`-dimension metrics

- bounded wait hit ratio is low and only adds tail latency
  - mitigation: default `0`; start from a tiny value such as `1~2ms`; trigger only when the same key already has an in-flight download

- new CPU overhead after enabling the limiter
  - mitigation: use chunked paths only when the limiter is enabled; keep current fast path when disabled

## Open Questions

- if we want to constrain “active remote body streams” again in the future, what model is closer to actual network occupancy than a reader-lifetime token?
- if phase 2 hit ratio is already respectable but direct-fallback spikes still happen after `RejectTooManyDownloading`, do we need a third phase with queue-full retry?
