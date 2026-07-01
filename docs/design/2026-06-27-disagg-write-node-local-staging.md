# Proposal: Localized Reads for Remote DTFiles in Background Merge Tasks in Disaggregated write node

Date: 2026-06-27

Purpose: Reduce the risk of `Segment::prepareMerge`, `Segment::prepareMergeDelta`, and `Segment::prepareSplitPhysical` opening a large number of S3 streams directly for wide tables in disaggregated mode, and define a first-version local read design with controlled complexity.

## Summary

This proposal introduces an explicit switch on write/storage nodes:

```text
dt_enable_write_filecache = false
```

When this switch is enabled, remote DTFile localized reads are enabled only for the following background tasks:

1. `Segment::prepareMerge`
2. `Segment::prepareMergeDelta`
3. `Segment::prepareSplitPhysical`

The first version does not introduce `RemoteReadPolicy::Auto`, does not make automatic decisions based on thresholds, does not add a batch download API, and does not support non-MetaV2 files. The reason is that newly generated DMFiles in the disaggregated architecture use MetaV2, while the core problem to solve is that background tasks on wide tables construct readers by column and substream, amplifying a small number of physical `.merged` files into many direct S3 streams.

The first-version design decisions are:

- Use `dt_enable_write_filecache` to explicitly control whether write-side FileCache and background-task localized reads are enabled.
- Trigger the behavior only in `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical`.
- Let `DMFileBlockInputStream` or `DMFileReader` directly hold `std::vector<FileSegmentPtr> local_read_files` to pin local cached files.
- Collect the physical objects that will actually be read under MetaV2 precisely according to `read_columns`, and deduplicate them by S3 key.
- Reuse the existing single-file API `FileCache::downloadFileForLocalReadWithRetry(...)` to download files one by one, without adding a batch API.
- If localization fails, always fall back to the existing direct S3 read path, and record logs and metrics.

This design keeps the implementation surface small. `Segment` only decides which background tasks enable localization. `DMFileBlockInputStreamBuilder` / `DMFileReader` are responsible for object collection and pinning. `FileCache` continues to handle downloads, capacity, deduplication, failure cleanup, and local file lifetime.

## Context

### Current State

The current `prepareMergeDelta` path is roughly:

```text
Segment::prepareMergeDelta
  -> getInputStreamForDataExport
     -> getReadInfo
     -> getPlacedStream
        -> StableValueSpace::Snapshot::getInputStream
           -> DMFileBlockInputStreamBuilder::build
              -> DMFileReader
                 -> ColumnReadStream per column/substream
```

The `prepareMerge` path constructs internal read streams for the segments to merge:

```text
Segment::prepareMerge
  -> for each segment: getReadInfo + getPlacedStream
  -> ConcatBlockInputStream
  -> createNewStable
```

`prepareSplitPhysical` also constructs internal read streams, writing the original segment data into new stables according to the split point.

The relevant confirmed code boundaries are:

- `Segment::prepareMergeDelta` reads stable and delta data in the current segment through `getInputStreamForDataExport(...)`, then writes a new stable.
- `Segment::prepareMerge` constructs internal read streams for multiple segments and eventually writes the merged stable through `createNewStable(...)`.
- `Segment::prepareSplitPhysical` constructs data streams for both sides of a physical split and writes stables separately.
- `StableValueSpace::Snapshot::getInputStream(...)` creates a `DMFileBlockInputStream` for each DMFile in the stable.
- The `DMFileReader` constructor iterates over `read_columns` and creates a `ColumnReadStream` for every existing column and its substreams.
- When `ColumnReadStream` is constructed, it reads marks and creates a read buffer for column data. If the underlying path is S3, `FileProvider::newRandomAccessFile(...)` enters `S3RandomAccessFile::create(...)`.
- `S3RandomAccessFile::create(...)` first tries `FileCache::getRandomAccessFile(...)`. On miss, it constructs `S3RandomAccessFile` and opens an S3 `GetObject` stream during construction.
- `FileCache` is currently mainly initialized on disaggregated compute nodes when remote cache is enabled. Background merge/split happens on write/storage nodes, so these internal tasks usually cannot benefit from `FileCache`.

Under Format V3 / MetaV2, multiple logical column files are recorded in DMFile metadata and may be written into physical `.merged` files. Today, `ColumnReadStream::buildColDataReadBuffByMetaV2(...)` uses `merged_sub_file_infos` to find the merged file, offset, and size for a logical subfile, then creates a random read object, seeks to the offset, and reads that subfile's raw data into memory. This layout reduces the number of physical files. However, if every column substream independently creates a read buffer, background tasks on wide tables can still amplify the number of columns into many S3 streams or range reads.

### Problem Statement

In a wide-table scenario, for example a table with 500 columns, `prepareMerge`, `prepareMergeDelta`, or `prepareSplitPhysical` on a segment often needs to read the full schema rather than a small projection from a query. The current read stream construction eagerly initializes `ColumnReadStream` by column and substream, causing:

- A large number of S3 `GetObject` streams to be opened in a short time.
- Each stream may hold a connection, body, buffer, retry state, and rate-limiting path.
- S3-side latency or network jitter can affect many column readers at the same time.
- Background tasks and foreground queries share remote read resources, which can amplify system jitter.
- Even though `FileCache` already has capacity control and download deduplication, background tasks on write/storage nodes may not have it enabled.

Background merge/split tasks are internal batch jobs that are known to perform broad data reads and rewrite stables. They are more suitable for pulling the relevant physical objects to local storage in a controlled way before constructing column readers, and then completing subsequent column-level seek/read operations from local files, instead of letting each column reader independently connect to S3 directly.

### Constraints and Decision Drivers

- DeltaMerge visible semantics must not change. Background merge/split only rewrites physical layout, and must not change row visibility, MVCC filtering, delete marks, or handle semantics.
- Existing remote object lifetime management must not be bypassed. In disaggregated mode, the remote location of DMFiles, GC enablement/disablement, and PageStorage external page references must remain managed by the existing mechanisms.
- Local disk must not be treated as unlimited capacity. Any localized download must go through `FileCache` capacity control, failure cleanup, and eviction/pin semantics.
- The first version must be easy to roll back. After disabling `dt_enable_write_filecache`, the behavior should return to the existing direct S3 read path.
- The first version only handles MetaV2. Newly generated DMFiles in the disaggregated architecture use MetaV2, so there is no need to introduce extra branches for old formats.
- The first version always falls back to direct reads, preventing local cache capacity shortage or download failures from reducing the success rate of background tasks.

## Terminology Baseline

| Term | Meaning |
| --- | --- |
| write FileCache | FileCache explicitly enabled on write/storage nodes by `dt_enable_write_filecache` |
| local staging | Downloading remote physical objects locally before a background task starts reading, and pinning them for the reader lifetime |
| direct S3 read | Reading directly from a remote stream after `S3RandomAccessFile` misses `FileCache` |
| physical object | An actual object on S3, for example `N.merged` |
| logical subfile | A column data, mark, or index file described by DMFile MetaV2 metadata; it may reside inside the same `.merged` physical object |

## Goals

1. Reduce S3 stream fan-out caused by per-column reads in `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical` for wide tables.
2. Control the behavior explicitly through `dt_enable_write_filecache`, with the default kept disabled.
3. For MetaV2 DMFiles, precisely collect the physical objects to localize according to the actual read columns, and deduplicate them by key.
4. Reuse the existing `FileCache::downloadFileForLocalReadWithRetry(...)` without adding a batch download API.
5. Pin downloaded local files for the lifetime of the reader/input stream, preventing eviction during reads.
6. Always fall back to direct S3 reads if localization fails, so the first version does not reduce the success rate of background tasks.

## Non-Goals

- Do not introduce `RemoteReadPolicy::Auto` or any heuristic automatic policy.
- Do not automatically decide enablement based on thresholds such as column count, bytes, or segment size. Enablement is decided only by `dt_enable_write_filecache` and the call site.
- Do not add a batch download API to `FileCache`.
- Do not support non-MetaV2 DMFiles.
- Do not cover internal read paths other than `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical`.
- Do not implement fallback=`fail`.
- Do not rewrite `DMFileReader` column-level decoding, block organization logic, or the eager `ColumnReadStream` initialization model.
- Do not force local staging for normal query paths.

## Architecture Overview

The target structure is:

```text
Segment::prepareMerge / prepareMergeDelta / prepareSplitPhysical
  -> build internal stream with write-filecache enabled flag
     -> DMFileBlockInputStreamBuilder
        -> collect MetaV2 physical objects for read_columns
        -> FileCache::downloadFileForLocalReadWithRetry for each object
        -> store returned FileSegmentPtr in local_read_files
        -> DMFileReader / ColumnReadStream
           -> FileProvider::newRandomAccessFile
              -> S3RandomAccessFile::create
                 -> FileCache hit: PosixRandomAccessFile
                 -> miss/failure: S3RandomAccessFile direct read
```

Key points:

- Localization happens before `DMFileReader` eagerly creates all `ColumnReadStream` instances.
- Subsequent column readers do not need a special read interface. They still go through `FileProvider` / `S3RandomAccessFile::create`.
- `local_read_files` holds `FileSegmentPtr`, ensuring that local files are not evicted during the reader lifetime.
- Localization failures do not block read stream construction. They always fall back to the existing direct read path.

## Design

### 1. Explicit Switch `dt_enable_write_filecache`

Add a new setting:

```text
dt_enable_write_filecache = false
```

Semantics:

- `false`: the write/storage node does not actively use FileCache for these three background tasks, and all logic related to this design degenerates to the existing behavior.
- `true`: the write/storage node initializes an available FileCache, and attempts to localize MetaV2 physical objects before constructing DMFile readers in `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical`.

This switch controls both write-side FileCache enablement and localized reads for the three background task types. The first version does not introduce a separate `RemoteReadPolicy` or automatic policy, to avoid too many configuration and behavior combinations.

### 2. Cover Only Three Call Sites

The first version only passes the "enable write FileCache local staging" flag from the following paths:

- `Segment::prepareMerge`
- `Segment::prepareMergeDelta`
- `Segment::prepareSplitPhysical`

In implementation, these paths can set a bool on `DMFileBlockInputStreamBuilder` when constructing internal read streams, for example:

```cpp
builder.enableWriteFileCacheLocalRead(dm_context.global_context.getSettingsRef().dt_enable_write_filecache);
```

Alternatively, the flag can be passed through an explicit field in `DMContext`. In either approach, the trigger scope must be limited to these three call sites to avoid automatically changing the behavior of all `ReadTag::Internal` paths.

### 3. Handle Only MetaV2

Localized object collection only supports files where `dmfile->useMetaV2()` is true.

If `dt_enable_write_filecache=true` but the file is not MetaV2:

- Record a debug or trace log.
- Do not localize it.
- Continue through the existing direct read path.

There is no need to collect `.dat`, `.mrk`, or `.idx` files for non-MetaV2. Newly generated remote DMFiles in the disaggregated architecture use MetaV2, so old formats are not a first-version target.

### 4. Precisely Collect Physical Objects by Read Columns

Before `DMFileBlockInputStreamBuilder::buildNoLocalIndex(...)` creates `DMFileReader`, collect the physical objects to localize from `dmfile` and `read_columns`.

The object collection flow is:

```text
DMFile MetaV2 + read_columns
  -> enumerate each column's substreams by IDataType::enumerateStreams
  -> for each stream_name:
       logical data file = colDataFileName(stream_name)
       logical mark file = colMarkFileName(stream_name)
       optional logical index file = colIndexFileName(stream_name)
  -> find each logical file in merged_sub_file_infos
  -> convert merged file number to physical mergedPath(number)
  -> deduplicate physical S3 key
  -> download each physical key through FileCache
```

Collection rules:

- For every read column that actually exists, enumerate substreams in the same way the current `DMFileReader` constructs `ColumnReadStream`.
- For every substream, collect the corresponding data and mark logical files.
- If pack filter or index loading reads index files, also collect the corresponding logical index files.
- Map every logical file to a physical `.merged` file through `DMFileMetaV2::merged_sub_file_infos`.
- Deduplicate by physical S3 key to avoid downloading the same `.merged` file repeatedly.

If a logical file is not found in `merged_sub_file_infos`, the first version does not perform any extra file download. It records a debug log and allows the subsequent read path to read directly. This fallback is compatible with abnormal or transitional states and avoids expanding the first version to non-merged file handling.

### 5. Reuse the Single-File Download API

The first version does not add a batch download API. For the deduplicated physical keys, call the following API one by one:

```cpp
FileCache::downloadFileForLocalReadWithRetry(s3_file_name, file_size, retry_count)
```

Requirements:

- `retry_count` can use a fixed value such as 3, or reuse an existing configuration.
- Files already Complete in FileCache should be returned directly.
- Files that are currently being downloaded should reuse the existing wait logic.
- On download failure, catch the exception, record a warning and metric, and continue with direct-read fallback.
- Every successfully returned `FileSegmentPtr` must be saved for the lifetime of the reader/input stream.

Sequential downloads are not optimal for throughput, but the first-version implementation is simple, has controlled behavior, and can reuse the existing FileCache reservation, failure cleanup, retry, and local file opening logic.

### 6. Hold `local_read_files` Directly in the Reader/Input Stream

The first version does not add a `LocalReadGuard` abstraction. Add a member directly to `DMFileBlockInputStream` or `DMFileReader`:

```cpp
std::vector<FileSegmentPtr> local_read_files;
```

Responsibilities of this member:

- Hold the `FileSegmentPtr` returned by FileCache.
- Prevent FileCache from evicting the corresponding local files during the reader/input stream lifetime.
- Not participate in remote object lifetime management.
- After destruction, the corresponding files return to normal FileCache LRU management.

The recommended location is `DMFileReader`. The reason is that localized object collection happens before reader construction or at the reader construction entry point, while `ColumnReadStream` is managed by `DMFileReader`; the reader lifetime covers the column data read process.

### 7. Always Fall Back to Direct Reads

The first version does not add fallback configuration. The behavior on localization failure is fixed:

```text
log warning + metric
continue with existing direct S3 read path
```

The following cases should all fall back to direct reads:

- `dt_enable_write_filecache=false`
- FileCache is not initialized
- The DMFile is not MetaV2
- A logical file cannot be mapped to a physical `.merged` file
- FileCache capacity is insufficient
- Download fails
- Download succeeds but opening the local file fails

This ensures that the first version does not reduce background task success rate due to local cache capacity, download failures, or implementation gaps. If S3 protection is needed later, a fail-fast policy can be added.

### 8. Observability

Add or reuse the following metrics / scan context fields:

- Number of write FileCache local staging attempts.
- Number of physical objects collected for staging.
- Number of objects and bytes hit in FileCache during staging.
- Number of objects and bytes downloaded during staging.
- Number of staging failures, classified by reason.
- Number of direct-read fallbacks.
- Total staging time for `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical`.
- Changes in `S3RandomAccessFile` `GetObject` count and current-open count.

Logs should output summaries at task or DMFile granularity, avoiding per-column log spam:

```text
Write FileCache local staging begin:
  task=prepareMergeDelta dmfile=... columns=... physical_objects=...

Write FileCache local staging finish:
  dmfile=... hit=... downloaded=... failed=... fallback=... cost_ms=...
```

## Compatibility and Invariants

- When `dt_enable_write_filecache=false`, behavior remains unchanged.
- Non-disaggregated mode behavior remains unchanged.
- Non-MetaV2 files remain unchanged.
- Only `prepareMerge`, `prepareMergeDelta`, and `prepareSplitPhysical` attempt localization.
- Localization only changes the read source. It does not change DMFile metadata, PageStorage external page references, or remote GC semantics.
- `local_read_files` only pins local cached files. It does not extend the lifetime of remote objects. Remote object lifetime is still controlled by checkpoint locations, external pages, and DMFile GC.
- Localization failures always fall back to direct reads and should not introduce extra prepare-stage failures.
- FileCache eviction must not delete local files still held by `FileSegmentPtr`.

## Incremental Plan

### Phase 1: Optional FileCache Initialization on Write/Storage Nodes

1. Add `dt_enable_write_filecache=false`.
2. When this configuration is enabled and remote cache configuration is available, initialize `FileCache` on write/storage nodes.
3. Keep the existing behavior when the configuration is disabled.
4. Add startup logs that clearly show whether write FileCache is enabled, along with cache paths and capacity.

### Phase 2: MetaV2 Physical Object Collection

1. Implement a MetaV2 object collector in `DMFileBlockInputStreamBuilder` or a nearby helper.
2. Use `DMFile` and `read_columns` as inputs.
3. Output deduplicated physical `.merged` S3 keys and file sizes.
4. Skip non-MetaV2 files and unmappable logical files directly, and record debug logs.
5. Add unit tests covering deduplication when multiple logical subfiles across many columns map to the same `.merged` file.

### Phase 3: Single-File Download and Pinning

1. Before constructing `DMFileReader`, call `FileCache::downloadFileForLocalReadWithRetry(...)` in order for the deduplicated physical keys.
2. Store successfully returned `FileSegmentPtr` objects in `DMFileReader::local_read_files` or `DMFileBlockInputStream::local_read_files`.
3. Catch download exceptions and fall back to direct reads.
4. Add metrics and summary logs.

### Phase 4: Integrate the Three Segment Call Sites

1. Enable write FileCache local staging when `Segment::prepareMerge` constructs internal read streams.
2. Enable it when `Segment::prepareMergeDelta` constructs the data export stream.
3. Enable it when `Segment::prepareSplitPhysical` constructs the data streams for both sides.
4. Confirm other `ReadTag::Internal` paths are unaffected.

## Validation Strategy

### Unit Tests

- When multiple logical subfiles in a MetaV2 DMFile map to the same `.merged` file, collect only one physical object.
- Collect only the substreams corresponding to `read_columns`, and do not collect unread columns.
- Non-MetaV2 DMFiles do not trigger localization.
- If a logical file cannot be mapped to `merged_sub_file_infos`, fall back to direct reads.
- When `local_read_files` holds `FileSegmentPtr`, FileCache eviction does not delete a local file that is being read.
- FileCache download failures do not prevent reader construction and can fall back to direct reads.

### Integration Tests

- Use mock S3 and FileCache to construct a wide-table MetaV2 DMFile, run `prepareMergeDelta`, and confirm that the S3 `GetObject` count changes from per-column amplification to deduplicated physical `.merged` objects.
- Run `prepareMerge`, and confirm that stable DMFiles from multiple segments can all trigger localization and pinning.
- Run `prepareSplitPhysical`, and confirm that read streams for both sides can hit the local cache before writing stables.
- When `dt_enable_write_filecache=false`, confirm that the three paths behave the same as the existing direct read path.
- When FileCache capacity is small, confirm that download failures fall back to direct reads and the background task still completes.

### Benchmark Tests

For a 500-column wide table with cold FileCache, compare direct S3 reads and write FileCache local staging on:

- Total `prepareMergeDelta` time.
- Total `prepareMerge` time.
- Total `prepareSplitPhysical` time.
- Number of S3 `GetObject` calls.
- Number of currently opened `S3RandomAccessFile` objects.
- FileCache download bytes.
- Local disk read bytes.
- Impact on foreground query P99 latency.

For a hot FileCache scenario, confirm that staging mostly produces hits and does not introduce significant extra latency.

## Risks and Mitigations

1. Background tasks may fill local disk capacity.
   - Downloads must go through FileCache reservation and capacity quotas. Failures always fall back to direct reads. `dt_enable_write_filecache` is disabled by default.

2. Downloading entire `.merged` files may cause read amplification.
   - The first version only covers three broad-read background tasks. Metrics record downloaded bytes and fallback counts so read amplification can be evaluated.

3. Background downloads may compete with foreground query resources.
   - Reuse the existing FileCache download path and S3 read limiter. The first version does not perform concurrent batch downloads, reducing instantaneous impact.

4. Enabling FileCache on write/storage nodes may increase operational complexity.
   - `dt_enable_write_filecache=false` keeps the feature disabled by default. Startup logs make the state explicit. Disabling it fully falls back to existing behavior.

5. Object collection may miss logical files that are actually read.
   - The first version always falls back to direct reads. The collector covers data, mark, and necessary index files. Mock S3 tests verify direct fallback counts.

6. Eager `ColumnReadStream` creation may still create many local file descriptors.
   - The first version only addresses S3 stream amplification. Local file descriptor optimization is left as future work.

## Alternatives Considered

### 1. Introduce `RemoteReadPolicy::Auto`

Not adopted. `Auto` requires heuristics based on column count, bytes, segment size, FileCache state, and other conditions. Its behavior is hard to explain and increases rollout and troubleshooting complexity. The first version uses the explicit `dt_enable_write_filecache` switch and fixes the covered scope to three background tasks.

### 2. Add a FileCache Batch Download API

Not adopted for the first version. A batch API could improve throughput and centralize statistics, but it would expand FileCache interface and concurrency-control changes. The first version calls the existing single-file API one by one to validate benefits and semantic correctness first.

### 3. Support Non-MetaV2 Files

Not adopted for the first version. Newly generated remote DMFiles in the disaggregated architecture use MetaV2. Non-MetaV2 support would introduce separate collection logic for `.dat`, `.mrk`, and `.idx` files, expanding the implementation surface.

### 4. Download to a Handwritten Temporary Directory in `Segment`

Not recommended. This approach bypasses FileCache capacity control, deduplication, LRU, failure cleanup, download throttling, and pin semantics. It would also require reimplementing temporary file cleanup, S3 retry, disk-full handling, and task failure recovery.

### 5. Immediately Make `DMFileReader` Lazily Create `ColumnReadStream`

Valuable, but not part of the first version. Lazy initialization can reduce local file descriptors and some memory peaks, but it does not directly solve the core problem of direct S3 reads amplifying streams by column.

## Future Optimizations

The following items are not included in the first version and are kept as future optimizations:

- Add `RemoteReadPolicy::Auto` or threshold strategies to enable the feature automatically based on column count, bytes, or segment size.
- Add `FileCache::downloadFilesForLocalReadWithRetry(...)` as a batch API with task-level deduplication, concurrency control, and statistics.
- Support localization for non-MetaV2 DMFiles, including `.dat`, `.mrk`, and `.idx`.
- Add fallback=`fail`, allowing background tasks to fail and retry instead of continuing direct reads when S3 pressure is high.
- Make `ColumnReadStream` lazily initialized to reduce local file descriptors and memory peaks.
- Configure independent concurrency, priority, or byte limits for write FileCache downloads.
- Integrate more internal read paths into the same mechanism, such as replace stable data and local index build.

## Open Questions

There are no open questions blocking the first-version design. The first-version behavior is explicitly controlled by `dt_enable_write_filecache`, disabled by default, and always falls back to direct reads when enabled.
