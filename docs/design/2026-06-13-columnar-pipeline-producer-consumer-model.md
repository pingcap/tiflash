# StorageDisaggregated Columnar: Producer-Consumer Pipeline Read Model

* Author(s): JaySon-Huang
* Date: 2026-06-13

## Summary

The current `RNColumnarSourceOp` pipeline model is "serial ping-pong": each call to `executeIOImpl()` reads and deserializes one block, returns `HAS_OUTPUT`, then traverses the full CPU transform chain (generated column placeholder → cast → filter → projection) before returning to the IO pool for the next block. IO and CPU work are strictly serialized within a single pipeline task, precluding any pipeline overlap.

This document proposes changing the columnar read path to a **producer-consumer pipeline model**: introduce `ColumnarReadSourceOp` as the IO producer source, which continuously materializes readers, reads blocks, deserializes columns, and pushes them into a bounded `SharedQueue` via the existing `SharedQueueSinkOp`; the consumer group uses `SharedQueueSourceOp` (or a thin `ColumnarQueueSourceOp` wrapper) to pop blocks from the queue, then executes generated column placeholder → cast → filter → projection. The two pipeline groups are connected via `SharedQueue`, enabling parallel execution of IO reads and CPU transforms.

## Context

### Current Model

**Block reads are serial**:

```text
Within a single Pipeline Task:
  CPU: awaitImpl() → IO_IN
  IO:  executeIOImpl() → fn_read_block + deserialize → HAS_OUTPUT
  CPU: readImpl() → swap block → downstream transform ops → Sink
  CPU: awaitImpl() → IO_IN
  IO:  executeIOImpl() → fn_read_block + deserialize → HAS_OUTPUT
  ...
```

Key constraint: IO and CPU work are bound to the same task. **Reading the next block must wait for the previous block to traverse the entire transform chain.**

### Reference Model: ExchangeReceiver

`ExchangeReceiverSourceOp` already uses a producer-consumer model:

```text
gRPC network thread (producer, outside pipeline):
  → ReceivedMessageQueue::push(packet)

ExchangeReceiverSourceOp (consumer, inside pipeline):
  → tryReceive() → decode → block_queue.push()
  → readImpl() → pop from block_queue → HAS_OUTPUT → downstream transform chain
```

Producer (gRPC thread) and consumer (pipeline task) are fully decoupled and never block each other.

The `SharedQueue` infrastructure already exists (`Operators/SharedQueue.h`) and is used in `executeUnion`, `executeMppExpand` etc. with `PipelineExecGroupBuilder::addGroup()` + `SharedQueueSinkOp` / `SharedQueueSourceOp` for multi-group connections.

### Why the Current Serial Model Bottlenecks Analytical Queries

Assuming `fn_read_block + deserialize = 10ms/block`, `filter + cast + projection = 8ms/block`:

```text
Serial (current): |==10ms IO==|==8ms CPU==|==10ms IO==|==8ms CPU==| = 36ms for 2 blocks
Parallel (proposed): |==10ms IO==|==10ms IO==| = 20ms IO parallel
                     |==8ms CPU==|==8ms CPU==| = 16ms CPU parallel
                     total ≈ max(20, 16) ≈ 20ms → ~44% latency improvement
```

For analytical queries with heavy filter/projection, pipeline overlap can yield significant throughput gains.

## Goals

* Separate `fn_read_block` + column deserialization from `RNColumnarSourceOp::executeIOImpl()` into a standalone `ColumnarReadSourceOp` that executes in the IO pool and produces at most one block per call
* The consumer group uses `SharedQueueSourceOp` or `ColumnarQueueSourceOp` to pop blocks from a bounded `SharedQueue`, performing only lightweight state checks in the CPU pool
* Backpressure via `SharedQueue` capacity limit: `SharedQueueSinkOp` returns `WAIT_FOR_NOTIFY` when IO outpaces CPU, releasing the task; `SharedQueueSourceOp` / `ColumnarQueueSourceOp` returns `WAIT_FOR_NOTIFY` when CPU outpaces IO
* Maintain order compatibility with the existing generated column placeholder → extra cast → filter → projection chain
* Maintain behavioral consistency with the stream path (`RNColumnarInputStream`), without modifying the stream path
* Correctly archive table scan source profile and inbound IO profile

## Non-Goals

* Do not change the columnar helper FFI protocol
* Do not change the columnar reader FFI lifecycle or read block protocol
* Do not reintroduce the detached prefetch thread + `WAIT_FOR_NOTIFY` reader materialize pattern in this design; this pattern has been proven to exhibit a lost wakeup race in the Observed Issue
* Do not change the basic reader work allocation strategy (shared `RNColumnarReadTask` work queue); however, the producer model must prevent the same reader work from being simultaneously created by both detached prefetch and inline materialize
* Do not change TiDB's DAGRequest, table scan schema, or filter pushdown semantics
* Do not allow multiple threads to concurrently read the same `ColumnarReaderPtr`; different reader works can be processed concurrently by different producer sources
* Do not implement this model for the non-pipeline path

## Design

### Architecture Overview

```text
readThroughColumnar (pipeline overload)
  │
  ├─ [Group 1] IO Producer Group (concurrency = producer_num)
  │     ColumnarReadSourceOp (SourceOp)
  │       ├─ awaitImpl() → reader NotStarted/Creating -> IO_IN
  │       ├─ executeIOImpl() → materialize reader + fn_read_block + deserialize
  │       └─ readImpl() → emit ready block
  │     SharedQueueSinkOp
  │       └─ writeImpl() → tryPush to SharedQueue → full? WAIT_FOR_NOTIFY
  │
  ├─ SharedQueue (bounded, cap = 2~4 blocks)
  │     ┌───┬───┬───┬───┐
  │     │   │   │   │   │
  │     └───┴───┴───┴───┘
  │       ↑ push        ↓ pop
  │
  ├─ [Group 2] CPU Consumer Group (concurrency = source_num)
  │     SharedQueueSourceOp / ColumnarQueueSourceOp (SourceOp)
  │       → readImpl() → tryPop from SharedQueue → empty? WAIT_FOR_NOTIFY
  │       → has block → HAS_OUTPUT → downstream transform chain
  │
  ├─ executeGeneratedColumnPlaceholder
  ├─ extraCast
  ├─ filterConditionsWithPushedDownFilters
  └─ addColumnarTableScanProfileInfos
```

### Key Components

#### ColumnarReadSourceOp (New SourceOp)

```cpp
class ColumnarReadSourceOp : public SourceOp
{
public:
    ColumnarReadSourceOp(
        PipelineExecutorContext & exec_context,
        const String & req_id,
        RNColumnarReadTaskPtr task);

protected:
    OperatorStatus readImpl(Block & block) override; // emit cached block or EOF
    OperatorStatus awaitImpl() override;      // reader NotStarted/Creating → IO_IN
    OperatorStatus executeIOImpl() override;  // fn_read_block + deserialize

private:
    RNColumnarReadTaskPtr task;
    BlockInputStreamPtr current_input_stream;
    std::optional<Block> t_block;
    RNColumnarReaderWorkPtr current_reader_work;
    // ... reader work management, reuse the current inline materialize state machine
};
```

**Responsibilities**:
- Manage the reader work lifecycle (acquire → inline materialize or consume ready reader → read)
- Materialize the reader inline in `executeIOImpl()`, call `fn_read_block` + column deserialization, produce one block
- `readImpl()` only delivers `t_block` to the downstream `SharedQueueSinkOp`
- Reader exhausted all blocks → reset → acquire next reader work → loop

`ColumnarReadSourceOp` is immediately followed by the existing `SharedQueueSinkOp`. This follows the current `PipelineExecBuilder` contract: every pipeline exec must be `SourceOp -> TransformOp* -> SinkOp`; a self-driven sink alone is not allowed.

**Concurrency**: Recommended `producer_num = min(source_num, reader_count)`, or conservatively limit to 1 via configuration for initial landing. The constraint is that multiple threads must not concurrently read the same `ColumnarReaderPtr`; but different reader works own different readers and can be processed concurrently by different producer sources. If initially fixed to 1, the documentation and implementation must clarify this is a conservative gate, not a necessary limitation imposed by reader thread-safety.

#### SharedQueueSourceOp / ColumnarQueueSourceOp (consumer)

The consumer group can directly reuse `SharedQueueSourceOp`. If the `RNColumnarSourceOp` name must be retained for profile/name compatibility, it should be refactored into a thin queue source wrapper that no longer holds a reader work or implements `executeIOImpl()` / `awaitImpl()`.

```cpp
OperatorStatus ColumnarQueueSourceOp::readImpl(Block & block)
{
    // Pop block from SharedQueue, analogous to ExchangeReceiverSourceOp
    auto result = shared_queue_source_holder->tryPop(block);
    switch (result) {
    case MPMCQueueResult::OK:
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::EMPTY:
        setNotifyFuture(shared_queue_source_holder.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::FINISHED:
        block = {};  // EOF
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::CANCELLED:
        throw Exception("query cancelled");
    }
}
```

**No longer needed**: reader materialize / read block related `executeIOImpl()` and `awaitImpl()` (all IO logic moved to `ColumnarReadSourceOp`). Queue empty/full waits are handled by `SharedQueueSourceOp` / `SharedQueueSinkOp` using `WAIT_FOR_NOTIFY`.

#### SharedQueue Configuration

```cpp
// cap = 2~4 blocks, balancing IO/CPU pipeline overlap and memory usage
SharedQueue::build(
    exec_context,
    /*producer=*/producer_num,
    /*consumer=*/source_num,
    /*max_buffered_bytes=*/-1,  // no byte limit, block count only
    /*max_queue_size=*/4
);
```

`SharedQueue` is based on `LooseBoundedMPMCQueue<Block>`. `tryPush` returns `MPMCQueueResult::FULL` when full; `tryPop` returns `MPMCQueueResult::EMPTY` when empty.

### Backpressure Flow

```text
Scenario A: IO faster than CPU (queue full):
  SharedQueueSinkOp::writeImpl()
    → tryPush → FULL
    → setNotifyFuture(shared_queue_sink_holder)
    → WAIT_FOR_NOTIFY ──► release producer pipeline task
  consumer source consumes → shared_queue notify write waiters → producer pipeline woken → continue push

Scenario B: CPU faster than IO (queue empty):
  SharedQueueSourceOp / ColumnarQueueSourceOp::readImpl()
    → tryPop → EMPTY
    → setNotifyFuture(shared_queue_source_holder)
    → WAIT_FOR_NOTIFY ──► release consumer pipeline task
  producer pushes → shared_queue notify read waiters → consumer source woken → continue pop

Scenario C: producer EOF (all reader works exhausted):
  ColumnarReadSourceOp emits empty block → SharedQueueSinkOp::writeImpl() returns FINISHED
  SharedQueueSinkOp destructor / finish → shared_queue.producerFinish()
  SharedQueueSourceOp::tryPop → FINISHED → emit empty block → downstream terminates normally
```

### SharedQueue WAIT_FOR_NOTIFY Safety

Both the producer-side `SharedQueueSinkOp` and consumer-side `SharedQueueSourceOp` / `ColumnarQueueSourceOp` use `WAIT_FOR_NOTIFY`, but their safety conditions fundamentally differ from the columnar detached thread's `WAIT_FOR_NOTIFY`.

The [design doc §Observed Issue](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race) provides a detailed analysis of the lost wakeup race in columnar reader materialize: because the producer (detached Prefetch thread) is one-shot — each reader work is materialized only once, `notifyAll` is called only once — if no waiter is registered in `pipe_cv` at the time of that call, the wakeup is permanently lost.

`SharedQueue`'s `WAIT_FOR_NOTIFY` does not suffer from this race, for two reasons:

1. **Both producer and consumer are pipeline tasks**. `ColumnarReadSourceOp -> SharedQueueSinkOp` will continue pushing blocks as long as reader works remain. `SharedQueueSourceOp` / `ColumnarQueueSourceOp` will continue popping as long as downstream needs data. Their wait targets are queue states, not one-shot reader materialize results.

2. **`SharedQueue` itself maintains read/write waiters and finish/cancel state**. `SharedQueueSinkHolder::registerTask()` registers write waiters; `SharedQueueSourceHolder::registerTask()` registers read waiters. `tryPush` / `tryPop` wake the opposite side on state changes; `producerFinish()` marks the queue as finished; `PipelineExecutorContext::cancel()` cancels registered shared queues. Thus empty/full/EOF/cancel all have queue-level state fallback.

This is analogous to the safety boundary of `ExchangeReceiver`: the wait target must be a queue that can preserve state and wake tasks on state changes, not a one-shot detached thread's single `notifyAll`.

**Constraint**: All producer sources' corresponding `SharedQueueSinkOp` instances must finish exactly once. After the last producer finishes, `SharedQueue` will not accept new pushes. At that point, if a consumer recovers from `EMPTY` state, `SharedQueueSourceOp::readImpl()`'s `tryPop` returns `FINISHED` rather than `EMPTY`, and the consumer receives EOF directly without falling into `WAIT_FOR_NOTIFY`. Hence the EOF scenario is also safe.

### Profile Archiving

Profile recording timing needs adjustment. `addColumnarTableScanProfileInfos` should be recorded after pipeline group 1's `ColumnarReadSourceOp` is added but before `SharedQueueSinkOp` is added, pointing to the source that actually performs columnar read/deserialize rather than the subsequent queue sink or consumer source:

```cpp
auto [sink_holder, source_holder] = SharedQueue::build(exec_context, producer_num, source_num, -1, 4);

// Group 1: ColumnarReadSourceOp (the actual IO work)
for (size_t i = 0; i < producer_num; ++i)
    group_builder.addConcurrency(std::make_unique<ColumnarReadSourceOp>(exec_context, log->identifier(), task_pool));

// Record profile to ColumnarReadSourceOp. Must be done before setSinkOp(SharedQueueSinkOp),
// otherwise getCurIOProfileInfos() will see the sink profile.
addColumnarTableScanProfileInfos(context, group_builder, table_scan);

group_builder.transform([&](auto & builder) {
    builder.setSinkOp(std::make_unique<SharedQueueSinkOp>(exec_context, log->identifier(), sink_holder));
});

// Group 2: queue consumer
auto header = group_builder.getCurrentHeader();
group_builder.addGroup();
for (size_t i = 0; i < source_num; ++i)
    group_builder.addConcurrency(
        std::make_unique<SharedQueueSourceOp>(exec_context, log->identifier(), header, source_holder));
```

`ColumnarReadSourceOp::getIOProfileInfo()` should follow the current `RNColumnarSourceOp` table scan IO profile semantics. `SharedQueueSinkOp` / `SharedQueueSourceOp` profiles must not be archived as table scan profiles.

### Compatibility with Existing Transform Chain

The two groups form the following pipeline structure:

```text
Group 1:  ColumnarReadSourceOp → SharedQueueSinkOp
                                      │
                              SharedQueue  (bounded)
                                      │
Group 2:  SharedQueueSourceOp / ColumnarQueueSourceOp
            → GeneratedColumnPlaceHolder → extraCast
            → filterConditionsWithPushedDownFilters → projection
```

`executeGeneratedColumnPlaceholder`, `extraCast`, `filterConditionsWithPushedDownFilters`, and other transforms are still added after group 2's queue source, requiring no changes.

### Integration with Reader Materialize Async Mechanism

`ColumnarReadSourceOp` reuses the current `RNColumnarSourceOp` inline materialize logic, rather than restoring `startAsyncMaterializeReader` + `WAIT_FOR_NOTIFY`:

```cpp
// ColumnarReadSourceOp::awaitImpl / executeIOImpl:
// - acquire reader work
// - Ready -> consume reader
// - NotStarted/Creating -> IO_IN -> createColumnarReaderWithBackoff inline
// - Failed/Consumed -> throw
// - reader Ready -> create input stream -> READING
```

If detached prefetch is to be reintroduced in the future, a one-shot future/latch with completion state must first be implemented, or concurrent materialize on the same work must be disabled. Otherwise both the prefetch thread and the producer source may call `createColumnarReaderWithBackoff()` on the same `RNColumnarReaderWork`, leading to duplicate FFI reader creation and loser reader release issues.

### Compatibility with Stream Path

The stream path (`StorageDisaggregated::readThroughColumnar(const Context&, unsigned)`) is entirely unaffected; `RNColumnarInputStream` remains unchanged.

## Incremental Modification Plan

### Phase 6: Add ColumnarReadSourceOp + SharedQueue Connection

Files modified:
* New: `dbms/src/Storages/Columnar/ColumnarReadSourceOp.h`
* New: `dbms/src/Storages/Columnar/ColumnarReadSourceOp.cpp`
* Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.h` — if retaining `RNColumnarSourceOp` name, simplify it to a queue consumer; or directly use `SharedQueueSourceOp`
* Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp` — remove reader work management, executeIOImpl / awaitImpl, keep only queue pop logic or delete the class
* Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — `readThroughColumnar` pipeline overload changed to dual group + SharedQueue connection
* Modify: `dbms/CMakeLists.txt` — add new files

Implementation order:
1. Move the current `RNColumnarSourceOp` reader work management, inline materialize, and `RNColumnarInputStream::createWithReader` related logic to `ColumnarReadSourceOp`, keeping at most one block per `executeIOImpl()`.
2. Add `producer_num` `ColumnarReadSourceOp` instances to the producer group.
3. Record table scan profile after producer sources are added but before `SharedQueueSinkOp` is added.
4. Attach `SharedQueueSinkOp` to the producer group, then call `addGroup()` to create the consumer group.
5. Consumer group uses `SharedQueueSourceOp` or lightweight `ColumnarQueueSourceOp`, followed by generated column, extra cast, filter, projection.

### Phase 7: Profile Adjustment + Testing

Files modified:
* `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — profile recording adjustment
* Extend `dbms/src/Storages/tests/gtest_storage_disaggregated_columnar.cpp`
* End-to-end validation

## Validation Strategy

### Unit Tests

* `ColumnarReadSourceOp` state transitions: has block, reader creating, reader failed, EOF, cancelled
* `SharedQueue` connection: producer push → consumer pop → results match
* Backpressure behavior: producer returns WAIT_FOR_NOTIFY on queue full; consumer returns WAIT_FOR_NOTIFY on queue empty
* Profile archiving: table scan profile points to `ColumnarReadSourceOp`, not covered by `SharedQueueSinkOp` or consumer source
* EOF propagation: producer finish → consumer receives EOF
* reader materialize: `NotStarted` / `Creating` do not return reader-work `WAIT_FOR_NOTIFY`, instead enter `IO_IN`

### Integration Tests

* disaggregated compute + `use_columnar=1` + pipeline executor table scan
* Table scan with generated column / timestamp cast / filter
* IO pool occupancy normal under high concurrency queries (does not monopolize IO threads)
* `EXPLAIN ANALYZE` table scan runtime stats correct
* `tiflash_pipeline_wait_on_notify_tasks` metrics include `type_wait_on_shared_queue_read` / `type_wait_on_shared_queue_write`

### Runtime Checks

* IO pool and CPU pool utilization more balanced (no longer IO pool busy while CPU pool idle)
* Table scan IO wait time in `EXPLAIN ANALYZE` excludes transform time

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| ColumnarReadSourceOp crash causes SharedQueue permanent empty | downstream consumer permanently WAIT_FOR_NOTIFY | Rely on `PipelineExecutorContext::cancel()` to cancel registered shared queues; if producer catches exception locally, must also trigger query cancel |
| Queue length poorly chosen (too small → frequent switching, too large → memory pressure) | performance worse than serial model | Default cap=4, configurable; A/B test to determine optimal value |
| Profile archived after SharedQueueSinkOp attached | EXPLAIN ANALYZE reports queue sink instead of table scan IO | Must record `ColumnarReadSourceOp`'s profile before `setSinkOp(SharedQueueSinkOp)` |
| Multiple producers read same ColumnarReaderPtr | incorrect query results | Shared work queue only assigns reader works; each `ColumnarReadSourceOp` exclusively owns its current reader |
| Detached prefetch and inline materialize simultaneously create same work | Rust ptr leak, duplicate reader creation, state overwrite | Disable pipeline prefetch under producer model, or add owner/claim state and explicit loser reader release |
| SharedQueue introduces additional block copy overhead | slight throughput drop | `LooseBoundedMPMCQueue` uses `std::move` semantics; blocks are moved, no additional deep copy |
| SharedQueue WAIT_FOR_NOTIFY may cause lost wakeup | task permanently waiting | Use existing `SharedQueue` holder's read/write waiter, finish, cancel semantics; do not use custom one-shot notify queues |

## Alternatives Considered

### Alternative B: No group split, batch read in executeIOImpl

Keep single group, let `executeIOImpl()` read multiple blocks at once and cache them. Problem: reading multiple blocks stays in the IO pool longer, impacting fairness, and still cannot achieve IO/CPU parallelism.

### Alternative C: Use detached thread as producer

Use custom threads to read blocks from columnar readers and write to a custom queue. **Rejected**: Based on [lost wakeup race empirical evidence](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race), the detached thread + custom queue `WAIT_FOR_NOTIFY` pattern produces unrecoverable missed wakeups when the producer is one-shot. Additionally, detached threads bypass TaskScheduler and cannot benefit from unified fairness, cancellation, and metrics.

**Rationale for choosing current Alternative A**: Reuses existing `SharedQueue` + `PipelineExecGroupBuilder::addGroup()` infrastructure, changes are relatively small, conforms to the current `SourceOp -> SinkOp` pipeline builder contract, and IO/CPU parallelism benefits are clear.

## Open Questions

1. **Optimal SharedQueue capacity?** Requires A/B testing. Initial recommendation cap=4; too large may cause excessive block accumulation in memory.

2. **Default value for producer_num?** Recommended default `min(source_num, reader_count)`, but workload A/B comparison is needed across `1`, `source_num`, and a smaller upper bound to avoid over-occupying the IO pool.

3. **Completely disable pipeline prefetch?** To prevent the same work from being simultaneously materialized by prefetch and producer inline, it is recommended to disable detached prefetch in the initial producer model; if retained, owner/claim state must first be designed.

4. **Retain the `RNColumnarSourceOp` class name?** If the consumer is just a queue pop, directly reusing `SharedQueueSourceOp` is simplest; if operator name/profile compatibility is needed, introduce `ColumnarQueueSourceOp`, but do not put reader work management back into the consumer source.
