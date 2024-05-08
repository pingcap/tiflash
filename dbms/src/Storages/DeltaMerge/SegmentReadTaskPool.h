// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <Common/MemoryTrackerSetter.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/ReadMode.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/DeltaMerge/Segment.h>
namespace DB
{
namespace DM
{
struct SegmentReadTask;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks = std::list<SegmentReadTaskPtr>;
using AfterSegmentRead = std::function<void(const DMContextPtr &, const SegmentPtr &)>;

struct SegmentReadTask
{
    SegmentPtr segment;
    SegmentSnapshotPtr read_snapshot;
    RowKeyRanges ranges;

    SegmentReadTask(
        const SegmentPtr & segment_, //
        const SegmentSnapshotPtr & read_snapshot_,
        const RowKeyRanges & ranges_);

    explicit SegmentReadTask(const SegmentPtr & segment_, const SegmentSnapshotPtr & read_snapshot_);

    ~SegmentReadTask();

    std::pair<size_t, size_t> getRowsAndBytes() const;

    void addRange(const RowKeyRange & range) { ranges.push_back(range); }

    void mergeRanges() { ranges = DM::tryMergeRanges(std::move(ranges), 1); }

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);
};

class BlockStat
{
public:
    BlockStat()
        : pending_count(0)
        , pending_bytes(0)
        , total_count(0)
        , total_bytes(0)
    {}

    void push(const Block & blk)
    {
        pending_count.fetch_add(1, std::memory_order_relaxed);
        total_count.fetch_add(1, std::memory_order_relaxed);

        auto b = blk.bytes();
        pending_bytes.fetch_add(b, std::memory_order_relaxed);
        total_bytes.fetch_add(b, std::memory_order_relaxed);

        total_rows.fetch_add(blk.rows(), std::memory_order_relaxed);
    }

    void pop(const Block & blk)
    {
        if (likely(blk))
        {
            pending_count.fetch_sub(1, std::memory_order_relaxed);
            pending_bytes.fetch_sub(blk.bytes(), std::memory_order_relaxed);
        }
    }

    Int64 pendingCount() const { return pending_count.load(std::memory_order_relaxed); }

    Int64 pendingBytes() const { return pending_bytes.load(std::memory_order_relaxed); }

    Int64 totalCount() const { return total_count.load(std::memory_order_relaxed); }

    Int64 totalBytes() const { return total_bytes.load(std::memory_order_relaxed); }

    Int64 totalRows() const { return total_rows.load(std::memory_order_relaxed); }

private:
    std::atomic<Int64> pending_count;
    std::atomic<Int64> pending_bytes;
    std::atomic<Int64> total_count;
    std::atomic<Int64> total_bytes;
    std::atomic<Int64> total_rows;
};

// If `enable_read_thread_` is true, `SegmentReadTasksWrapper` use `std::unordered_map` to index `SegmentReadTask` by segment id,
// else it is the same as `SegmentReadTasks`, a `std::list` of `SegmentReadTask`.
// `SegmeneReadTasksWrapper` is not thread-safe.
class SegmentReadTasksWrapper
{
public:
    SegmentReadTasksWrapper(bool enable_read_thread_, SegmentReadTasks && ordered_tasks_);

    // `nextTask` pops task sequentially. This function is used when `enable_read_thread` is false.
    SegmentReadTaskPtr nextTask();

    // `getTask` and `getTasks` are used when `enable_read_thread` is true.
    SegmentReadTaskPtr getTask(UInt64 seg_id);
    const std::unordered_map<UInt64, SegmentReadTaskPtr> & getTasks() const;

    bool empty() const;

private:
    bool enable_read_thread;
    SegmentReadTasks ordered_tasks;
    std::unordered_map<UInt64, SegmentReadTaskPtr> unordered_tasks;
};

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    SegmentReadTaskPool(
        int64_t physical_table_id_,
        int extra_table_id_index_,
        const DMContextPtr & dm_context_,
        const ColumnDefines & columns_to_read_,
        const PushDownFilterPtr & filter_,
        uint64_t max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        SegmentReadTasks && tasks_,
        AfterSegmentRead after_segment_read_,
        const String & tracing_id,
        bool enable_read_thread_,
        Int64 num_streams_,
        const String & res_group_name_);

    ~SegmentReadTaskPool()
    {
        auto [pop_times, pop_empty_times, max_queue_size] = q.getStat();
        auto pop_empty_ratio = pop_times > 0 ? pop_empty_times * 1.0 / pop_times : 0.0;
        auto total_count = blk_stat.totalCount();
        auto total_bytes = blk_stat.totalBytes();
        auto blk_avg_bytes = total_count > 0 ? total_bytes / total_count : 0;
        auto approximate_max_pending_block_bytes = blk_avg_bytes * max_queue_size;
        auto total_rows = blk_stat.totalRows();
        LOG_INFO(
            log,
            "Done. pool_id={} table_id={} pop={} pop_empty={} pop_empty_ratio={} "
            "max_queue_size={} blk_avg_bytes={} approximate_max_pending_block_bytes={:.2f}MB "
            "total_count={} total_bytes={:.2f}MB total_rows={} avg_block_rows={} avg_rows_bytes={}B",
            pool_id,
            physical_table_id,
            pop_times,
            pop_empty_times,
            pop_empty_ratio,
            max_queue_size,
            blk_avg_bytes,
            approximate_max_pending_block_bytes / 1024.0 / 1024.0,
            total_count,
            total_bytes / 1024.0 / 1024.0,
            total_rows,
            total_count > 0 ? total_rows / total_count : 0,
            total_rows > 0 ? total_bytes / total_rows : 0);
    }

    SegmentReadTaskPtr nextTask();
    const std::unordered_map<UInt64, SegmentReadTaskPtr> & getTasks();
    SegmentReadTaskPtr getTask(UInt64 seg_id);

    BlockInputStreamPtr buildInputStream(SegmentReadTaskPtr & t);

    bool readOneBlock(BlockInputStreamPtr & stream, const SegmentPtr & seg);
    void popBlock(Block & block);
    bool tryPopBlock(Block & block);

    std::unordered_map<uint64_t, std::vector<uint64_t>>::const_iterator scheduleSegment(
        const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments,
        uint64_t expected_merge_count);

    Int64 increaseUnorderedInputStreamRefCount();
    Int64 decreaseUnorderedInputStreamRefCount();
    Int64 getFreeBlockSlots() const;
    Int64 getFreeActiveSegments() const;
    Int64 getPendingSegmentCount() const;
    bool valid() const;
    void setException(const DB::Exception & e);

    std::once_flag & addToSchedulerFlag() { return add_to_scheduler; }

public:
    const uint64_t pool_id;
    const int64_t physical_table_id;

    // The memory tracker of MPPTask.
    const MemoryTrackerPtr mem_tracker;

    ColumnDefines & getColumnToRead() { return columns_to_read; }

    void appendRSOperator(RSOperatorPtr & new_filter)
    {
        if (filter->rs_operator == DM::EMPTY_RS_OPERATOR)
        {
            filter->rs_operator = new_filter;
        }
        else
        {
            RSOperators children;
            children.push_back(filter->rs_operator);
            children.push_back(new_filter);
            filter->rs_operator = createAnd(children);
        }
    }

    bool isRUExhausted();

private:
    Int64 getFreeActiveSegmentsUnlock() const;
    bool exceptionHappened() const;
    void finishSegment(const SegmentPtr & seg);
    void pushBlock(Block && block);

    bool isRUExhaustedImpl();

    const int extra_table_id_index;
    DMContextPtr dm_context;
    ColumnDefines columns_to_read;
    PushDownFilterPtr filter;
    const uint64_t max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;
    SegmentReadTasksWrapper tasks_wrapper;
    AfterSegmentRead after_segment_read;
    mutable std::mutex mutex;
    std::unordered_set<uint64_t> active_segment_ids;
    WorkQueue<Block> q;
    BlockStat blk_stat;
    LoggerPtr log;

    std::atomic<Int64> unordered_input_stream_ref_count;

    std::atomic<bool> exception_happened;
    DB::Exception exception;

    // SegmentReadTaskPool will be holded by several UnorderedBlockInputStreams.
    // It will be added to SegmentReadTaskScheduler when one of the UnorderedBlockInputStreams being read.
    // Since several UnorderedBlockInputStreams can be read by several threads concurrently, we use
    // std::once_flag and std::call_once to prevent duplicated add.
    std::once_flag add_to_scheduler;

    const Int64 block_slot_limit;
    const Int64 active_segment_limit;

    const String res_group_name;
    std::mutex ru_mu;
    std::atomic<Int64> last_time_check_ru = 0;
    std::atomic<bool> ru_is_exhausted = false;
    std::atomic<UInt64> read_bytes_after_last_check = 0;

    inline static std::atomic<uint64_t> pool_id_gen{1};
    inline static BlockStat global_blk_stat;
    static uint64_t nextPoolId() { return pool_id_gen.fetch_add(1, std::memory_order_relaxed); }
    inline static constexpr Int64 check_ru_interval_ms = 100;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPools = std::vector<SegmentReadTaskPoolPtr>;

} // namespace DM
} // namespace DB


template <>
struct fmt::formatter<DB::DM::SegmentReadTaskPtr>
{
    template <typename FormatContext>
    auto format(const DB::DM::SegmentReadTaskPtr & t, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(
            ctx.out(),
            "{}_{}_{}",
            t->segment->segmentId(),
            t->segment->segmentEpoch(),
            t->read_snapshot->delta->getDeltaIndexEpoch());
    }
};
