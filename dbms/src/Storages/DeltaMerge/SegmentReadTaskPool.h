// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>

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

    SegmentReadTask(const SegmentPtr & segment_, //
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
    }

    void pop(const Block & blk)
    {
        if (likely(blk))
        {
            pending_count.fetch_sub(1, std::memory_order_relaxed);
            pending_bytes.fetch_sub(blk.bytes(), std::memory_order_relaxed);
        }
    }

    int64_t pendingCount() const
    {
        return pending_count.load(std::memory_order_relaxed);
    }

    int64_t pendingBytes() const
    {
        return pending_bytes.load(std::memory_order_relaxed);
    }

    int64_t totalCount() const
    {
        return total_count.load(std::memory_order_relaxed);
    }
    int64_t totalBytes() const
    {
        return total_bytes.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> pending_count;
    std::atomic<int64_t> pending_bytes;
    std::atomic<int64_t> total_count;
    std::atomic<int64_t> total_bytes;
};

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    explicit SegmentReadTaskPool(
        int64_t table_id_,
        const DMContextPtr & dm_context_,
        const ColumnDefines & columns_to_read_,
        const RSOperatorPtr & filter_,
        uint64_t max_version_,
        size_t expected_block_size_,
        bool is_raw_,
        bool do_range_filter_for_raw_,
        SegmentReadTasks && tasks_,
        AfterSegmentRead after_segment_read_,
        const String & tracing_id)
        : pool_id(nextPoolId())
        , table_id(table_id_)
        , dm_context(dm_context_)
        , columns_to_read(columns_to_read_)
        , filter(filter_)
        , max_version(max_version_)
        , expected_block_size(expected_block_size_)
        , is_raw(is_raw_)
        , do_range_filter_for_raw(do_range_filter_for_raw_)
        , tasks(std::move(tasks_))
        , after_segment_read(after_segment_read_)
        , log(Logger::get("SegmentReadTaskPool", tracing_id))
        , unordered_input_stream_ref_count(0)
        , exception_happened(false)
        , mem_tracker(current_memory_tracker == nullptr ? nullptr : current_memory_tracker->shared_from_this())
    {}

    ~SegmentReadTaskPool()
    {
        auto [pop_times, pop_empty_times, max_queue_size] = q.getStat();
        auto pop_empty_ratio = pop_times > 0 ? pop_empty_times * 1.0 / pop_times : 0.0;
        auto total_count = blk_stat.totalCount();
        auto total_bytes = blk_stat.totalBytes();
        auto blk_avg_bytes = total_count > 0 ? total_bytes / total_count : 0;
        auto approximate_max_pending_block_bytes = blk_avg_bytes * max_queue_size;
        LOG_FMT_DEBUG(log, "Done. pool_id={} table_id={} pop={} pop_empty={} pop_empty_ratio={} max_queue_size={} blk_avg_bytes={} approximate_max_pending_block_bytes={:.2f}MB total_count={} total_bytes={:.2f}MB", //
                      pool_id,
                      table_id,
                      pop_times,
                      pop_empty_times,
                      pop_empty_ratio,
                      max_queue_size,
                      blk_avg_bytes,
                      approximate_max_pending_block_bytes / 1024.0 / 1024.0,
                      total_count,
                      total_bytes / 1024.0 / 1024.0);
    }

    SegmentReadTaskPtr nextTask()
    {
        std::lock_guard lock(mutex);
        if (tasks.empty())
            return {};
        auto task = tasks.front();
        tasks.pop_front();
        return task;
    }

    uint64_t poolId() const { return pool_id; }

    int64_t tableId() const { return table_id; }

    const SegmentReadTasks & getTasks() const { return tasks; }

    BlockInputStreamPtr buildInputStream(SegmentReadTaskPtr & t);

    bool readOneBlock(BlockInputStreamPtr & stream, const SegmentPtr & seg);
    void popBlock(Block & block);

    std::unordered_map<uint64_t, std::vector<uint64_t>>::const_iterator scheduleSegment(
        const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments,
        uint64_t expected_merge_count);

    int64_t increaseUnorderedInputStreamRefCount();
    int64_t decreaseUnorderedInputStreamRefCount();
    int64_t getFreeBlockSlots() const;
    bool valid() const;
    void setException(const DB::Exception & e);
    SegmentReadTaskPtr getTask(uint64_t seg_id);

    std::once_flag & addToSchedulerFlag()
    {
        return add_to_scheduler;
    }

    MemoryTrackerPtr & getMemoryTracker()
    {
        return mem_tracker;
    }

private:
    int64_t getFreeActiveSegmentCountUnlock();
    bool exceptionHappened() const;
    void finishSegment(const SegmentPtr & seg);
    void pushBlock(Block && block);

    const uint64_t pool_id;
    const int64_t table_id;
    DMContextPtr dm_context;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    const uint64_t max_version;
    const size_t expected_block_size;
    const bool is_raw;
    const bool do_range_filter_for_raw;
    SegmentReadTasks tasks;
    AfterSegmentRead after_segment_read;
    std::mutex mutex;
    std::unordered_set<uint64_t> active_segment_ids;
    WorkQueue<Block> q;
    BlockStat blk_stat;
    LoggerPtr log;

    std::atomic<int64_t> unordered_input_stream_ref_count;

    std::atomic<bool> exception_happened;
    DB::Exception exception;

    // The memory tracker of MPPTask.
    MemoryTrackerPtr mem_tracker;

    // SegmentReadTaskPool will be holded by several UnorderedBlockInputStreams.
    // It will be added to SegmentReadTaskScheduler when one of the UnorderedBlockInputStreams being read.
    // Since several UnorderedBlockInputStreams can be read by several threads concurrently, we use
    // std::once_flag and std::call_once to prevent duplicated add.
    std::once_flag add_to_scheduler;

    inline static std::atomic<uint64_t> pool_id_gen{1};
    inline static BlockStat global_blk_stat;
    static uint64_t nextPoolId()
    {
        return pool_id_gen.fetch_add(1, std::memory_order_relaxed);
    }
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPools = std::vector<SegmentReadTaskPoolPtr>;

} // namespace DM
} // namespace DB
