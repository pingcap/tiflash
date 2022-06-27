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

#include <Common/setThreadName.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "Common/Exception.h"
#include "Core/Block.h"
#include "Debug/DBGInvoker.h"
#include "Storages/DeltaMerge/DMContext.h"
#include "Storages/DeltaMerge/StableValueSpace.h"
#include "boost/core/noncopyable.hpp"
#include "common/logger_useful.h"
#include "common/types.h"
namespace DB
{
namespace DM
{
struct DMContext;
struct SegmentReadTask;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

using DMContextPtr = std::shared_ptr<DMContext>;
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

class PendingBlockStatistic
{
public:
    explicit PendingBlockStatistic() 
        : pending_block_count(0)
        , pending_block_byte(0)
        , total_block_count(0)
        , total_block_byte(0) {}

    void push(const Block & block)
    {
        pending_block_count.fetch_add(1, std::memory_order_relaxed);
        total_block_count.fetch_add(1, std::memory_order_relaxed);

        auto b = block.bytes();
        pending_block_byte.fetch_add(b, std::memory_order_relaxed);
        total_block_byte.fetch_add(b, std::memory_order_relaxed);
    }

    void pop(const Block & block)
    {
        if (likely(block))
        {
            pending_block_count.fetch_sub(1, std::memory_order_relaxed);
            pending_block_byte.fetch_sub(block.bytes(), std::memory_order_relaxed);
        }
    }

    int64_t count() const
    {
        return pending_block_count.load(std::memory_order_relaxed);
    }

    int64_t byte() const
    {
        return pending_block_byte.load(std::memory_order_relaxed);
    }

    int64_t totalCount() const
    {
        return total_block_count.load(std::memory_order_relaxed);
    }
    int64_t totalByte() const
    {
        return total_block_byte.load(std::memory_order_relaxed);
    }
private:
    std::atomic<int64_t> pending_block_count;
    std::atomic<int64_t> pending_block_byte;
    std::atomic<int64_t> total_block_count;
    std::atomic<int64_t> total_block_byte;
};

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    explicit SegmentReadTaskPool(
        int64_t table_id_,
        const DMContextPtr & dm_context_,
        const ColumnDefines & columns_to_read_,
        const RSOperatorPtr & filter_,
        UInt64 max_version_,
        size_t expected_block_size_,
        bool is_raw_,
        bool do_range_filter_for_raw_,
        SegmentReadTasks && tasks_)
        : id(nextPoolId())
        , table_id(table_id_)
        , dm_context(dm_context_)
        , columns_to_read(columns_to_read_)
        , filter(filter_)
        , max_version(max_version_)
        , expected_block_size(expected_block_size_)
        , is_raw(is_raw_)
        , do_range_filter_for_raw(do_range_filter_for_raw_)
        , tasks(std::move(tasks_))
        , log(&Poco::Logger::get("SegmentReadTaskPool"))
        , unordered_input_stream_ref_count(0)
    {}

    ~SegmentReadTaskPool()
    {
        auto [pop_times, pop_empty_times, peak_queue_size] = q.getStat();
        auto total_count = local_pending_stat.totalCount();
        auto total_byte = local_pending_stat.totalByte();
        auto avg_byte = total_count > 0 ? total_byte / total_count : 0;
        LOG_FMT_DEBUG(log, "pool {} pop_times {} pop_empty_times {} pop_empty_ratio {} peak_queue_size {} block_avg_bytes {} => {} MB",
            id, pop_times, pop_empty_times, pop_empty_times * 1.0 / pop_times, peak_queue_size, avg_byte, peak_queue_size * avg_byte * 1.0 / 1024 / 1024);
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
    
    uint64_t getId() const { return id; }
    
    int64_t tableId() const { return table_id; }

    const SegmentReadTasks & getTasks() const { return tasks; }

    BlockInputStreamPtr getInputStream(UInt64 seg_id);

    void finishSegment(UInt64 seg_id);

    void pushBlock(Block && block) 
    { 
        local_pending_stat.push(block);
        global_pending_stat.push(block);
        q.push(std::move(block)); 
    }

    void popBlock(Block & block) 
    { 
        q.pop(block);
        local_pending_stat.pop(block);
        global_pending_stat.pop(block);
    }

    int64_t pendingBlockCount() const
    {
        return local_pending_stat.count();
    }

    int64_t pendingBlockByte() const
    {
        return local_pending_stat.byte();
    }

    int64_t activeSegmentCount()
    {
        std::lock_guard lock(mutex);
        return active_segment_ids.size();
    }
    
     
    std::pair<uint64_t, std::vector<uint64_t>> scheduleSegment(
        const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments,
        uint64_t expected_merge_count);

    void increaseUnorderedInputStreamRefCount()
    {
        unordered_input_stream_ref_count.fetch_add(1, std::memory_order_relaxed);
    }
    void decreaseUnorderedInputStreamRefCount()
    {
        unordered_input_stream_ref_count.fetch_sub(1, std::memory_order_relaxed);
    }
    bool expired()
    {
        return unordered_input_stream_ref_count.load(std::memory_order_relaxed) == 0;
    }
private:
    SegmentReadTaskPtr getTask(uint64_t seg_id);

    const uint64_t id;
    const int64_t table_id;
    DMContextPtr dm_context;
    ColumnDefines columns_to_read;
    RSOperatorPtr filter;
    const UInt64 max_version;
    const size_t expected_block_size;
    const bool is_raw;
    const bool do_range_filter_for_raw;

    std::mutex mutex;
    SegmentReadTasks tasks;
    std::unordered_set<UInt64> active_segment_ids;
    WorkQueue<Block> q;
    PendingBlockStatistic local_pending_stat;
    Poco::Logger * log;

    std::atomic<int64_t> unordered_input_stream_ref_count;

    inline static std::atomic<uint64_t> pool_id_gen{1};
    inline static PendingBlockStatistic global_pending_stat;
    static uint64_t nextPoolId()
    {
        return pool_id_gen.fetch_add(1, std::memory_order_relaxed);
    }
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPoolWeakPtr = std::weak_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPools = std::vector<SegmentReadTaskPoolPtr>;

} // namespace DM
} // namespace DB
