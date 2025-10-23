// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>

namespace DB::DM
{

struct TableTaskStat
{
    size_t num_read_tasks = 0;
    size_t num_active_segs = 0;
    size_t num_finished_segs = 0;
};
} // namespace DB::DM

template <>
struct fmt::formatter<DB::DM::TableTaskStat>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::DM::TableTaskStat & s, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "{{tot:{} act:{} fin:{}}}",
            s.num_read_tasks,
            s.num_active_segs,
            s.num_finished_segs);
    }
};

namespace DB::DM
{

// Statistics of blocks pushed.
// All methods are thread-safe.
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

// ActiveSegmentReadTaskQueue manages the blocks read from active segments.
// If the instance is shared among multiple SegmentReadTaskPools, the instance
// limits the total number of active segments and the total number of pending blocks
// among all SegmentReadTaskPools. a.k.a among all physical tables.
class ActiveSegmentReadTaskQueue
{
public:
    explicit ActiveSegmentReadTaskQueue(size_t max_streams, LoggerPtr log_)
        : // If the queue is too short, only 1 in the extreme case, it may cause the computation thread
        // to encounter empty queues frequently, resulting in too much waiting and thread context switching.
        // We limit the length of block queue to be 1.5 times of `num_streams_`, and in the extreme case,
        // when `num_streams_` is 1, `block_slot_limit` is at least 2.
        block_slot_limit(std::ceil(std::max(1, max_streams) * 1.5))
        // Limiting the minimum number of reading segments to 2 is to avoid, as much as possible,
        // situations where the computation may be faster and the storage layer may not be able to keep up.
        , active_segment_limit(std::max(2, max_streams))
        , log(std::move(log_))
    {}

    ~ActiveSegmentReadTaskQueue()
    {
        auto [pop_times, pop_empty_times, peak_blocks_in_queue] = q.getStat();
        auto pop_empty_ratio = pop_times > 0 ? pop_empty_times * 1.0 / pop_times : 0.0;
        auto total_count = blk_stat.totalCount();
        auto total_bytes = blk_stat.totalBytes();
        auto blk_avg_bytes = total_count > 0 ? total_bytes / total_count : 0;
        auto approx_max_pending_block_bytes = blk_avg_bytes * peak_blocks_in_queue;
        auto total_rows = blk_stat.totalRows();
        LOG_INFO(
            log,
            "ActiveSegmentReadTaskQueue finished. pop={} pop_empty={} pop_empty_ratio={:.3f} "
            "active_segment_limit={} peak_active_segments={} "
            "block_slot_limit={} peak_blocks_in_queue={} blk_avg_bytes={} approx_max_pending_block_bytes={:.2f}MB "
            "total_count={} total_bytes={:.2f}MB total_rows={} avg_block_rows={} avg_rows_bytes={}B",
            pop_times,
            pop_empty_times,
            pop_empty_ratio,
            active_segment_limit,
            peak_active_segments,
            block_slot_limit,
            peak_blocks_in_queue,
            blk_avg_bytes,
            approx_max_pending_block_bytes / 1024.0 / 1024.0,
            total_count,
            total_bytes / 1024.0 / 1024.0,
            total_rows,
            total_count > 0 ? total_rows / total_count : 0,
            total_rows > 0 ? total_bytes / total_rows : 0);
    }

    // === table task management ===

    // Add a table task.
    void addTableTask(TableID table_id, size_t num_read_tasks)
    {
        std::unique_lock lock(mu);
        tables[table_id] = TableTaskStat{
            .num_read_tasks = num_read_tasks,
        };
    }

    // Remove a table task without finishing the queue.
    void resetTableTask(TableID table_id)
    {
        std::unique_lock lock(mu);
        tables.erase(table_id);
    }

    // Remove a table task. If there is no any table task, finish the queue.
    void removeTableTask(TableID table_id, UInt64 pool_id)
    {
        String table_id_remain;
        size_t remain_count = 0;
        {
            std::unique_lock lock(mu);
            tables.erase(table_id);
            remain_count = tables.size();
            table_id_remain = remain_count > 0 ? fmt::format("{}", tables) : "none";
        }

        if (remain_count == 0)
        {
            q.finish();
        }

        LOG_INFO(
            log,
            "all segments are finished, table_id={} pool_id={} remain_count={} remain_table_ids={}",
            table_id,
            pool_id,
            remain_count,
            table_id_remain);
    }

    // Finish the queue if there is no any table task.
    // Must be called after all table tasks are added.
    void finishQueueIfEmpty()
    {
        String table_id_remain;
        {
            std::unique_lock lock(mu);
            if (tables.empty())
            {
                q.finish();
            }
            table_id_remain = tables.empty() ? "none" : fmt::format("{}", tables);
        }
        LOG_INFO(log, "finishQueueIfEmpty called, remain_table_ids={}", table_id_remain);
    }

    // Finish the queue unconditionally. Used when aborting when exception happens.
    void finishQueue() { q.finish(); }

    // === scheduling limit helpers ===

    Int64 getFreeBlockSlots() const { return block_slot_limit - blk_stat.pendingCount(); }
    Int64 getFreeActiveSegments() const
    {
        std::unique_lock lock(mu);
        return active_segment_limit - static_cast<Int64>(active_segment_ids.size());
    }

    void addActiveSegment(TableID table_id, const GlobalSegmentID & seg_id)
    {
        std::unique_lock lock(mu);
        active_segment_ids.emplace(seg_id);
        peak_active_segments = std::max(peak_active_segments, active_segment_ids.size());
        // increase the counter of active segments for the table_id
        auto iter = tables.find(table_id);
        RUNTIME_CHECK_MSG(
            iter != tables.end(),
            "table_id not found from ActiveSegmentReadTaskQueue, table_id={} tables={}",
            table_id,
            tables);
        iter->second.num_active_segs++;
    }

    size_t finishActiveSegment(TableID table_id, const GlobalSegmentID & seg_id)
    {
        std::unique_lock lock(mu);
        active_segment_ids.erase(seg_id);
        auto iter = tables.find(table_id);
        RUNTIME_CHECK_MSG(
            iter != tables.end(),
            "table_id not found from ActiveSegmentReadTaskQueue, table_id={} tables={}",
            table_id,
            tables);
        RUNTIME_CHECK_MSG(
            iter->second.num_active_segs >= 1,
            "table_id has invalid active segments number, table_id={} table_stat={} tables={}",
            table_id,
            iter->second,
            tables);
        // decrease the counter of active segments for the table_id
        --iter->second.num_active_segs;
        ++iter->second.num_finished_segs;
        return iter->second.num_active_segs;
    }

    // === queue operations ===

    void pushBlock(Block && block)
    {
        blk_stat.push(block);
        q.push(std::move(block), nullptr);
    }

    // Blocking pop
    void popBlock(Block & block)
    {
        q.pop(block);
        blk_stat.pop(block);
    }

    // Non-blocking pop
    bool tryPopBlock(Block & block)
    {
        if (q.tryPop(block))
        {
            blk_stat.pop(block);
            return true;
        }
        else
        {
            return false;
        }
    }

    void registerTask(TaskPtr && task) { q.registerPipeTask(std::move(task), NotifyType::WAIT_ON_TABLE_SCAN_READ); }

private:
    WorkQueue<Block> q;

    const Int64 block_slot_limit;
    const Int64 active_segment_limit;
    // Statistics of blocks pushed into the queue.
    BlockStat blk_stat;

    LoggerPtr log;

    mutable std::mutex mu;

    std::unordered_map<TableID, TableTaskStat> tables;
    size_t peak_active_segments = 0;
    std::unordered_set<GlobalSegmentID> active_segment_ids;
};
using ActiveSegmentReadTaskQueuePtr = std::shared_ptr<ActiveSegmentReadTaskQueue>;

} // namespace DB::DM
