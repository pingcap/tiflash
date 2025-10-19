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

class SharedBlockQueue
{
public:
    explicit SharedBlockQueue(LoggerPtr log_)
        : log(std::move(log_))
    {}

    ~SharedBlockQueue()
    {
        auto [pop_times, pop_empty_times, max_queue_size] = q.getStat();
        auto pop_empty_ratio = pop_times > 0 ? pop_empty_times * 1.0 / pop_times : 0.0;
        auto total_count = blk_stat.totalCount();
        auto total_bytes = blk_stat.totalBytes();
        auto blk_avg_bytes = total_count > 0 ? total_bytes / total_count : 0;
        auto approx_max_pending_block_bytes = blk_avg_bytes * max_queue_size;
        auto total_rows = blk_stat.totalRows();
        LOG_INFO(
            log,
            "SharedBlockQueue finished. pop={} pop_empty={} pop_empty_ratio={:.3f} "
            "max_queue_size={} blk_avg_bytes={} approx_max_pending_block_bytes={:.2f}MB "
            "total_count={} total_bytes={:.2f}MB total_rows={} avg_block_rows={} avg_rows_bytes={}B",
            pop_times,
            pop_empty_times,
            pop_empty_ratio,
            max_queue_size,
            blk_avg_bytes,
            approx_max_pending_block_bytes / 1024.0 / 1024.0,
            total_count,
            total_bytes / 1024.0 / 1024.0,
            total_rows,
            total_count > 0 ? total_rows / total_count : 0,
            total_rows > 0 ? total_bytes / total_rows : 0);
    }

    void addTableTask(TableID table_id)
    {
        std::unique_lock lock(mu);
        tables.emplace(table_id);
    }

    void resetTableTask(TableID table_id)
    {
        std::unique_lock lock(mu);
        tables.erase(table_id);
    }
    void removeTableTask(TableID table_id)
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
            "removeTableTask table_id={} remain_count={} remain_table_ids={}",
            table_id,
            remain_count,
            table_id_remain);
    }

    void pushBlock(Block && block)
    {
        blk_stat.push(block);
        q.push(std::move(block), nullptr);
    }
    void popBlock(Block & block)
    {
        q.pop(block);
        blk_stat.pop(block);
    }
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

    void finishQueue() { q.finish(); }

    const BlockStat & getBlockStat() const { return blk_stat; }

private:
    WorkQueue<Block> q;

    // Statistics of blocks pushed into the queue.
    BlockStat blk_stat;

    LoggerPtr log;

    std::mutex mu;
    std::unordered_set<TableID> tables;
};
using SharedBlockQueuePtr = std::shared_ptr<SharedBlockQueue>;

} // namespace DB::DM
