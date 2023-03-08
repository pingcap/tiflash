// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Allocator.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache_fwd.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Transaction/Types.h>
#include <common/types.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{
class Context;
namespace DM
{
namespace tests
{
class RemoteReadTaskTest;
}

class RNRemoteReadTask;
using RNRemoteReadTaskPtr = std::shared_ptr<RNRemoteReadTask>;
class RNRemoteTableReadTask;
using RNRemoteTableReadTaskPtr = std::shared_ptr<RNRemoteTableReadTask>;
class RNRemoteSegmentReadTask;
using RNRemoteSegmentReadTaskPtr = std::shared_ptr<RNRemoteSegmentReadTask>;

enum class SegmentReadTaskState
{
    Init,
    Error,
    Receiving,
    // All data are ready for reading
    DataReady,
    // The data are ready for reading, doing place index to
    // speed up later reading
    DataReadyAndPrepraring,
    // The data are ready for reading with some preparation done
    DataReadyAndPrepared,
};

// Represent a read tasks for one disagg task.
// The read node use it as a task pool for RNRemoteSegmentReadTask.
class RNRemoteReadTask
{
public:
    explicit RNRemoteReadTask(std::vector<RNRemoteTableReadTaskPtr> && tasks_);

    ~RNRemoteReadTask();

    size_t numSegments() const;

    // Return a segment task that need to fetch pages from
    // write node.
    RNRemoteSegmentReadTaskPtr nextFetchTask();

    // After the fetch pages done for a segment task, the
    // worker thread need to update the task state.
    // Then the read threads can know the segment is ready
    // or there is error happened.
    void updateTaskState(
        const RNRemoteSegmentReadTaskPtr & seg_task,
        SegmentReadTaskState target_state,
        bool meet_error);

    void allDataReceive(const String & end_err_msg);

    // Return a segment read task that is ready for some preparation
    // to speed up later reading
    RNRemoteSegmentReadTaskPtr nextTaskForPrepare();

    // Return a segment read task that is ready for reading.
    RNRemoteSegmentReadTaskPtr nextReadyTask();

    void wakeAll() { cv_ready_tasks.notify_all(); }

    const String & getErrorMessage() const;

    friend class tests::RemoteReadTaskTest;

private:
    void insertTask(const RNRemoteSegmentReadTaskPtr & seg_task, std::unique_lock<std::mutex> &);

    bool doneOrErrorHappen() const;

private:
    // The original number of segment tasks
    // Only assign when init
    size_t num_segments;

    // A task pool for fetching data from write nodes
    mutable std::mutex mtx_tasks;
    std::unordered_map<UInt64, RNRemoteTableReadTaskPtr> tasks;
    std::unordered_map<UInt64, RNRemoteTableReadTaskPtr>::iterator curr_store;

    // A task pool for segment tasks
    // The tasks are sorted by the ready state of segment tasks
    mutable std::mutex mtx_ready_tasks;
    std::condition_variable cv_ready_tasks;
    String err_msg;
    std::map<SegmentReadTaskState, std::list<RNRemoteSegmentReadTaskPtr>> ready_segment_tasks;

    LoggerPtr log;
};

// Represent a read tasks from one write node
class RNRemoteTableReadTask
{
public:
    RNRemoteTableReadTask(
        UInt64 store_id_,
        TableID table_id_,
        DisaggTaskId snap_id_,
        const String & address_)
        : store_id(store_id_)
        , table_id(table_id_)
        , snapshot_id(std::move(snap_id_))
        , address(address_)
    {}

    StoreID storeID() const { return store_id; }

    TableID tableID() const { return table_id; }

    static RNRemoteTableReadTaskPtr buildFrom(
        const Context & db_context,
        StoreID store_id,
        const String & address,
        const DisaggTaskId & snapshot_id,
        const RemotePb::RemotePhysicalTable & table,
        const LoggerPtr & log);

    size_t size() const
    {
        std::lock_guard guard(mtx_tasks);
        return tasks.size();
    }

    RNRemoteSegmentReadTaskPtr nextTask()
    {
        std::lock_guard gurad(mtx_tasks);
        if (tasks.empty())
            return nullptr;
        auto task = tasks.front();
        tasks.pop_front();
        return task;
    }

    const std::list<RNRemoteSegmentReadTaskPtr> & allTasks() const
    {
        return tasks;
    }

    friend class tests::RemoteReadTaskTest;

private:
    const StoreID store_id;
    const TableID table_id;
    const DisaggTaskId snapshot_id;
    const String address;

    mutable std::mutex mtx_tasks;
    // The remote segment tasks
    std::list<RNRemoteSegmentReadTaskPtr> tasks;
};

class RNRemoteSegmentReadTask
{
public:
    static RNRemoteSegmentReadTaskPtr buildFrom(
        const Context & db_context,
        const RemotePb::RemoteSegment & proto,
        const DisaggTaskId & snapshot_id,
        StoreID store_id,
        TableID table_id,
        const String & address,
        const LoggerPtr & log);

    // The page ids that is absent from local cache
    const PageIdU64s & pendingPageIds() const { return pending_page_ids; }

    size_t totalCFTinys() const { return total_num_cftiny; }

    RowKeyRanges getReadRanges() const { return read_ranges; }

    BlockInputStreamPtr getInputStream(
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & key_ranges,
        UInt64 read_tso,
        const DM::RSOperatorPtr & rs_filter,
        size_t expected_block_size);

    void addPendingMsg() { num_msg_to_consume += 1; }
    bool addConsumedMsg()
    {
        num_msg_consumed += 1;
        // return there are more pending msg or not
        return num_msg_consumed == num_msg_to_consume;
    }

    void receivePage(RemotePb::RemotePage && remote_page);

    void receiveMemTable(Block && block)
    {
        // Keep the block in memory for reading (multiple times)
        std::lock_guard lock(mtx_queue);
        mem_table_blocks.push(std::move(block));
    }

    void prepare();

    friend class tests::RemoteReadTaskTest;

    // Only used by buildFrom
    RNRemoteSegmentReadTask(
        DisaggTaskId snapshot_id_,
        StoreID store_id_,
        TableID table_id_,
        UInt64 segment_id_,
        String address_,
        LoggerPtr log_);

public:
    SegmentReadTaskState state = SegmentReadTaskState::Init;
    const DisaggTaskId snapshot_id;
    const StoreID store_id;
    const TableID table_id;
    const UInt64 segment_id;
    const String address;

private:
    Remote::RNLocalPageCachePtr page_cache;

    // The snapshot of reading ids acquired from write node
    std::vector<UInt64> delta_page_ids;
    std::vector<UInt64> stable_files;

    DMContextPtr dm_context;
    SegmentPtr segment;
    RowKeyRanges read_ranges;
    SegmentSnapshotPtr segment_snap;

    // The page ids need to fetch from write node
    std::vector<UInt64> pending_page_ids;
    size_t total_num_cftiny;

public:
    std::atomic<size_t> num_msg_to_consume;
    std::atomic<size_t> num_msg_consumed;

private:
    std::mutex mtx_queue;

    // A temporary queue for storing the blocks
    // from remote mem-table
    std::queue<Block> mem_table_blocks;

    static Allocator<false> allocator;

    LoggerPtr log;
};

} // namespace DM
} // namespace DB
