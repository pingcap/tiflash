
#pragma once

#include <Common/Allocator.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
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

class RemoteReadTask;
using RemoteReadTaskPtr = std::shared_ptr<RemoteReadTask>;
class RemoteTableReadTask;
using RemoteTableReadTaskPtr = std::shared_ptr<RemoteTableReadTask>;
class RemoteSegmentReadTask;
using RemoteSegmentReadTaskPtr = std::shared_ptr<RemoteSegmentReadTask>;

namespace Remote
{
class LocalPageCache;
using LocalPageCachePtr = std::shared_ptr<LocalPageCache>;
} // namespace Remote

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

class RemoteReadTask
{
public:
    explicit RemoteReadTask(std::vector<RemoteTableReadTaskPtr> && tasks_);

    size_t numSegments() const;

    // Return a segment task that need to fetch pages from
    // write node.
    RemoteSegmentReadTaskPtr nextFetchTask();

    // After the fetch pages done for a segment task, the
    // worker thread need to update the task state.
    // Then the read threads can know the segment is ready
    // or there is error happened.
    void updateTaskState(
        const RemoteSegmentReadTaskPtr & seg_task,
        SegmentReadTaskState target_state,
        bool meet_error);

    void allDataReceive(const String & end_err_msg);

    // Return a segment read task that is ready for some preparation
    // to speed up later reading
    RemoteSegmentReadTaskPtr nextTaskForPrepare();

    // Return a segment read task that is ready for reading.
    RemoteSegmentReadTaskPtr nextReadyTask();

    const String & getErrorMessage() const;

    friend class tests::RemoteReadTaskTest;

private:
    void insertTask(const RemoteSegmentReadTaskPtr & seg_task, std::unique_lock<std::mutex> &);

    bool doneOrErrorHappen() const;

private:
    // The original number of segment tasks
    // Only assign when init
    size_t num_segments;

    // A task pool for fetching data from write nodes
    mutable std::mutex mtx_tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr> tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr>::iterator curr_store;

    // A task pool for segment tasks
    // The tasks are sorted by the ready state of segment tasks
    mutable std::mutex mtx_ready_tasks;
    std::condition_variable cv_ready_tasks;
    String err_msg;
    std::map<SegmentReadTaskState, std::list<RemoteSegmentReadTaskPtr>> ready_segment_tasks;
};

// Represent a read tasks from one write node
class RemoteTableReadTask
{
public:
    RemoteTableReadTask(
        UInt64 store_id_,
        TableID table_id_,
        DisaggregatedTaskId snap_id_,
        const String & address_)
        : store_id(store_id_)
        , table_id(table_id_)
        , snapshot_id(std::move(snap_id_))
        , address(address_)
    {}

    UInt64 storeID() const { return store_id; }

    TableID tableID() const { return table_id; }

    static dtpb::DisaggregatedPhysicalTable getSerializeTask(
        TableID table_id,
        SegmentReadTasks & tasks);

    static RemoteTableReadTaskPtr buildFrom(
        const Context & db_context,
        UInt64 store_id,
        const String & address,
        const DisaggregatedTaskId & snapshot_id,
        const dtpb::DisaggregatedPhysicalTable & table,
        const LoggerPtr & log);

    size_t size() const
    {
        std::lock_guard guard(mtx_tasks);
        return tasks.size();
    }

    RemoteSegmentReadTaskPtr nextTask()
    {
        std::lock_guard gurad(mtx_tasks);
        if (tasks.empty())
            return nullptr;
        auto task = tasks.front();
        tasks.pop_front();
        return task;
    }

    const std::list<RemoteSegmentReadTaskPtr> & allTasks() const
    {
        return tasks;
    }

    friend class tests::RemoteReadTaskTest;

private:
    const UInt64 store_id;
    const TableID table_id;
    const DisaggregatedTaskId snapshot_id;
    const String address;

    mutable std::mutex mtx_tasks;
    // The remote segment tasks
    std::list<RemoteSegmentReadTaskPtr> tasks;
};

class RemoteSegmentReadTask
{
public:
    static RemoteSegmentReadTaskPtr buildFrom(
        const Context & db_context,
        const dtpb::DisaggregatedSegment & proto,
        const DisaggregatedTaskId & snapshot_id,
        UInt64 store_id,
        TableID table_id,
        const String & address,
        const LoggerPtr & log);

    // The page ids that is absent from local cache
    const std::vector<UInt64> & pendingPageIds() const { return pending_page_ids; }

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

    void receivePage(dtpb::RemotePage && remote_page);

    void receiveMemTable(Block && block)
    {
        // Keep the block in memory for reading (multiple times)
        std::lock_guard lock(mtx_queue);
        mem_table_blocks.push(std::move(block));
    }

    void prepare();

    friend class tests::RemoteReadTaskTest;

    // Only used by buildFrom
    RemoteSegmentReadTask(
        DisaggregatedTaskId snapshot_id_,
        UInt64 store_id_,
        TableID table_id_,
        UInt64 segment_id_,
        String address_);

public:
    SegmentReadTaskState state = SegmentReadTaskState::Init;
    const DisaggregatedTaskId snapshot_id;
    const UInt64 store_id;
    const TableID table_id;
    const UInt64 segment_id;
    const String address;

private:
    Remote::LocalPageCachePtr page_cache;

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

    // FIXME: this should be directly persisted to local cache? Or it will consume
    // too many memory
    // A temporary queue for storing the pages
    // std::queue<std::pair<PageId, Page>> persisted_pages;
    // A temporary queue for storing the blocks
    // from remote mem-table
    std::queue<Block> mem_table_blocks;

    static Allocator<false> allocator;
};

} // namespace DM
} // namespace DB
