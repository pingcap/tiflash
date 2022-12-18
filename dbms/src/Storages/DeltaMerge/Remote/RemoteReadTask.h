
#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>
#include <common/types.h>

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace DB
{
class Context;
namespace DM
{
class RemoteReadTask;
using RemoteReadTaskPtr = std::shared_ptr<RemoteReadTask>;
class RemoteTableReadTask;
using RemoteTableReadTaskPtr = std::shared_ptr<RemoteTableReadTask>;
struct RemoteSegmentReadTask;
using RemoteSegmentReadTaskPtr = std::shared_ptr<RemoteSegmentReadTask>;

enum class SegmentReadTaskState
{
    Init,
    Error,
    PersistedDataReady,
    AllReady,
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
    void updateTaskState(const RemoteSegmentReadTaskPtr & seg_task, bool meet_error);

    // Return a segment read task that is ready for reading.
    RemoteSegmentReadTaskPtr nextReadyTask();

private:
    void insertReadyTask(const RemoteSegmentReadTaskPtr & seg_task, std::unique_lock<std::mutex> &);

private:
    // The original number of segment tasks
    // Only assign when init
    size_t num_segments;

    // A task pool for fetching data from write nodes
    mutable std::mutex mtx_tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr> tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr>::iterator curr_store;

    // A task pool for segment tasks
    // The tasks are sorted by the ready state of segments
    std::mutex mtx_ready_tasks;
    std::condition_variable cv_ready_tasks;
    std::map<SegmentReadTaskState, std::list<RemoteSegmentReadTaskPtr>> ready_segment_tasks;
};

// Represent a read tasks from one write node
class RemoteTableReadTask
{
public:
    explicit RemoteTableReadTask(UInt64 store_id_, UInt64 table_id_)
        : store_id(store_id_)
        , table_id(table_id_)
    {}

    UInt64 storeID() const { return store_id; }

    UInt64 tableID() const { return table_id; }

    // static RemoteTableReadTaskPtr buildFrom(const DMContext & context, SegmentReadTasks & tasks);

    static RemoteTableReadTaskPtr buildFrom(
        const Context & db_context,
        UInt64 store_id,
        const dtpb::DisaggregatedPhysicalTable & table);

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

private:
    const UInt64 store_id;
    const UInt64 table_id;
    mutable std::mutex mtx_tasks;
    // The remote segment tasks
    std::list<RemoteSegmentReadTaskPtr> tasks;
};

struct RemoteSegmentReadTask
{
    SegmentReadTaskState state = SegmentReadTaskState::Init;
    UInt64 store_id;
    TableID table_id;
    UInt64 segment_id;
    RowKeyRanges ranges;

    // The snapshot of reading ids acquired from write node
    std::vector<UInt64> delta_page_ids;
    std::vector<UInt64> stable_files;

    // FIXME: These should be only stored in write node
    SegmentPtr segment;
    SegmentSnapshotPtr segment_snap;

    BlockInputStreamPtr getInputStream(
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & key_ranges,
        UInt64 read_tso,
        const DM::RSOperatorPtr & rs_filter,
        size_t expected_block_size);

    void receivePage(PageId page_id, Page && page)
    {
        // TODO: directly write down to local cache?
        std::lock_guard lock(mtx_queue);
        persisted_pages.push(std::make_pair(page_id, std::move(page)));
    }

    void receiveMemTable(Block && block)
    {
        // Keep the block in memory for reading (multiple times)
        std::lock_guard lock(mtx_queue);
        mem_table_blocks.push(std::move(block));
    }

private:
    std::mutex mtx_queue;
    // FIXME: this should be directly persisted to local cache? Or it will consume
    // too many memory
    // A temporary queue for storing the pages
    std::queue<std::pair<PageId, Page>> persisted_pages;
    // A temporary queue for storing the blocks
    // from remote mem-table
    std::queue<Block> mem_table_blocks;
};

} // namespace DM
} // namespace DB
