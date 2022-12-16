
#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>
#include <common/types.h>

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

class RemoteReadTask
{
public:
    explicit RemoteReadTask(std::vector<RemoteTableReadTaskPtr> && tasks_);

    RemoteSegmentReadTaskPtr nextTask();

private:
    mutable std::mutex mtx_tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr> tasks;
    std::unordered_map<UInt64, RemoteTableReadTaskPtr>::iterator curr_store;
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

private:
    const UInt64 store_id;
    const UInt64 table_id;
    mutable std::mutex mtx_tasks;
    // The remote segment tasks
    std::list<RemoteSegmentReadTaskPtr> tasks;
};

struct RemoteSegmentReadTask
{
    TableID table_id;
    UInt64 segment_id;
    RowKeyRanges ranges;

    // The snapshot of reading ids acquired from write node
    std::vector<UInt64> delta_page_ids;
    std::vector<UInt64> stable_files;

    // FIXME: These should be only stored in write node
    SegmentPtr segment;
    SegmentSnapshotPtr segment_snap;
};

} // namespace DM
} // namespace DB
