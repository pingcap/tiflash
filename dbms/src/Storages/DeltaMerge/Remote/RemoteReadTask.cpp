#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RemoteSegmentThreadInputStream.h>

namespace DB::DM
{

RemoteReadTask::RemoteReadTask(std::vector<RemoteTableReadTaskPtr> && tasks_)
{
    for (const auto & task : tasks_)
    {
        auto res = tasks.emplace(task->storeID(), task);
        RUNTIME_CHECK_MSG(res.second, "Duplicated task from store_id={}", task->storeID());
    }
    curr_store = tasks.begin();
}

RemoteSegmentReadTaskPtr RemoteReadTask::nextTask()
{
    // A simple scheduling policy that try to execute the segment tasks
    // from different stores in parallel
    std::lock_guard gurad(mtx_tasks);
    while (true)
    {
        if (tasks.empty())
            return nullptr;
        if (curr_store->second->size() > 0)
        {
            auto task = curr_store->second->nextTask();
            // Move to next store
            curr_store++;
            if (curr_store == tasks.end())
                curr_store = tasks.begin();
            return task;
        }
        // No tasks left in this store, erase and try to pop task from the next store
        curr_store = tasks.erase(curr_store);
        if (curr_store == tasks.end())
            curr_store = tasks.begin();
    }
}

#if 0
RemoteTableReadTaskPtr RemoteTableReadTask::buildFrom(const DMContext & context, SegmentReadTasks & tasks)
{
    RUNTIME_CHECK(context.table_id > 0);

    // FIXME store_id
    auto read_tasks = std::make_shared<RemoteTableReadTask>(-1, context.table_id);
    for (const auto & task : tasks)
    {
        auto seg_task = std::make_shared<RemoteSegmentReadTask>();
        seg_task->segment = task->segment;
        seg_task->ranges = task->ranges;

        auto delta_snap = task->segment->getDelta()->createSnapshotFromRemote(
            context,
            task->segment->getRowKeyRange());
        auto stable_snap = task->segment->getStable()->createSnapshotFromRemote(
            context,
            task->segment->getRowKeyRange());

        seg_task->segment_snap = std::make_shared<SegmentSnapshot>(std::move(delta_snap), std::move(stable_snap));

        read_tasks->tasks.emplace_back(std::move(seg_task));
    }

    return read_tasks;
}
#endif

RemoteTableReadTaskPtr RemoteTableReadTask::buildFrom(
    const Context & db_context,
    UInt64 store_id,
    const dtpb::DisaggregatedPhysicalTable & table)
{
    UNUSED(db_context, store_id, table);
    // Deserialize from `DisaggregatedPhysicalTable`, this should also
    // ensure the local cache pages.
    return std::make_shared<RemoteTableReadTask>(store_id, table.table_id());
}
} // namespace DB::DM
