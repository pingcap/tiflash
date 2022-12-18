#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RemoteSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

#include <memory>
#include <mutex>

namespace DB::DM
{

RemoteReadTask::RemoteReadTask(std::vector<RemoteTableReadTaskPtr> && tasks_)
    : num_segments(0)
{
    for (const auto & table_task : tasks_)
    {
        auto res = tasks.emplace(table_task->storeID(), table_task);
        RUNTIME_CHECK_MSG(res.second, "Duplicated task from store_id={}", table_task->storeID());
        num_segments += table_task->size();

        // Push all inited tasks to ready queue
        for (const auto & task : table_task->allTasks())
        {
            if (auto iter = ready_segment_tasks.find(task->state); iter != ready_segment_tasks.end())
            {
                iter->second.push_back(task);
            }
            else
            {
                ready_segment_tasks.emplace(task->state, std::list<RemoteSegmentReadTaskPtr>{task});
            }
        }
    }
    curr_store = tasks.begin();
}

size_t RemoteReadTask::numSegments() const
{
    return num_segments;
}

RemoteSegmentReadTaskPtr RemoteReadTask::nextFetchTask()
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

void RemoteReadTask::updateTaskState(const RemoteSegmentReadTaskPtr & seg_task, bool meet_error)
{
    {
        std::unique_lock ready_lock(mtx_ready_tasks);
        const auto old_state = seg_task->state;
        auto state_iter = ready_segment_tasks.find(old_state);
        RUNTIME_CHECK(state_iter != ready_segment_tasks.end());

        // TODO: make it an unordered_map
        bool found = false;
        for (auto task_iter = state_iter->second.begin(); task_iter != state_iter->second.end(); task_iter++)
        {
            auto & task = *task_iter;
            if (task->store_id == seg_task->store_id
                && task->table_id == seg_task->table_id
                && task->segment_id == seg_task->segment_id)
            {
                task->state = meet_error ? SegmentReadTaskState::Error : SegmentReadTaskState::AllReady;
                found = true;
                // Move it into the right ready state
                state_iter->second.erase(task_iter);
                insertReadyTask(task, ready_lock);
                break;
            }
        }
        RUNTIME_CHECK(found);
    }

    cv_ready_tasks.notify_one();
}

void RemoteReadTask::insertReadyTask(const RemoteSegmentReadTaskPtr & seg_task, std::unique_lock<std::mutex> &)
{
    if (auto state_iter = ready_segment_tasks.find(seg_task->state);
        state_iter != ready_segment_tasks.end())
        state_iter->second.push_back(seg_task);
    else
        ready_segment_tasks.emplace(seg_task->state, std::list<RemoteSegmentReadTaskPtr>{seg_task});
}

RemoteSegmentReadTaskPtr RemoteReadTask::nextReadyTask()
{
    std::unique_lock ready_lock(mtx_ready_tasks);
    RemoteSegmentReadTaskPtr seg_task = nullptr;
    cv_ready_tasks.wait(ready_lock, [this, &seg_task] {
        // All segment task are processed, return a nullptr
        if (ready_segment_tasks.empty())
            return true;

        // Check whether there are segment task ready for reading
        if (auto iter = ready_segment_tasks.find(SegmentReadTaskState::AllReady); iter != ready_segment_tasks.end())
        {
            if (iter->second.empty())
                return false;
            seg_task = iter->second.front();
            iter->second.pop_front();
            return true;
        }
        return false;
    });

    return seg_task;
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

BlockInputStreamPtr RemoteSegmentReadTask::getInputStream(
    const ColumnDefines & columns_to_read,
    const RowKeyRanges & key_ranges,
    UInt64 read_tso,
    const DM::RSOperatorPtr & rs_filter,
    size_t expected_block_size)
{
    UNUSED(this, rs_filter, key_ranges, read_tso, expected_block_size);
    auto block = toEmptyBlock(columns_to_read);
    return std::make_shared<NullBlockInputStream>(block);
}

} // namespace DB::DM
