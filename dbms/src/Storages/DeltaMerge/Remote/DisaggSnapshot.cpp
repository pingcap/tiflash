// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Segment.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>

#include <memory>

namespace DB::DM::Remote
{

SegmentPagesFetchTask DisaggReadSnapshot::popSegTask(TableID physical_table_id, UInt64 segment_id)
{
    std::unique_lock lock(mtx);
    auto table_iter = table_snapshots.find(physical_table_id);
    if (table_iter == table_snapshots.end())
    {
        return SegmentPagesFetchTask::error(fmt::format(
            "Segment task not found by table_id, table_id={}, segment_id={}",
            physical_table_id,
            segment_id));
    }

    assert(table_iter->second->ks_physical_table_id.second == physical_table_id);
    auto seg_task = table_iter->second->popTask(segment_id);
    if (!seg_task)
    {
        return SegmentPagesFetchTask::error(fmt::format(
            "Segment task not found by segment_id, table_id={}, segment_id={}",
            physical_table_id,
            segment_id));
    }

    auto task = SegmentPagesFetchTask::task(seg_task, table_iter->second->column_defines);
    if (table_iter->second->empty())
    {
        table_snapshots.erase(table_iter);
        LOG_DEBUG(Logger::get(), "all tasks of table are pop, table_id={}", physical_table_id);
    }
    return task;
}

void DisaggReadSnapshot::iterateTableSnapshots(std::function<void(const DisaggPhysicalTableReadSnapshotPtr &)> fn) const
{
    std::shared_lock read_lock(mtx);
    for (const auto & [_, table_snapshot] : table_snapshots)
        fn(table_snapshot);
}

bool DisaggReadSnapshot::empty() const
{
    std::shared_lock read_lock(mtx);
    for (const auto & tbl : table_snapshots)
    {
        if (!tbl.second->empty())
            return false;
    }
    return true;
}

SegmentReadTasks DisaggReadSnapshot::releaseNoNeedFetchTasks()
{
    SegmentReadTasks to_release_tasks;
    std::unique_lock lock(mtx);
    for (auto & [table_id, table_snap] : table_snapshots)
        table_snap->releaseNoNeedFetchTasks(to_release_tasks);
    return to_release_tasks;
}

DisaggPhysicalTableReadSnapshot::DisaggPhysicalTableReadSnapshot(
    KeyspaceTableID ks_table_id_,
    SegmentReadTasks && tasks_)
    : ks_physical_table_id(ks_table_id_)
{
    for (auto && t : tasks_)
    {
        tasks.emplace(t->segment->segmentId(), t);
    }
}

SegmentReadTaskPtr DisaggPhysicalTableReadSnapshot::popTask(const UInt64 segment_id)
{
    std::unique_lock lock(mtx);
    if (auto iter = tasks.find(segment_id); iter != tasks.end())
    {
        auto task = iter->second;
        tasks.erase(iter);
        return task;
    }
    return nullptr;
}

void DisaggPhysicalTableReadSnapshot::releaseNoNeedFetchTasks(SegmentReadTasks & to_release_tasks)
{
    std::unique_lock lock(mtx);
    for (auto itr = tasks.begin(); itr != tasks.end();)
    {
        if (itr->second->hasColumnFileToFetch())
        {
            ++itr;
        }
        else
        {
            to_release_tasks.push_back(itr->second);
            itr = tasks.erase(itr);
        }
    }
}
} // namespace DB::DM::Remote
