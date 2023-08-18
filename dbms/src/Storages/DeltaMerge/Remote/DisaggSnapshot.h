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

#pragma once

#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Transaction/Types.h>
#include <common/defines.h>
#include <tipb/expression.pb.h>

#include <mutex>
#include <unordered_map>

namespace DB::DM::Remote
{

class DisaggReadSnapshot;
using DisaggReadSnapshotPtr = std::shared_ptr<DisaggReadSnapshot>;
class DisaggPhysicalTableReadSnapshot;
using DisaggPhysicalTableReadSnapshotPtr = std::unique_ptr<DisaggPhysicalTableReadSnapshot>;

struct SegmentPagesFetchTask
{
    SegmentReadTaskPtr seg_task;
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> output_field_types;

    String err_msg;

    bool isValid() const { return seg_task != nullptr; }

public:
    static SegmentPagesFetchTask error(String err_msg)
    {
        return SegmentPagesFetchTask{nullptr, nullptr, nullptr, std::move(err_msg)};
    }
    static SegmentPagesFetchTask task(
        SegmentReadTaskPtr seg_task,
        DM::ColumnDefinesPtr column_defines,
        std::shared_ptr<std::vector<tipb::FieldType>> output_field_types)
    {
        return SegmentPagesFetchTask{
            std::move(seg_task),
            std::move(column_defines),
            std::move(output_field_types),
            ""};
    }
};

// The read snapshot stored on the write node.
// It stores the segment tasks for reading a logical table.
class DisaggReadSnapshot
{
public:
    using TableSnapshotMap = std::unordered_map<TableID, DisaggPhysicalTableReadSnapshotPtr>;

    DisaggReadSnapshot() = default;

    // Add read tasks for a physical table.
    void addTask(TableID physical_table_id, DisaggPhysicalTableReadSnapshotPtr && task)
    {
        if (!task)
            return;
        std::unique_lock lock(mtx);
        table_snapshots.emplace(physical_table_id, std::move(task));
    }

    // Pop one segment task for reading
    SegmentPagesFetchTask popSegTask(TableID physical_table_id, UInt64 segment_id);

    void iterateTableSnapshots(std::function<void(const DisaggPhysicalTableReadSnapshotPtr &)>) const;

    bool empty() const;

    DISALLOW_COPY(DisaggReadSnapshot);

private:
    mutable std::shared_mutex mtx;
    TableSnapshotMap table_snapshots;
};

// The read snapshot of one physical table
class DisaggPhysicalTableReadSnapshot
{
    friend struct Serializer;

public:
    DisaggPhysicalTableReadSnapshot(KeyspaceTableID ks_table_id_, SegmentReadTasks && tasks_);

    SegmentReadTaskPtr popTask(UInt64 segment_id);

    ALWAYS_INLINE bool empty() const
    {
        std::shared_lock read_lock(mtx);
        return tasks.empty();
    }

    DISALLOW_COPY(DisaggPhysicalTableReadSnapshot);

public:
    const KeyspaceTableID ks_physical_table_id;

    // TODO: these members are the same in the logical table level,
    //       maybe we can reuse them to reduce memory consumption.
    DM::ColumnDefinesPtr column_defines;
    std::shared_ptr<std::vector<tipb::FieldType>> output_field_types;

private:
    mutable std::shared_mutex mtx;
    // segment_id -> SegmentReadTaskPtr
    std::unordered_map<UInt64, SegmentReadTaskPtr> tasks;
};


} // namespace DB::DM::Remote
