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

#include <Flash/Pipeline/Exec/PipelineExec.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Storages/DeltaMerge/ReadThread/SharedBlockQueue.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>

namespace DB
{

class TableScanPipelineExecGroupBuilder : public PipelineExecGroupBuilder
{
public:
    TableScanPipelineExecGroupBuilder(DM::SharedBlockQueuePtr read_queue_, DM::Remote::DisaggReadSnapshotPtr disagg_snap_)
        : read_queue(std::move(read_queue_))
        , disagg_snapshot(std::move(disagg_snap_))
    {}

    void addPhysicalTableTask(TableID table_id, DM::Remote::DisaggPhysicalTableReadSnapshotPtr table_snap)
    {
        if (table_snap != nullptr)
        {
            RUNTIME_CHECK(disagg_snapshot != nullptr);
            disagg_snapshot->addTask(table_id, std::move(table_snap));
        }
        read_queue->addTableTask(table_id);
    }

private:
    DM::SharedBlockQueuePtr read_queue;
    DM::Remote::DisaggReadSnapshotPtr disagg_snapshot;
};

} // namespace DB
