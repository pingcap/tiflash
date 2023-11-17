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

#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <kvproto/mpp.pb.h>

namespace DB::DM
{
const DisaggTaskId DisaggTaskId::unknown_disaggregated_task_id{};

DisaggTaskId::DisaggTaskId(const disaggregated::DisaggTaskMeta & task_meta)
    : mpp_task_id(
        task_meta.start_ts(),
        task_meta.task_id(),
        task_meta.server_id(),
        task_meta.gather_id(),
        task_meta.query_ts(),
        task_meta.local_query_id(),
        /*resource_group_name=*/"",
        task_meta.connection_id(),
        task_meta.connection_alias())
    , executor_id(task_meta.executor_id())
{}

disaggregated::DisaggTaskMeta DisaggTaskId::toMeta() const
{
    disaggregated::DisaggTaskMeta meta;
    meta.set_start_ts(mpp_task_id.gather_id.query_id.start_ts);
    meta.set_server_id(mpp_task_id.gather_id.query_id.server_id);
    meta.set_query_ts(mpp_task_id.gather_id.query_id.query_ts);
    meta.set_local_query_id(mpp_task_id.gather_id.query_id.local_query_id);
    meta.set_gather_id(mpp_task_id.gather_id.gather_id);
    meta.set_task_id(mpp_task_id.task_id);
    meta.set_executor_id(executor_id);
    return meta;
}

bool operator==(const DisaggTaskId & lhs, const DisaggTaskId & rhs)
{
    return lhs.mpp_task_id == rhs.mpp_task_id && lhs.executor_id == rhs.executor_id;
}
} // namespace DB::DM
