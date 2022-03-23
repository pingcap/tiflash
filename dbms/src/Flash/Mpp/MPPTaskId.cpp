// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/core.h>

namespace DB
{
String MPPTaskId::toString() const
{
    return isUnknown() ? "MPP<query:N/A,task:N/A>" : fmt::format("MPP<query:{},task:{}>", start_ts, task_id);
}

const MPPTaskId MPPTaskId::unknown_mpp_task_id = MPPTaskId{};

bool operator==(const MPPTaskId & lid, const MPPTaskId & rid)
{
    return lid.start_ts == rid.start_ts && lid.task_id == rid.task_id;
}
} // namespace DB
