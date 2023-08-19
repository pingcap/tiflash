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

#include <common/types.h>

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId()
        : start_ts(0)
        , task_id(unknown_task_id){};

    MPPTaskId(UInt64 start_ts_, Int64 task_id_)
        : start_ts(start_ts_)
        , task_id(task_id_){};

    UInt64 start_ts;
    Int64 task_id;

    bool isUnknown() const { return task_id == unknown_task_id; }

    String toString() const;

    static const MPPTaskId unknown_mpp_task_id;

private:
    static constexpr Int64 unknown_task_id = -1;
};

bool operator==(const MPPTaskId & lid, const MPPTaskId & rid);
} // namespace DB

namespace std
{
template <>
class hash<DB::MPPTaskId>
{
public:
    size_t operator()(const DB::MPPTaskId & id) const
    {
        return hash<UInt64>()(id.start_ts) ^ hash<Int64>()(id.task_id);
    }
};
} // namespace std