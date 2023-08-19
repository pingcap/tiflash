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
#include <fmt/core.h>
#include <kvproto/mpp.pb.h>

namespace DB
{
// global unique MPP query id.
struct MPPQueryId
{
    UInt64 query_ts;
    UInt64 local_query_id;
    UInt64 server_id;
    UInt64 start_ts;
    MPPQueryId(UInt64 query_ts, UInt64 local_query_id, UInt64 server_id, UInt64 start_ts)
        : query_ts(query_ts)
        , local_query_id(local_query_id)
        , server_id(server_id)
        , start_ts(start_ts)
    {}
    explicit MPPQueryId(const mpp::TaskMeta & task_meta)
        : query_ts(task_meta.query_ts())
        , local_query_id(task_meta.local_query_id())
        , server_id(task_meta.server_id())
        , start_ts(task_meta.start_ts())
    {}
    bool operator<(const MPPQueryId & mpp_query_id) const;
    bool operator==(const MPPQueryId & rid) const;
    bool operator!=(const MPPQueryId & rid) const;
    bool operator<=(const MPPQueryId & rid) const;
    String toString() const
    {
        return fmt::format("<query_ts:{}, local_query_id:{}, server_id:{}, start_ts:{}>", query_ts, local_query_id, server_id, start_ts);
    }
};

struct MPPQueryIdHash
{
    size_t operator()(MPPQueryId const & mpp_query_id) const noexcept;
};

// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId()
        : task_id(unknown_task_id)
        , query_id({0, 0, 0, 0}){};

    MPPTaskId(UInt64 start_ts, Int64 task_id_, UInt64 server_id, UInt64 query_ts, UInt64 local_query_id)
        : task_id(task_id_)
        , query_id(query_ts, local_query_id, server_id, start_ts)
    {}

    explicit MPPTaskId(const mpp::TaskMeta & task_meta)
        : task_id(task_meta.task_id())
        , query_id(task_meta)
    {}

    Int64 task_id;
    MPPQueryId query_id;

    bool isUnknown() const { return task_id == unknown_task_id; }

    String toString() const;
    static const MPPTaskId unknown_mpp_task_id;
    static const MPPQueryId Max_Query_Id;

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
        return DB::MPPQueryIdHash()(id.query_id) ^ hash<Int64>()(id.task_id);
    }
};
} // namespace std