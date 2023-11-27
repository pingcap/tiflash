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
    String resource_group_name;
    UInt64 connection_id;
    String connection_alias;

    MPPQueryId(
        UInt64 query_ts,
        UInt64 local_query_id,
        UInt64 server_id,
        UInt64 start_ts,
        const String & resource_group_name_,
        UInt64 connection_id_,
        const String & connection_alias_)
        : query_ts(query_ts)
        , local_query_id(local_query_id)
        , server_id(server_id)
        , start_ts(start_ts)
        , resource_group_name(resource_group_name_)
        , connection_id(connection_id_)
        , connection_alias(connection_alias_)
    {}
    explicit MPPQueryId(const mpp::TaskMeta & task_meta)
        : query_ts(task_meta.query_ts())
        , local_query_id(task_meta.local_query_id())
        , server_id(task_meta.server_id())
        , start_ts(task_meta.start_ts())
        , resource_group_name(task_meta.resource_group_name())
        , connection_id(task_meta.connection_id())
        , connection_alias(task_meta.connection_alias())
    {}
    bool operator<(const MPPQueryId & mpp_query_id) const;
    bool operator==(const MPPQueryId & rid) const;
    bool operator!=(const MPPQueryId & rid) const;
    bool operator<=(const MPPQueryId & rid) const;

    String toString() const
    {
        return fmt::format(
            "<query_ts:{}, local_query_id:{}, server_id:{}, start_ts:{}>",
            query_ts,
            local_query_id,
            server_id,
            start_ts);
    }
};

struct MPPQueryIdHash
{
    size_t operator()(MPPQueryId const & mpp_query_id) const noexcept;
};

/// A MPP query has one or more MPPGathers, each mpp gather has one or more MPPTasks. The mpp tasks in different mpp gathers are independent
/// to each other, while MPPGathers belong to the same MPP query could have dependence to each other(e.g. for query A join B, if the join is
/// not supported in TiFlash, TiDB will generate two mpp gathers, one is reading from A and the other is reading from B, the probe side's mpp
/// gather depends on the build side's mpp gather), so the smallest scheduling unit in TiFlash is MPP query, but the smallest cancel/retry unit
/// in TiFlash is MPP gather.
struct MPPGatherId
{
    Int64 gather_id;
    MPPQueryId query_id;
    MPPGatherId(Int64 gather_id_, const MPPQueryId & query_id_)
        : gather_id(gather_id_)
        , query_id(query_id_)
    {}
    MPPGatherId(
        Int64 gather_id_,
        UInt64 query_ts,
        UInt64 local_query_id,
        UInt64 server_id,
        UInt64 start_ts,
        const String & resource_group_name,
        UInt64 connection_id,
        const String & connection_alias)
        : gather_id(gather_id_)
        , query_id(query_ts, local_query_id, server_id, start_ts, resource_group_name, connection_id, connection_alias)
    {}
    explicit MPPGatherId(const mpp::TaskMeta & task_meta)
        : gather_id(task_meta.gather_id())
        , query_id(task_meta)
    {}
    String toString() const
    {
        return fmt::format(
            "gather_id:{}, query_ts:{}, local_query_id:{}, server_id:{}, start_ts:{}",
            gather_id,
            query_id.query_ts,
            query_id.local_query_id,
            query_id.server_id,
            query_id.start_ts);
    }
    bool hasMeaningfulGatherId() const { return gather_id > 0; }
    bool operator==(const MPPGatherId & rid) const;
};

struct MPPGatherIdHash
{
    size_t operator()(MPPGatherId const & mpp_gather_id) const noexcept;
};

// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId()
        : task_id(unknown_task_id)
        , gather_id(0, 0, 0, 0, 0, "", 0, ""){};

    MPPTaskId(
        UInt64 start_ts,
        Int64 task_id_,
        UInt64 server_id,
        Int64 gather_id,
        UInt64 query_ts,
        UInt64 local_query_id,
        const String resource_group_name,
        UInt64 connection_id,
        const String & connection_alias)
        : task_id(task_id_)
        , gather_id(
              gather_id,
              query_ts,
              local_query_id,
              server_id,
              start_ts,
              resource_group_name,
              connection_id,
              connection_alias)
    {}

    explicit MPPTaskId(const mpp::TaskMeta & task_meta)
        : task_id(task_meta.task_id())
        , gather_id(task_meta)
    {}

    Int64 task_id;
    MPPGatherId gather_id;

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
        return DB::MPPGatherIdHash()(id.gather_id) ^ hash<Int64>()(id.task_id);
    }
};
} // namespace std
