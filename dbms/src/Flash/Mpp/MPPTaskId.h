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

#pragma once

#include <common/types.h>

namespace DB
{
// Identify a mpp task.
struct MPPTaskId
{
    MPPTaskId()
        : start_ts(0)
        , server_id(0)
        , query_ts(0)
        , task_id(unknown_task_id){};

    MPPTaskId(UInt64 start_ts_, Int64 task_id_, UInt64 server_id_, UInt64 query_ts_, UInt64 local_query_id_)
        : start_ts(start_ts_)
        , server_id(server_id_)
        , query_ts(query_ts_)
        , task_id(task_id_)
        , local_query_id(local_query_id_)
    {
        query_id = generateQueryID(query_ts, local_query_id, server_id, start_ts_);
    }

    UInt64 start_ts;
    UInt64 server_id;
    UInt64 query_ts;
    Int64 task_id;
    UInt64 local_query_id;
    String query_id;

    bool isUnknown() const { return task_id == unknown_task_id; }

    String toString() const;
    static const MPPTaskId unknown_mpp_task_id;
    static const String Max_Query_Id;
    static String generateQueryID(UInt64 query_ts_, UInt64 local_query_id_, UInt64 server_id_, UInt64 start_ts_);
    static std::tuple<UInt64, UInt64, UInt64> decodeQueryID(String query_id_str);

private:
    static constexpr Int64 unknown_task_id = -1;

    static String intToHex(UInt64 i)
    {
        std::stringstream stream;
        stream << std::setfill('0') << std::setw(sizeof(UInt64) * 2)
               << std::hex << i;
        return stream.str();
    }
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
        return hash<String>()(id.query_id);
    }
};
} // namespace std