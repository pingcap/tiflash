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
#include <IO/WriteHelpers.h>
#include <fmt/core.h>

namespace DB
{
String MPPTaskId::toString() const
{
    return isUnknown() ? "MPP<query_id:N/A,start_ts:N/A,query_ts:N/A,task_id:N/A,server_id:N/A>" : fmt::format("MPP<query:{},start_ts:{},query_ts:{},task_id:{},server_id:{},local_query_id:{}>", query_id, start_ts, query_ts, task_id, server_id, local_query_id);
}

const MPPTaskId MPPTaskId::unknown_mpp_task_id = MPPTaskId{};

constexpr UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();
const String MPPTaskId::Max_Query_Id = generateQueryID(MAX_UINT64, MAX_UINT64, MAX_UINT64, MAX_UINT64);

String MPPTaskId::generateQueryID(UInt64 query_ts_, UInt64 local_query_id_, UInt64 server_id_, UInt64 start_ts_)
{
    if (local_query_id_ == 0)
    {
        return intToHex(0) + intToHex(0) + intToHex(start_ts_);
    }
    return intToHex(query_ts_) + intToHex(local_query_id_) + intToHex(server_id_);
}

std::tuple<UInt64, UInt64, UInt64> MPPTaskId::decodeQueryID(String query_id_str)
{
    UInt64 decode_query_ts = std::stoull(query_id_str.substr(0, sizeof(Int64) * 2), 0, 16);
    UInt64 decode_local_query_id = std::stoull(query_id_str.substr(sizeof(Int64) * 2, sizeof(Int64) * 2), 0, 16);
    UInt64 decode_server_id = std::stoull(query_id_str.substr(sizeof(Int64) * 2 * 2, sizeof(UInt64) * 2), 0, 16);
    LOG_DEBUG(Logger::get(__FUNCTION__), "query_ts_={}, local_query_id_={}, server_id_={}", decode_query_ts, decode_local_query_id, decode_server_id);
    return {decode_query_ts, decode_local_query_id, decode_server_id};
}

bool operator==(const MPPTaskId & lid, const MPPTaskId & rid)
{
    return lid.query_id == rid.query_id && lid.task_id == rid.task_id;
}
} // namespace DB
