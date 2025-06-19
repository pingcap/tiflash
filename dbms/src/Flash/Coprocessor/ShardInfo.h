// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/CoprocessorHandler.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>

#include <cstddef>
#include <sstream>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

struct ShardInfo
{
    UInt64 shard_id;
    UInt64 shard_epoch;
    using KeyRanges = google::protobuf::RepeatedPtrField<coprocessor::KeyRange>;
    KeyRanges key_ranges;

    explicit ShardInfo(const coprocessor::ShardInfo & info)
        : shard_id(info.shard_id())
        , shard_epoch(info.shard_epoch())
        , key_ranges(info.ranges())
    {}

    String toString() const
    {
        std::ostringstream sb;
        sb << "ShardID: " << shard_id << ", ShardEpoch: " << shard_epoch << ", KeyRanges: ";
        for (const auto & range : key_ranges)
        {
            sb << "[" << range.start() << ", " << range.end() << ") ";
        }
        return sb.str();
    }
};

using ShardInfoMap = std::unordered_map<UInt64, ShardInfo>;
using ShardInfoList = std::vector<ShardInfo>;

class TableShardInfos
{
public:
    TableShardInfos() = default;

    static TableShardInfos create(const coprocessor::TableShardInfos & table_shard_infos)
    {
        TableShardInfos infos;
        infos.executor_id = table_shard_infos.executor_id();
        for (const auto & info : table_shard_infos.shard_infos())
        {
            ShardInfo shard_info(info);
            infos.shard_info_list.push_back(shard_info);
        }
        return infos;
    }

    String toString() const
    {
        std::ostringstream sb;
        sb << "TableShardInfos: ExecutorID: " << executor_id << ", ShardInfos: [";
        for (const auto & shard_info : shard_info_list)
        {
            sb << shard_info.toString() << ", ";
        }
        sb << "]";
        return sb.str();
    }

    String executor_id;
    ShardInfoList shard_info_list;
};

using TableShardInfoMap = std::unordered_map<UInt64, TableShardInfos>;
using TableShardInfoList = std::vector<TableShardInfos>;

class QueryShardInfos
{
public:
    QueryShardInfos() = default;

    static QueryShardInfos create(const google::protobuf::RepeatedPtrField<coprocessor::TableShardInfos> & shard_infos)
    {
        QueryShardInfos query_shard_infos;
        for (const auto & shard_info : shard_infos)
        {
            auto table_shard_info = TableShardInfos::create(shard_info);
            query_shard_infos.table_shard_info_list.push_back(table_shard_info);
        }
        return query_shard_infos;
    }

    TableShardInfos getTableShardInfosByExecutorID(String executor_id) const
    {
        for (const auto & table_shard_info : table_shard_info_list)
        {
            if (table_shard_info.executor_id == executor_id)
                return table_shard_info;
        }
        throw Exception("No TableShardInfo found for executor ID: " + executor_id, ErrorCodes::LOGICAL_ERROR);
    }


    String toString() const
    {
        std::ostringstream sb;
        sb << "QueryShardInfos: [";
        for (const auto & shard_info : table_shard_info_list)
        {
            sb << shard_info.toString() << ", ";
        }
        sb << "]";
        return sb.str();
    }

    size_t size() const { return table_shard_info_list.size(); }

    TableShardInfoList table_shard_info_list;
};

} // namespace DB
