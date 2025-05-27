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
    using KeyRanges = std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>>;
    KeyRanges key_ranges;

    ShardInfo(UInt64 id, UInt64 epoch, KeyRanges && key_ranges_)
        : shard_id(id)
        , shard_epoch(epoch)
        , key_ranges(std::move(key_ranges_))
    {}

    String toString() const
    {
        std::ostringstream sb;
        sb << "ShardID: " << shard_id << ", ShardEpoch: " << shard_epoch << ", KeyRanges: ";
        for (const auto & range : key_ranges)
        {
            sb << "[" << range.first->toString() << ", " << range.second->toString() << ") ";
        }
        return sb.str();
    }
};

using ShardInfoMap = std::unordered_map<UInt64, ShardInfo>;
using ShardInfoList = std::vector<ShardInfo>;

class QueryShardInfos
{
public:
    QueryShardInfos() = default;
    static QueryShardInfos create(const google::protobuf::RepeatedPtrField<coprocessor::ShardInfo> & shard_infos)
    {
        QueryShardInfos query_shard_infos;
        for (const auto & shard_info : shard_infos)
        {
            auto key_ranges = genCopKeyRange(shard_info.ranges());
            ShardInfo info(shard_info.shard_id(), shard_info.shard_epoch(), std::move(key_ranges));
            query_shard_infos.shard_info_list.push_back(info);
        }
        return query_shard_infos;
    }

    String toString() const
    {
        std::ostringstream sb;
        sb << "QueryShardInfos: [";
        for (const auto & shard_info : shard_info_list)
        {
            sb << shard_info.toString() << ", ";
        }
        sb << "]";
        return sb.str();
    }

    size_t size() const { return shard_info_list.size(); }

    ShardInfoList shard_info_list;
};

} // namespace DB
