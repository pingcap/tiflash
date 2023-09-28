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

#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/Types.h>

#include <boost/noncopyable.hpp>
#include <memory>

namespace kvrpcpb
{
class LockInfo;
} // namespace kvrpcpb

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;

class RegionRangeKeys;
using ImutRegionRangePtr = std::shared_ptr<const RegionRangeKeys>;

struct RaftCommandResult : private boost::noncopyable
{
    enum Type
    {
        Default,
        IndexError,
        BatchSplit,
        ChangePeer,
        CommitMerge,
    };

    bool sync_log;

    Type type = Type::Default;
    std::vector<RegionPtr> split_regions{};
    ImutRegionRangePtr ori_region_range;
    RegionID source_region_id;
};

struct RegionMergeResult
{
    bool source_at_left;
    UInt64 version;
};

} // namespace DB
