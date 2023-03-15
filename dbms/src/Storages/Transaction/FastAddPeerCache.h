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

#include <Poco/File.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

#include <boost/container_hash/hash_fwd.hpp>

namespace DB
{
struct TableIdentifier
{
    UInt64 key_space_id;
    DB::NamespaceId table_id;

    bool operator==(const TableIdentifier & other) const
    {
        return key_space_id == other.key_space_id && table_id == other.table_id;
    }
};
} // namespace DB

namespace std
{
template <>
struct hash<DB::TableIdentifier>
{
    size_t operator()(const DB::TableIdentifier & k) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash_value(k.key_space_id));
        boost::hash_combine(seed, boost::hash_value(k.table_id));
        return seed;
    }
};
} // namespace std

namespace DB
{
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

struct AsyncTasks;

class SegmentEndKeyCache
{
public:
    UInt64 getSegmentIdContainingKey(const DM::RowKeyValue & key);

    void build(const std::vector<std::pair<DM::RowKeyValue, UInt64>> & end_key_and_segment_ids);

    struct KeyComparator
    {
        bool operator()(const DM::RowKeyValue & key1, const DM::RowKeyValue & key2) const
        {
            return compare(key1.toRowKeyValueRef(), key2.toRowKeyValueRef());
        }
    };

private:
    std::mutex mu;

    std::condition_variable cv;

    bool is_ready = false;

    // Store the mapping from end key to segment id
    // Segment Range End -> Segment ID
    std::map<DM::RowKeyValue, UInt64, KeyComparator> end_key_to_segment_id;
};
using SegmentEndKeyCachePtr = std::shared_ptr<SegmentEndKeyCache>;

class ParsedCheckpointDataHolder
{
public:
    UniversalPageStoragePtr temp_ps;

    std::vector<String> paths = {};

    // return pair<ptr_to_cache, need_build_cache>
    std::pair<SegmentEndKeyCachePtr, bool> getSegmentEndKeyCache(const TableIdentifier & identifier);

    ~ParsedCheckpointDataHolder()
    {
        for (const auto & path : paths)
        {
            Poco::File(path).remove(true);
        }
    }

private:
    std::mutex mu; // protect segment_end_key_cache
    using SegmentEndKeyCacheMap = std::unordered_map<TableIdentifier, SegmentEndKeyCachePtr>;
    SegmentEndKeyCacheMap segment_end_key_cache_map;
};
using ParsedCheckpointDataHolderPtr = std::shared_ptr<ParsedCheckpointDataHolder>;

ParsedCheckpointDataHolderPtr buildParsedCheckpointData(Context & context, const String & manifest_key, UInt64 dir_seq);
} // namespace DB