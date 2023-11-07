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
#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>
#include <common/types.h>

#include <boost/container_hash/hash_fwd.hpp>

namespace std
{
using Digest = UInt256;
template <>
struct hash<Digest>
{
    size_t operator()(const Digest & digest) const
    {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash_value(digest.a));
        boost::hash_combine(seed, boost::hash_value(digest.b));
        boost::hash_combine(seed, boost::hash_value(digest.c));
        boost::hash_combine(seed, boost::hash_value(digest.d));
        return seed;
    }
};
} // namespace std

namespace DB
{
namespace DM
{
using Digest = UInt256;
class ColumnFileSchema
{
    friend struct Remote::Serializer;

private:
    Block schema;

    using ColIdToOffset = std::unordered_map<ColId, size_t>;
    ColIdToOffset colid_to_offset;

public:
    explicit ColumnFileSchema(const Block & block);

    const DataTypePtr & getDataType(ColId column_id) const;

    String toString() const;

    const Block & getSchema() const { return schema; }
    const ColIdToOffset & getColIdToOffset() const { return colid_to_offset; }
};

using ColumnFileSchemaPtr = std::shared_ptr<ColumnFileSchema>;

class SharedBlockSchemas
{
private:
    // we use LRU to store digest->weak_ptr<ColumnFileSchema>
    // we choose to implement a new self LRUCache, instead use common/LRUCache.h,
    // because the following reasons:
    // 1. We need to add some important metrics, to observe the effectiveness of LRU, and whether the max_size is enough.
    // 2. We need to use weak_ptr to avoid extra memory usage when the ColumnFileSchema is not used by ColumnFiles.
    // 3. If we use common/LRUCache.h, for getOrCreate, we need
    //    1. first use getOrSet to check whether the key exists.
    //    2. If the key exists, we need to check whether the value(weak_ptr.lock()) is nullptr.
    //    3. If the value is nullptr, we need to remove the key from the cache, and then create a new value by getOrSet.
    //    While to ensure the atomicity, we need to use an extra mutex in SharedBlockSchemas to protect the whole process.

    // we use sha256 to generate Digest for each ColumnFileSchema as the key of column_file_schemas,
    // to minimize the possibility of two different schemas having the same key in column_file_schemas.

    // Besides, we use weak_ptr to avoid extra memory usage when the ColumnFileSchema is not used by ColumnFiles.
    // We can use weak_ptr to observe when the item is evicted by LRU, whether it still be used by ColumnFiles,
    // to know whether the max_size is enough.

    struct Holder;
    using ColumnFileSchemaMap = std::unordered_map<Digest, Holder>;
    using LRUQueue = std::list<Digest>;
    using LRUQueueItr = typename LRUQueue::iterator;

    struct Holder
    {
        std::weak_ptr<ColumnFileSchema> column_file_schema;
        LRUQueueItr queue_it;
    };

private:
    ColumnFileSchemaMap column_file_schemas;
    LRUQueue lru_queue;

    const size_t max_size;
    std::mutex mutex;

private:
    void removeOverflow();

public:
    explicit SharedBlockSchemas(size_t max_size_)
        : max_size(max_size_)
    {}
    ColumnFileSchemaPtr find(const Digest & digest);

    ColumnFileSchemaPtr getOrCreate(const Block & block);
};

std::shared_ptr<DB::DM::SharedBlockSchemas> getSharedBlockSchemas(const DMContext & context);
} // namespace DM
} // namespace DB
