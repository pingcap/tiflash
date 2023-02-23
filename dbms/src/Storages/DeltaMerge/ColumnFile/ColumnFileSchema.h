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
#include <Common/LRUCache.h>
#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
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
    // we use sha256 to generate Digest for each ColumnFileSchema as the key of column_file_schemas,
    // to minimize the possibility of two different schemas having the same key in column_file_schemas.
    // Besides, we use weak_ptr to ensure we can remove the ColumnFileSchema,
    // when no one use it, to avoid too much memory usage.
    LRUCache<Digest, std::weak_ptr<ColumnFileSchema>> column_file_schemas;

public:
    explicit SharedBlockSchemas(size_t max_size)
        : column_file_schemas(max_size)
    {}
    ColumnFileSchemaPtr find(const Digest & digest);

    ColumnFileSchemaPtr getOrCreate(const Block & block);
};

std::shared_ptr<DB::DM::SharedBlockSchemas> getSharedBlockSchemas(const DMContext & context);
} // namespace DM
} // namespace DB