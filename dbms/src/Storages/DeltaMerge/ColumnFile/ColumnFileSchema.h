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

#include <Common/nocopyable.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <common/types.h>
#include <openssl/base.h>
#include <openssl/sha.h>

#include "boost/container_hash/hash_fwd.hpp"

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
    explicit ColumnFileSchema(const Block & schema_)
        : schema(schema_)
    {
        for (size_t i = 0; i < schema.columns(); ++i)
            colid_to_offset.emplace(schema.getByPosition(i).column_id, i);
    }

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema.getByPosition(index).type;
    }

    String toString() const
    {
        return "{schema:" + (schema ? schema.dumpStructure() : "none") + "}";
    }

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
    std::unordered_map<Digest, std::weak_ptr<ColumnFileSchema>> column_file_schemas;
    std::mutex mutex;
    BackgroundProcessingPool::TaskHandle handle;
    BackgroundProcessingPool & background_pool;

public:
    explicit SharedBlockSchemas(DB::Context & context)
        : background_pool(context.getBackgroundPool())
    {
        handle = background_pool.addTask([&, this] {
            std::lock_guard<std::mutex> lock(mutex);
            for (auto iter = column_file_schemas.begin(); iter != column_file_schemas.end();)
            {
                if (iter->second.expired())
                {
                    iter = column_file_schemas.erase(iter);
                }
                else
                {
                    ++iter;
                }
            }
            return true;
        },
                                         false);
    }

    ~SharedBlockSchemas()
    {
        background_pool.removeTask(handle);
    }

    ColumnFileSchemaPtr find(const Digest & digest)
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = column_file_schemas.find(digest);
        if (it == column_file_schemas.end())
            return nullptr;
        return it->second.lock();
    }

    void insert(const Digest & digest, const ColumnFileSchemaPtr & schema)
    {
        std::lock_guard<std::mutex> lock(mutex);
        column_file_schemas.emplace(digest, schema);
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return column_file_schemas.size();
    }
};
} // namespace DM
} // namespace DB