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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>

#include <memory>

namespace DB
{
namespace DM
{

ColumnFileSchema::ColumnFileSchema(const Block & block)
    : schema(block.cloneEmpty())
{
    for (size_t i = 0; i < schema.columns(); ++i)
        colid_to_offset.emplace(schema.getByPosition(i).column_id, i);
}

const DataTypePtr & ColumnFileSchema::getDataType(ColId column_id) const
{
    /// Returns the data type of a column.
    /// The specified column id must exist, otherwise something unexpected will happen.
    auto index = colid_to_offset.at(column_id);
    return schema.getByPosition(index).type;
}

String ColumnFileSchema::toString() const
{
    return "{schema:" + (schema ? schema.dumpJsonStructure() : "none") + "}";
}

void SharedBlockSchemas::removeOverflow()
{
    size_t current_size = column_file_schemas.size();
    while (current_size > max_size)
    {
        const auto & digest = lru_queue.front();

        auto iter = column_file_schemas.find(digest);
        if (iter == column_file_schemas.end())
            throw Exception(String(__FUNCTION__) + " inconsistent", ErrorCodes::LOGICAL_ERROR);

        const Holder & holder = iter->second;
        if (auto p = holder.column_file_schema.lock(); p)
        {
            // If when the item is evicted, but the item is still be used by some ColumnFiles,
            // we increment this metrics, to show maybe the max_size of SharedBlockSchemas is not enough now.
            GET_METRIC(tiflash_shared_block_schemas, type_still_used_when_evict).Increment();
        }

        --current_size;
        lru_queue.pop_front();
        column_file_schemas.erase(iter);
        GET_METRIC(tiflash_shared_block_schemas, type_current_size).Decrement();
    }

    if (current_size > (1ull << 63))
    {
        throw Exception(String(__FUNCTION__) + " inconsistent, current_size < 0", ErrorCodes::LOGICAL_ERROR);
    }
}


ColumnFileSchemaPtr SharedBlockSchemas::find(const Digest & digest)
{
    std::lock_guard lock(mutex);
    auto iter = column_file_schemas.find(digest);
    if (iter == column_file_schemas.end())
    {
        GET_METRIC(tiflash_shared_block_schemas, type_miss_count).Increment();
        return nullptr;
    }

    lru_queue.splice(lru_queue.end(), lru_queue, iter->second.queue_it);
    GET_METRIC(tiflash_shared_block_schemas, type_hit_count).Increment();

    return iter->second.column_file_schema.lock();
}

ColumnFileSchemaPtr SharedBlockSchemas::getOrCreate(const Block & block)
{
    Digest digest = hashSchema(block);

    std::lock_guard lock(mutex);
    auto iter = column_file_schemas.find(digest);
    if (iter != column_file_schemas.end())
    {
        if (auto schema = iter->second.column_file_schema.lock(); schema)
        {
            GET_METRIC(tiflash_shared_block_schemas, type_hit_count).Increment();
            lru_queue.splice(lru_queue.end(), lru_queue, iter->second.queue_it);
            return schema;
        }
    }

    GET_METRIC(tiflash_shared_block_schemas, type_miss_count).Increment();
    auto schema = std::make_shared<ColumnFileSchema>(block);
    auto pair = column_file_schemas.emplace(std::piecewise_construct, std::forward_as_tuple(digest), std::forward_as_tuple());
    auto & holder = pair.first->second;
    holder.queue_it = lru_queue.insert(lru_queue.end(), digest);

    holder.column_file_schema = schema;
    GET_METRIC(tiflash_shared_block_schemas, type_current_size).Increment();

    removeOverflow();

    return schema;
}

std::shared_ptr<DB::DM::SharedBlockSchemas> getSharedBlockSchemas(const DMContext & context)
{
    return context.db_context.getSharedBlockSchemas();
}
} // namespace DM
} // namespace DB