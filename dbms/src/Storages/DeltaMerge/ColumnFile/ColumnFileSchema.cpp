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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>

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

SharedBlockSchemas::SharedBlockSchemas(DB::Context & context)
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
        return false;
    },
                                     /*multi*/ false,
                                     /*interval_ms*/ 60000);
}

SharedBlockSchemas::~SharedBlockSchemas()
{
    if (handle)
    {
        background_pool.removeTask(handle);
    }
}

ColumnFileSchemaPtr SharedBlockSchemas::find(const Digest & digest)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = column_file_schemas.find(digest);
    if (it == column_file_schemas.end())
        return nullptr;
    return it->second.lock();
}

ColumnFileSchemaPtr SharedBlockSchemas::getOrCreate(const Block & block)
{
    Digest digest = hashSchema(block);
    std::lock_guard<std::mutex> lock(mutex);
    auto it = column_file_schemas.find(digest);
    if (it == column_file_schemas.end() || it->second.expired())
    {
        auto schema = std::make_shared<ColumnFileSchema>(block);
        column_file_schemas.emplace(digest, schema);
        return schema;
    }
    else
        return it->second.lock();
}

std::shared_ptr<DB::DM::SharedBlockSchemas> getSharedBlockSchemas(const DMContext & context)
{
    return context.db_context.getSharedBlockSchemas();
}
} // namespace DM
} // namespace DB