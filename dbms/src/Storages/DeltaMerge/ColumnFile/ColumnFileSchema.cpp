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

ColumnFileSchemaPtr SharedBlockSchemas::find(const Digest & digest)
{
    auto schema = column_file_schemas.get(digest);
    if (schema)
    {
        return schema.get()->lock();
    }
    else
    {
        return nullptr;
    }
}

ColumnFileSchemaPtr SharedBlockSchemas::getOrCreate(const Block & block)
{
    Digest digest = hashSchema(block);

    auto schema_outer = column_file_schemas.get(digest);
    if (schema_outer)
    {
        auto schema = schema_outer.get()->lock();
        if (schema)
        {
            return schema;
        }
    }

    auto schema = std::make_shared<ColumnFileSchema>(block);
    column_file_schemas.set(digest, std::make_shared<std::weak_ptr<ColumnFileSchema>>(schema));
    return schema;
}

std::shared_ptr<DB::DM::SharedBlockSchemas> getSharedBlockSchemas(const DMContext & context)
{
    return context.db_context.getSharedBlockSchemas();
}
} // namespace DM
} // namespace DB