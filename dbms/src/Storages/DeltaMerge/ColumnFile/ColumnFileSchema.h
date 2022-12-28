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
#include <Common/TiFlashMetrics.h>
#include "Storages/DeltaMerge/DeltaMergeDefines.h"

namespace DB
{
namespace DM
{
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
        
        GET_METRIC(tiflash_column_file_info, column_file_schema_count).Increment();
    }

    ~ColumnFileSchema()
    {
        GET_METRIC(tiflash_column_file_info, column_file_schema_count).Decrement();
    }

    const DataTypePtr & getDataType(ColId column_id) const
    {
        // Note that column_id must exist
        auto index = colid_to_offset.at(column_id);
        return schema.getByPosition(index).type;
    }

    String toString() const
    {
        return "{schema:" + (schema ? schema.dumpStructure() : "none");
    }

    const Block & getSchema() const { return schema; }
    const ColIdToOffset & getColIdToOffset() const { return colid_to_offset; }
};

using ColumnFileSchemaPtr = std::shared_ptr<ColumnFileSchema>;

} // namespace DM
} // namespace DB