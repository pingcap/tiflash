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

#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Interpreters/ExpressionActions.h>
#include <common/types.h>

#include <unordered_map>

namespace DB::FinalizeHelper
{
void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous)
{
    RUNTIME_CHECK(columns_from_previous >= actions->getRequiredColumnsWithTypes().size());
    if (!actions->getRequiredColumnsWithTypes().empty()
        && columns_from_previous > actions->getRequiredColumnsWithTypes().size())
    {
        actions->prependProjectInput();
    }
}

void checkSchemaContainsParentRequire(const NamesAndTypes & schema, const Names & parent_require)
{
    NameSet schema_set;
    for (const auto & column : schema)
        schema_set.insert(column.name);
    for (const auto & parent_require_column : parent_require)
    {
        RUNTIME_CHECK(
            schema_set.find(parent_require_column) != schema_set.end(),
            DB::dumpJsonStructure(schema),
            parent_require_column);
    }
}

void checkSampleBlockContainsSchema(const Block & sample_block, const NamesAndTypes & schema)
{
    for (const auto & schema_column : schema)
    {
        RUNTIME_CHECK(sample_block.has(schema_column.name), sample_block.dumpJsonStructure(), schema_column.name);

        const auto & type_in_sample_block = sample_block.getByName(schema_column.name).type;
        const auto & type_in_schema = schema_column.type;
        RUNTIME_CHECK(
            type_in_sample_block->equals(*type_in_schema),
            schema_column.name,
            type_in_sample_block->getName(),
            type_in_schema->getName());
    }
}

void checkSampleBlockContainsParentRequire(const Block & sample_block, const Names & parent_require)
{
    for (const auto & parent_require_column : parent_require)
    {
        RUNTIME_CHECK(sample_block.has(parent_require_column), sample_block.dumpJsonStructure(), parent_require_column);
    }
}
} // namespace DB::FinalizeHelper
