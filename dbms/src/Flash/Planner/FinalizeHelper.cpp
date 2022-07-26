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
namespace
{
String namesToString(const Names & names)
{
    return fmt::format("{{{}}}", fmt::join(names, ","));
}

String schemaToString(const NamesAndTypes & schema)
{
    FmtBuffer bf;
    bf.append("{");
    bf.joinStr(
        schema.cbegin(),
        schema.cend(),
        [](const auto & col, FmtBuffer & fb) { fb.fmtAppend("<{}, {}>", col.name, col.type->getName()); },
        ", ");
    bf.append("}");
    return bf.toString();
}

String blockMetaToString(const Block & block)
{
    FmtBuffer bf;
    bf.append("{");
    bf.joinStr(
        block.cbegin(),
        block.cend(),
        [](const ColumnWithTypeAndName & col, FmtBuffer & fb) { fb.fmtAppend("<{}, {}>", col.name, col.type->getName()); },
        ", ");
    bf.append("}");
    return bf.toString();
}
} // namespace

void prependProjectInputIfNeed(ExpressionActionsPtr & actions, size_t columns_from_previous)
{
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
        if (unlikely(schema_set.find(parent_require_column) == schema_set.end()))
            throw TiFlashException(
                fmt::format("schema {} don't contain parent require column: {}", schemaToString(schema), parent_require_column),
                Errors::Planner::Internal);
    }
}

void checkParentRequireContainsSchema(const Names & parent_require, const NamesAndTypes & schema)
{
    NameSet parent_require_set;
    for (const auto & parent_require_column : parent_require)
        parent_require_set.insert(parent_require_column);
    for (const auto & schema_column : schema)
    {
        if (unlikely(parent_require_set.find(schema_column.name) == parent_require_set.end()))
            throw TiFlashException(
                fmt::format("parent require {} don't contain schema column: {}", namesToString(parent_require), schema_column.name),
                Errors::Planner::Internal);
    }
}

void checkSampleBlockContainsSchema(const Block & sample_block, const NamesAndTypes & schema)
{
    for (const auto & schema_column : schema)
    {
        if (unlikely(!sample_block.has(schema_column.name)))
            throw TiFlashException(
                fmt::format("sample block {} don't contain schema column: {}", blockMetaToString(sample_block), schema_column.name),
                Errors::Planner::Internal);

        const auto & type_in_sample_block = sample_block.getByName(schema_column.name).type->getName();
        const auto & type_in_schema = schema_column.type->getName();
        if (unlikely(type_in_sample_block != type_in_schema))
            throw TiFlashException(
                fmt::format(
                    "the type of column `{}` in sample block `{}` is different from the one in schema `{}`",
                    schema_column.name,
                    type_in_sample_block,
                    type_in_schema),
                Errors::Planner::Internal);
    }
}

void checkSampleBlockContainsParentRequire(const Block & sample_block, const Names & parent_require)
{
    for (const auto & parent_require_column : parent_require)
    {
        if (unlikely(!sample_block.has(parent_require_column)))
            throw TiFlashException(
                fmt::format("sample block {} don't contain parent_require column: {}", blockMetaToString(sample_block), parent_require_column),
                Errors::Planner::Internal);
    }
}
} // namespace DB::FinalizeHelper
