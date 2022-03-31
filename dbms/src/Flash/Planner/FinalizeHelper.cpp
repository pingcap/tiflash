#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Core/Names.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Interpreters/ExpressionActions.h>

#include <unordered_map>

namespace DB::FinalizeHelper
{
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
        if (schema_set.find(parent_require_column) == schema_set.end())
            throw TiFlashException(
                fmt::format("schema don't contain parent require column: {}", parent_require_column),
                Errors::Coprocessor::Internal);
    }
}

void checkParentRequireContainsSchema(const Names & parent_require, const NamesAndTypes & schema)
{
    NameSet parent_require_set;
    for (const auto & parent_require_column : parent_require)
        parent_require_set.insert(parent_require_column);
    for (const auto & schema_column : schema)
    {
        if (parent_require_set.find(schema_column.name) == parent_require_set.end())
            throw TiFlashException(
                fmt::format("parent require don't contain schema column: {}", schema_column.name),
                Errors::Coprocessor::Internal);
    }
}

void checkSampleBlockContainsSchema(const Block & sample_block, const NamesAndTypes & schema)
{
    for (const auto & schema_column : schema)
    {
        if (!sample_block.has(schema_column.name))
            throw TiFlashException(
                fmt::format("sample block don't contain schema column: {}", schema_column.name),
                Errors::Coprocessor::Internal);

        const auto & type_in_sample_block = sample_block.getByName(schema_column.name).type->getName();
        const auto & type_in_schema = schema_column.type->getName();
        if (type_in_sample_block != type_in_schema)
            throw TiFlashException(
                fmt::format(
                    "the type of column `{}` in sample block `{}` is difference from the one in schema `{}`",
                    schema_column.name,
                    type_in_sample_block,
                    type_in_schema),
                Errors::Coprocessor::Internal);
    }
}

void checkSchemaContainsSampleBlock(const NamesAndTypes & schema, const Block & sample_block)
{
    std::unordered_map<String, DataTypePtr> schema_map;
    for (const auto & column : schema)
        schema_map[column.name] = column.type;
    for (const auto & sample_block_column : sample_block)
    {
        auto it = schema_map.find(sample_block_column.name);
        if (it == schema_map.end())
            throw TiFlashException(
                fmt::format("schema don't contain sample block column: {}", sample_block_column.name),
                Errors::Coprocessor::Internal);

        const auto & type_in_schema = it->second->getName();
        const auto & type_in_sample_block = sample_block_column.type->getName();
        if (type_in_sample_block != type_in_schema)
            throw TiFlashException(
                fmt::format(
                    "the type of column `{}` in schema `{}` is difference from the one in sample block `{}`",
                    sample_block_column.name,
                    type_in_schema,
                    type_in_sample_block),
                Errors::Coprocessor::Internal);
    }
}
} // namespace DB::FinalizeHelper