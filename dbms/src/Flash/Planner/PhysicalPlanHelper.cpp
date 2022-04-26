#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>

#include <unordered_set>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema)
{
    Names names(schema.size());
    for (size_t i = 0; i < schema.size(); ++i)
        names[i] = schema[i].name;
    return names;
}

ExpressionActionsPtr newActions(const Block & input_block, const Context & context)
{
    const ColumnsWithTypeAndName & actions_input_columns = input_block.getColumnsWithTypeAndName();
    return std::make_shared<ExpressionActions>(actions_input_columns, context.getSettingsRef());
}

ExpressionActionsPtr newActions(const NamesAndTypes & input_columns, const Context & context)
{
    NamesAndTypesList actions_input_column;
    std::unordered_set<String> column_name_set;
    for (const auto & col : input_columns)
    {
        if (column_name_set.find(col.name) == column_name_set.end())
        {
            actions_input_column.emplace_back(col.name, col.type);
            column_name_set.emplace(col.name);
        }
    }
    return std::make_shared<ExpressionActions>(actions_input_column, context.getSettingsRef());
}

Block constructBlockFromSchema(const NamesAndTypes & schema)
{
    ColumnsWithTypeAndName columns;
    for (const auto & column : schema)
        columns.emplace_back(column.type, column.name);
    return Block(columns);
}
} // namespace DB::PhysicalPlanHelper
