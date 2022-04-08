#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>

#include <unordered_set>

namespace DB::PhysicalPlanHelper
{
Names schemaToNames(const NamesAndTypes & schema)
{
    Names names;
    names.reserve(schema.size());
    for (const auto & column : schema)
        names.push_back(column.name);
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

void executeSchemaProjectAction(const Context & context, DAGPipeline & pipeline, const NamesAndTypes & schema)
{
    const auto & logger = context.getDAGContext()->log;
    assert(pipeline.hasMoreThanOneStream());
    const auto & header = pipeline.firstStream()->getHeader();
    ExpressionActionsPtr project_actions = PhysicalPlanHelper::newActions(header, context);
    const auto & header_names = header.getNames();
    assert(header_names.size() == schema.size());
    FinalizeHelper::checkSchemaContainsSampleBlock(schema, header);
    NamesWithAliases project_aliases;
    for (size_t i = 0; i < header_names.size(); ++i)
        project_aliases.emplace_back(header_names[i], schema[i].name);
    project_actions->add(ExpressionAction::project(project_aliases));
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project_actions, logger->identifier()); });
}
} // namespace DB::PhysicalPlanHelper