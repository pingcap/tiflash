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

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Planner/PhysicalPlanHelper.h>

namespace DB::PhysicalPlanHelper
{
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

NamesAndTypes addProjectAction(
    const ExpressionActionsPtr & expr_actions,
    const NamesAndTypes & before_schema,
    const String & column_prefix,
    const Context & context)
{
    assert(expr_actions);
    assert(!before_schema.empty());
    DAGExpressionAnalyzer analyzer{before_schema, context};
    const auto & project_aliases = analyzer.genNonRootFinalProjectAliases(column_prefix);
    expr_actions->add(ExpressionAction::project(project_aliases));

    NamesAndTypes after_schema = before_schema;
    assert(project_aliases.size() == after_schema.size());
    // replace column name of after_schema by alias.
    for (size_t i = 0; i < project_aliases.size(); ++i)
    {
        assert(after_schema[i].name == project_aliases[i].first);
        after_schema[i].name = project_aliases[i].second;
    }
    return after_schema;
}
} // namespace DB::PhysicalPlanHelper
