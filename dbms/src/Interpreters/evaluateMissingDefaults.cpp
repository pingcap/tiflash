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

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <Storages/ColumnDefault.h>

#include <utility>


namespace DB
{
void evaluateMissingDefaults(
    Block & block,
    const NamesAndTypesList & required_columns,
    const ColumnDefaults & column_defaults,
    const Context & context)
{
    if (column_defaults.empty())
        return;

    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();

    for (const auto & column : required_columns)
    {
        if (block.has(column.name))
            continue;

        const auto it = column_defaults.find(column.name);

        /// expressions must be cloned to prevent modification by the ExpressionAnalyzer
        if (it != column_defaults.end())
            default_expr_list->children.emplace_back(setAlias(it->second.expression->clone(), it->first));
    }

    /// nothing to evaluate
    if (default_expr_list->children.empty())
        return;

    /** ExpressionAnalyzer eliminates "unused" columns, in order to ensure their safety
      * we are going to operate on a copy instead of the original block */
    Block copy_block{block};
    /// evaluate default values for defaulted columns

    NamesAndTypesList available_columns;
    for (size_t i = 0, size = block.columns(); i < size; ++i)
        available_columns.emplace_back(block.getByPosition(i).name, block.getByPosition(i).type);

    ExpressionAnalyzer{default_expr_list, context, {}, available_columns}.getActions(true)->execute(copy_block);

    /// move evaluated columns to the original block, materializing them at the same time
    for (auto & column_name_type : copy_block)
    {
        if (ColumnPtr converted = column_name_type.column->convertToFullColumnIfConst())
            column_name_type.column = converted;

        block.insert(std::move(column_name_type));
    }
}

} // namespace DB
