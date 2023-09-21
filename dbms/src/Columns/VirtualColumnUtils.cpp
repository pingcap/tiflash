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

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>


namespace DB::VirtualColumnUtils
{

/// Verifying that the function depends only on the specified columns
static bool isValidFunction(const ASTPtr & expression, const NameSet & columns)
{
    for (size_t i = 0; i < expression->children.size(); ++i) // NOLINT
        if (!isValidFunction(expression->children[i], columns))
            return false;

    if (const auto * identifier = typeid_cast<const ASTIdentifier *>(&*expression))
    {
        if (identifier->kind == ASTIdentifier::Kind::Column)
            return columns.count(identifier->name);
    }
    return true;
}

/// Extract all subfunctions of the main conjunction, but depending only on the specified columns
static void extractFunctions(const ASTPtr & expression, const NameSet & columns, std::vector<ASTPtr> & result)
{
    const auto * function = typeid_cast<const ASTFunction *>(expression.get());
    if (function && function->name == "and")
    {
        for (size_t i = 0; i < function->arguments->children.size(); ++i) // NOLINT
            extractFunctions(function->arguments->children[i], columns, result);
    }
    else if (isValidFunction(expression, columns))
    {
        result.push_back(expression->clone());
    }
}

/// Construct a conjunction from given functions
static ASTPtr buildWhereExpression(const ASTs & functions)
{
    if (functions.empty())
        return nullptr;
    if (functions.size() == 1)
        return functions[0];
    ASTPtr new_query = std::make_shared<ASTFunction>();
    ASTFunction & new_function = typeid_cast<ASTFunction &>(*new_query);
    new_function.name = "and";
    new_function.arguments = std::make_shared<ASTExpressionList>();
    new_function.arguments->children = functions;
    new_function.children.push_back(new_function.arguments);
    return new_query;
}

void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context)
{
    const auto & select = typeid_cast<const ASTSelectQuery &>(*query);
    if (!select.where_expression && !select.prewhere_expression)
        return;

    NameSet columns;
    for (const auto & it : block.getNamesAndTypesList())
        columns.insert(it.name);

    /// We will create an expression that evaluates the expressions in WHERE and PREWHERE, depending only on the existing columns.
    std::vector<ASTPtr> functions;
    if (select.where_expression)
        extractFunctions(select.where_expression, columns, functions);
    if (select.prewhere_expression)
        extractFunctions(select.prewhere_expression, columns, functions);

    ASTPtr expression_ast = buildWhereExpression(functions);
    if (!expression_ast)
        return;

    /// Let's analyze and calculate the expression.
    ExpressionAnalyzer analyzer(expression_ast, context, {}, block.getNamesAndTypesList());
    ExpressionActionsPtr actions = analyzer.getActions(false);

    Block block_with_filter = block;
    actions->execute(block_with_filter);

    /// Filter the block.
    String filter_column_name = expression_ast->getColumnName();
    ColumnPtr filter_column = block_with_filter.getByName(filter_column_name).column;
    if (ColumnPtr converted = filter_column->convertToFullColumnIfConst())
        filter_column = converted;
    const IColumn::Filter & filter = typeid_cast<const ColumnUInt8 &>(*filter_column).getData();

    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnPtr & column = block.safeGetByPosition(i).column;
        column = column->filter(filter, -1);
    }
}

} // namespace DB::VirtualColumnUtils
