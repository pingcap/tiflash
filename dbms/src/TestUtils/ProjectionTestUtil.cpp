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

#include <Debug/MockExecutor/AstToPB.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnsToTiPBExpr.h>
#include <TestUtils/ProjectionTestUtil.h>


namespace DB
{
namespace tests
{

ExpressionActionsPtr buildChangeNullable(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const ColumnNumbers & column_nullable_numbers)
{
    NamesAndTypes source_columns;
    for (const auto & column : columns)
    {
        source_columns.emplace_back(column.name, column.type);
    }
    DAGExpressionAnalyzer analyzer(source_columns, context);
    ExpressionActionsChain chain;
    auto & last_step = analyzer.initAndGetLastStep(chain);
    for (auto column_ref_number : column_nullable_numbers)
    {
        last_step.actions->add(ExpressionAction::convertToNullable(columns[column_ref_number].name));
    }
    return last_step.actions;
}

std::pair<ExpressionActionsPtr, std::vector<String>> buildProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const ColumnNumbers & column_literal_numbers,
    const std::vector<Field> & val_fields,
    const ColumnNumbers & column_ref_numbers)
{
    NamesAndTypes source_columns;
    std::vector<tipb::Expr> tipb_exprs;
    for (const auto & column : columns)
    {
        source_columns.emplace_back(column.name, column.type);
    }
    for (size_t i = 0; i < column_literal_numbers.size(); i++)
    {
        tipb::Expr literal_expr;
        const auto & val_field = val_fields[i];
        auto ci = reverseGetColumnInfo(
            {columns[column_literal_numbers[i]].name, columns[column_literal_numbers[i]].type},
            0,
            Field(),
            true);
        literalFieldToTiPBExpr(ci, val_field, &literal_expr, 0);
        tipb_exprs.push_back(literal_expr);
    }
    for (auto column_ref_number : column_ref_numbers)
    {
        auto column_ref_expr = columnToTiPBExpr(columns[column_ref_number], column_ref_number);
        tipb_exprs.push_back(column_ref_expr);
    }
    DAGExpressionAnalyzer analyzer(source_columns, context);
    ExpressionActionsChain chain;
    auto & last_step = analyzer.initAndGetLastStep(chain);
    std::vector<String> result_names;
    for (auto const & common_expr : tipb_exprs)
    {
        auto result_name = analyzer.getActions(common_expr, last_step.actions);
        last_step.required_output.push_back(result_name);
        result_names.push_back(result_name);
    }
    chain.finalize();
    return std::make_pair(last_step.actions, result_names);
}

std::pair<ExpressionActionsPtr, std::vector<String>> buildLiteralProjection(
    Context & context,
    const ColumnsWithTypeAndName & columns,
    const std::vector<Field> & val_fields)
{
    NamesAndTypes source_columns;
    // this function will use reversed column_info's field-type which inferred from bare column to fill the result.
    std::vector<tipb::Expr> tipb_exprs;
    for (size_t i = 0; i < columns.size(); i++)
    {
        auto column = columns[i];
        const auto & val_field = val_fields[i];
        source_columns.emplace_back(column.name, column.type);
        tipb::Expr literal_expr;
        auto ci = reverseGetColumnInfo({column.name, column.type}, 0, Field(), true);
        literalFieldToTiPBExpr(ci, val_field, &literal_expr, 0);
        tipb_exprs.push_back(literal_expr);
    }
    DAGExpressionAnalyzer analyzer(source_columns, context);
    ExpressionActionsChain chain;
    auto & last_step = analyzer.initAndGetLastStep(chain);
    std::vector<String> result_names;
    for (auto const & literal_expr : tipb_exprs)
    {
        auto result_name = analyzer.getActions(literal_expr, last_step.actions);
        last_step.required_output.push_back(result_name);
        result_names.push_back(result_name);
    }
    chain.finalize();
    return std::make_pair(last_step.actions, result_names);
}

ColumnsWithTypeAndName toColumnsWithUniqueNames(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName columns_with_distinct_name = columns;
    std::string base_name = "col";
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns_with_distinct_name[i].name = fmt::format("{}_{}", base_name, i);
    }
    return columns_with_distinct_name;
}

ColumnsWithTypeAndName executeLiteralProjection(
    Context & context,
    const std::vector<String> literals,
    const ColumnsWithTypeAndName & columns)
{
    auto columns_with_unique_name = toColumnsWithUniqueNames(columns);
    std::vector<Field> vals;
    for (auto const & lit : literals)
    {
        Field val_field;
        if (lit == "null")
        {
            vals.push_back(Field(Null()));
        }
        else
        {
            vals.push_back(Field(static_cast<UInt64>(std::stoi(lit))));
        }
    }
    auto [actions, result_names] = buildLiteralProjection(context, columns_with_unique_name, vals);
    Block block(columns_with_unique_name);
    actions->execute(block);
    ColumnsWithTypeAndName res;
    for (auto const & result_name : result_names)
        res.push_back(block.getByName(result_name));
    return res;
}

ProjectionTest::ProjectionTest()
{
    context = TiFlashTestEnv::getContext();
}

void ProjectionTest::initializeDAGContext()
{
    dag_context_ptr = std::make_unique<DAGContext>(1024);
    context->setDAGContext(dag_context_ptr.get());
}

ColumnsWithTypeAndName ProjectionTest::executeProjection(
    const std::vector<String> literals,
    const ColumnsWithTypeAndName & columns)
{
    return DB::tests::executeLiteralProjection(*context, literals, columns);
}

} // namespace tests
} // namespace DB
