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

#include <Debug/astToExecutor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>

#include <initializer_list>

namespace DB
{
namespace tests
{
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockColumnInfos = std::vector<MockColumnInfo>;
using MockColumnInfoList = std::initializer_list<MockColumnInfo>;
using MockTableName = std::pair<String, String>;
using MockOrderByItem = std::pair<String, bool>;
using MockOrderByItems = std::initializer_list<MockOrderByItem>;
using MockColumnNames = std::initializer_list<String>;
using MockAsts = std::initializer_list<ASTPtr>;

class DAGRequestBuilder
{
public:
    size_t & executor_index;

    size_t & getExecutorIndex() const
    {
        return executor_index;
    }

    explicit DAGRequestBuilder(size_t & index)
        : executor_index(index)
    {}

    std::shared_ptr<tipb::DAGRequest> build(Context & context);

    // ywq todo check arguments
    DAGRequestBuilder & mockTable(const String & db, const String & table, const MockColumnInfos & columns);
    DAGRequestBuilder & mockTable(const MockTableName & name, const MockColumnInfos & columns);
    DAGRequestBuilder & mockTable(const MockTableName & name, const MockColumnInfoList & columns);

    DAGRequestBuilder & filter(ASTPtr filter_expr);

    DAGRequestBuilder & limit(int limit);
    DAGRequestBuilder & limit(ASTPtr limit_expr);

    DAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);
    DAGRequestBuilder & topN(const String & col_name, bool desc, int limit);
    DAGRequestBuilder & topN(MockOrderByItems order_by_items, int limit);
    DAGRequestBuilder & topN(MockOrderByItems order_by_items, ASTPtr limit_expr);

    DAGRequestBuilder & project(const String & col_name);
    DAGRequestBuilder & project(MockAsts expr);
    DAGRequestBuilder & project(MockColumnNames col_names);

    // Currentlt only support inner join, left join and right join.
    // TODO support more types of join.
    DAGRequestBuilder & join(const DAGRequestBuilder & right, ASTPtr using_expr_list);
    DAGRequestBuilder & join(const DAGRequestBuilder & right, ASTPtr using_expr_list, ASTTableJoin::Kind kind);

    // aggregation
    DAGRequestBuilder & aggregation(ASTPtr agg_func, ASTPtr group_by_expr);
    DAGRequestBuilder & aggregation(MockAsts agg_funcs, MockAsts group_by_exprs);

private:
    void initDAGRequest(tipb::DAGRequest & dag_request);
    DAGRequestBuilder & buildAggregation(ASTPtr agg_funcs, ASTPtr group_by_exprs);

    ExecutorPtr root;
    DAGProperties properties;
};

class DAGRequestBuilderFactory
{
public:
    size_t index;
    DAGRequestBuilderFactory()
    {
        index = 0;
    }
    DAGRequestBuilder createDAGRequestBuilder()
    {
        return DAGRequestBuilder(index);
    }
};

ASTPtr buildColumn(const String & column_name);
ASTPtr buildLiteral(const Field & field);
ASTPtr buildFunction(MockAsts exprs, const String & name);
ASTPtr buildOrderByItemList(MockOrderByItems order_by_items);


#define col(name) buildColumn((name))
#define lit(field) buildLiteral((field))
#define eq(expr1, expr2) makeASTFunction("equals", (expr1), (expr2))
#define Not_eq(expr1, expr2) makeASTFunction("notEquals", (expr1), (expr2))
#define lt(expr1, expr2) makeASTFunction("less", (expr1), (expr2))
#define gt(expr1, expr2) makeASTFunction("greater", (expr1), (expr2))
#define And(expr1, expr2) makeASTFunction("and", (expr1), (expr2))
#define Or(expr1, expr2) makeASTFunction("or", (expr1), (expr2))
#define NOT(expr) makeASTFunction("not", (expr1), (expr2))
#define Max(expr) makeASTFunction("max", expr)
} // namespace tests
} // namespace DB