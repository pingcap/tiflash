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

#include <cstddef>
#include <initializer_list>
#include <utility>

namespace DB
{
namespace tests
{
// <name, type>
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockColumnInfos = std::vector<MockColumnInfo>;
using MockTableName = std::pair<String, String>;
using MockOrderByItem = std::pair<String, bool>;
using MockOrderByItems = std::initializer_list<MockOrderByItem>;
using MockColumnNames = std::initializer_list<String>;
using MockAsts = std::initializer_list<ASTPtr>;

class DAGRequestBuilder
{
public:
    static size_t executor_index;
    static size_t & getExecutorIndex()
    {
        return executor_index;
    }

    std::shared_ptr<tipb::DAGRequest> build(Context & context);

    DAGRequestBuilder & mockTable(const String & db, const String & table, const MockColumnInfos & columns);
    DAGRequestBuilder & mockTable(const MockTableName & name, const MockColumnInfos & columns);

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

    // Currentlt only support inner join.
    // TODO support more joins
    DAGRequestBuilder & join(const DAGRequestBuilder & right, ASTPtr using_expr_list);

private:
    void initDAGRequest(tipb::DAGRequest & dag_request);

    ExecutorPtr root;
    DAGProperties properties;
};

size_t DAGRequestBuilder::executor_index = 0;

ASTPtr buildColumn(const String & column_name);
ASTPtr buildLiteral(const Field & field);
ASTPtr buildFunction(MockAsts exprs, const String & name);
ASTPtr buildOrderByItemList(MockOrderByItems order_by_items);


#define col(name) buildColumn((name))
#define lit(field) buildLiteral((field))
#define eq(expr1, expr2) buildFunction({(expr1), (expr2)}, "equals")
#define Not_eq(expr1, expr2) buildFunction({(expr1), (expr2)}, "notEquals")
#define lt(expr1, expr2) buildFunction({(expr1), (expr2)}, "less")
#define gt(expr1, expr2) buildFunction({(expr1), (expr2)}, "greater")
#define And(expr1, expr2) buildFunction({(expr1), (expr2)}, "and")
#define Or(expr1, expr2) buildFunction({(expr1), (expr2)}, "or")
#define NOT(expr) buildFunction({expr1}, "not")
} // namespace tests
} // namespace DB