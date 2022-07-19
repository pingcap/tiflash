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

#include <Core/ColumnsWithTypeAndName.h>
#include <Debug/astToExecutor.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <tipb/executor.pb.h>

namespace DB::tests
{
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockColumnInfoVec = std::vector<MockColumnInfo>;
using MockTableName = std::pair<String, String>;
using MockOrderByItem = std::pair<String, bool>;
using MockOrderByItemVec = std::vector<MockOrderByItem>;
using MockPartitionByItem = std::pair<String, bool>;
using MockPartitionByItemVec = std::vector<MockPartitionByItem>;
using MockColumnNameVec = std::vector<String>;
using MockAstVec = std::vector<ASTPtr>;
using MockWindowFrame = mock::MockWindowFrame;

class MockDAGRequestContext;

/** Responsible for Hand write tipb::DAGRequest
  * Use this class to mock DAGRequest, then feed the DAGRequest into 
  * the Interpreter for test purpose.
  * The mockTable() method must called first in order to generate the table schema.
  * After construct all necessary operators in DAGRequest, call build() to generate DAGRequest。
  */
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
    {
    }

    ExecutorPtr getRoot()
    {
        return root;
    }

    std::shared_ptr<tipb::DAGRequest> build(MockDAGRequestContext & mock_context);
    QueryTasks buildMPPTasks(MockDAGRequestContext & mock_context);

    DAGRequestBuilder & mockTable(const String & db, const String & table, const MockColumnInfoVec & columns);
    DAGRequestBuilder & mockTable(const MockTableName & name, const MockColumnInfoVec & columns);

    DAGRequestBuilder & exchangeReceiver(const MockColumnInfoVec & columns, uint64_t fine_grained_shuffle_stream_count = 0);

    DAGRequestBuilder & filter(ASTPtr filter_expr);

    DAGRequestBuilder & limit(int limit);
    DAGRequestBuilder & limit(ASTPtr limit_expr);

    DAGRequestBuilder & topN(ASTPtr order_exprs, ASTPtr limit_expr);
    DAGRequestBuilder & topN(const String & col_name, bool desc, int limit);
    DAGRequestBuilder & topN(MockOrderByItemVec order_by_items, int limit);
    DAGRequestBuilder & topN(MockOrderByItemVec order_by_items, ASTPtr limit_expr);

    DAGRequestBuilder & project(MockAstVec exprs);
    DAGRequestBuilder & project(MockColumnNameVec col_names);

    DAGRequestBuilder & exchangeSender(tipb::ExchangeType exchange_type);

    // Currently only support inner join, left join and right join.
    // TODO support more types of join.
    DAGRequestBuilder & join(const DAGRequestBuilder & right, MockAstVec exprs);
    DAGRequestBuilder & join(const DAGRequestBuilder & right, MockAstVec exprs, ASTTableJoin::Kind kind);

    // aggregation
    DAGRequestBuilder & aggregation(ASTPtr agg_func, ASTPtr group_by_expr);
    DAGRequestBuilder & aggregation(MockAstVec agg_funcs, MockAstVec group_by_exprs);

    // window
    DAGRequestBuilder & window(ASTPtr window_func, MockOrderByItem order_by, MockPartitionByItem partition_by, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count = 0);
    DAGRequestBuilder & window(MockAstVec window_funcs, MockOrderByItemVec order_by_vec, MockPartitionByItemVec partition_by_vec, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count = 0);
    DAGRequestBuilder & window(ASTPtr window_func, MockOrderByItemVec order_by_vec, MockPartitionByItemVec partition_by_vec, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count = 0);
    DAGRequestBuilder & sort(MockOrderByItem order_by, bool is_partial_sort, uint64_t fine_grained_shuffle_stream_count = 0);
    DAGRequestBuilder & sort(MockOrderByItemVec order_by_vec, bool is_partial_sort, uint64_t fine_grained_shuffle_stream_count = 0);

private:
    void initDAGRequest(tipb::DAGRequest & dag_request);
    DAGRequestBuilder & buildAggregation(ASTPtr agg_funcs, ASTPtr group_by_exprs);
    DAGRequestBuilder & buildExchangeReceiver(const MockColumnInfoVec & columns, uint64_t fine_grained_shuffle_stream_count = 0);

    ExecutorPtr root;
    DAGProperties properties;
};

/** Responsible for storing necessary arguments in order to Mock DAGRequest
  * index: used in DAGRequestBuilder to identify executors
  * mock_tables: DAGRequestBuilder uses it to mock TableScan executors
  */
class MockDAGRequestContext
{
public:
    explicit MockDAGRequestContext(Context context_)
        : context(context_)
    {
        index = 0;
    }

    DAGRequestBuilder createDAGRequestBuilder()
    {
        return DAGRequestBuilder(index);
    }

    void addMockTable(const String & db, const String & table, const MockColumnInfoVec & columnInfos);
    void addMockTable(const MockTableName & name, const MockColumnInfoVec & columnInfos);
    void addExchangeRelationSchema(String name, const MockColumnInfoVec & columnInfos);
    void addMockTableColumnData(const String & db, const String & table, ColumnsWithTypeAndName columns);
    void addMockTable(const String & db, const String & table, const MockColumnInfoVec & columnInfos, ColumnsWithTypeAndName columns);
    void addMockTable(const MockTableName & name, const MockColumnInfoVec & columnInfos, ColumnsWithTypeAndName columns);
    void addMockTableColumnData(const MockTableName & name, ColumnsWithTypeAndName columns);
    void addExchangeReceiverColumnData(const String & name, ColumnsWithTypeAndName columns);
    void addExchangeReceiver(const String & name, MockColumnInfoVec columnInfos, ColumnsWithTypeAndName columns);

    std::unordered_map<String, ColumnsWithTypeAndName> & executorIdColumnsMap() { return executor_id_columns_map; }

    DAGRequestBuilder scan(String db_name, String table_name);
    DAGRequestBuilder receive(String exchange_name, uint64_t fine_grained_shuffle_stream_count = 0);

private:
    size_t index;
    std::unordered_map<String, MockColumnInfoVec> mock_tables;
    std::unordered_map<String, MockColumnInfoVec> exchange_schemas;
    std::unordered_map<String, ColumnsWithTypeAndName> mock_table_columns;
    std::unordered_map<String, ColumnsWithTypeAndName> mock_exchange_columns;
    std::unordered_map<String, ColumnsWithTypeAndName> executor_id_columns_map; /// <executor_id, columns>

public:
    // Currently don't support task_id, so the following to structure is useless,
    // but we need it to contruct the TaskMeta.
    // In TiFlash, we use task_id to identify an Mpp Task.
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    Context context;
};

ASTPtr buildColumn(const String & column_name);
ASTPtr buildLiteral(const Field & field);
ASTPtr buildFunction(MockAstVec exprs, const String & name);
ASTPtr buildOrderByItemVec(MockOrderByItemVec order_by_items);

MockWindowFrame buildDefaultRowsFrame();

#define col(name) buildColumn((name))
#define lit(field) buildLiteral((field))
#define concat(expr1, expr2) makeASTFunction("concat", (expr1), (expr2))
#define eq(expr1, expr2) makeASTFunction("equals", (expr1), (expr2))
#define Not_eq(expr1, expr2) makeASTFunction("notEquals", (expr1), (expr2))
#define lt(expr1, expr2) makeASTFunction("less", (expr1), (expr2))
#define gt(expr1, expr2) makeASTFunction("greater", (expr1), (expr2))
#define And(expr1, expr2) makeASTFunction("and", (expr1), (expr2))
#define Or(expr1, expr2) makeASTFunction("or", (expr1), (expr2))
#define NOT(expr) makeASTFunction("not", (expr))

// Aggregation functions
#define Max(expr) makeASTFunction("max", (expr))
#define Min(expr) makeASTFunction("min", (expr))
#define Count(expr) makeASTFunction("count", (expr))
#define Sum(expr) makeASTFunction("sum", (expr))

/// Window functions
#define RowNumber() makeASTFunction("RowNumber")
#define Rank() makeASTFunction("Rank")
#define DenseRank() makeASTFunction("DenseRank")

} // namespace DB::tests
