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

#include <Debug/MockComputeServerManager.h>
#include <Debug/dbgQueryExecutor.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Storages/Transaction/TiDB.h>
#include <TestUtils/TiFlashTestException.h>
#include <TestUtils/mockExecutor.h>
#include <tipb/executor.pb.h>

#include <unordered_set>

namespace DB::tests
{
using TableInfo = TiDB::TableInfo;

ASTPtr buildColumn(const String & column_name)
{
    return std::make_shared<ASTIdentifier>(column_name);
}

ASTPtr buildLiteral(const Field & field)
{
    return std::make_shared<ASTLiteral>(field);
}

ASTPtr buildOrderByItemVec(MockOrderByItemVec order_by_items)
{
    std::vector<ASTPtr> vec(order_by_items.size());
    size_t i = 0;
    for (auto item : order_by_items)
    {
        int direction = item.second ? -1 : 1;
        ASTPtr locale_node;
        auto order_by_item = std::make_shared<ASTOrderByElement>(direction, direction, false, locale_node);
        order_by_item->children.push_back(std::make_shared<ASTIdentifier>(item.first));
        vec[i++] = order_by_item;
    }
    auto exp_list = std::make_shared<ASTExpressionList>();
    exp_list->children.insert(exp_list->children.end(), vec.begin(), vec.end());
    return exp_list;
}

MockWindowFrame buildDefaultRowsFrame()
{
    MockWindowFrame frame;
    frame.type = tipb::WindowFrameType::Rows;
    frame.end = {tipb::WindowBoundType::CurrentRow, false, 0};
    frame.start = {tipb::WindowBoundType::CurrentRow, false, 0};
    return frame;
}

// a mock DAGRequest should prepare its time_zone, flags, encode_type and output_schema.
void DAGRequestBuilder::initDAGRequest(tipb::DAGRequest & dag_request)
{
    dag_request.set_time_zone_name(properties.tz_name);
    dag_request.set_time_zone_offset(properties.tz_offset);
    dag_request.set_flags(dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));

    if (properties.encode_type == "chunk")
        dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
    else if (properties.encode_type == "chblock")
        dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
    else
        dag_request.set_encode_type(tipb::EncodeType::TypeDefault);

    for (size_t i = 0; i < root->output_schema.size(); ++i)
        dag_request.add_output_offsets(i);
}

// traval the AST tree to build tipb::Executor recursively.
std::shared_ptr<tipb::DAGRequest> DAGRequestBuilder::build(MockDAGRequestContext & mock_context, DAGRequestType type)
{
    // build tree struct base executor
    MPPInfo mpp_info(properties.start_ts, -1, -1, {}, mock_context.receiver_source_task_ids_map);
    std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
    tipb::DAGRequest & dag_request = *dag_request_ptr;
    initDAGRequest(dag_request);
    root->toTiPBExecutor(dag_request.mutable_root_executor(), properties.collator, mpp_info, mock_context.context);
    root.reset();
    executor_index = 0;

    // convert to list struct base executor
    if (type == DAGRequestType::list)
    {
        auto & mutable_executors = *dag_request_ptr->mutable_executors();
        traverseExecutorsReverse(dag_request_ptr.get(), [&](const tipb::Executor & executor) -> bool {
            auto * mutable_executor = mutable_executors.Add();
            (*mutable_executor) = executor;
            mutable_executor->clear_executor_id();
            return true;
        });
        auto * root_executor = dag_request_ptr->release_root_executor();
        delete root_executor;
    }

    return dag_request_ptr;
}

// Currently Sort and Window Executors don't support columnPrune.
// TODO: support columnPrume for Sort and Window.
void columnPrune(mock::ExecutorBinderPtr executor)
{
    std::unordered_set<String> used_columns;
    for (auto & schema : executor->output_schema)
        used_columns.emplace(schema.first);
    executor->columnPrune(used_columns);
}


// Split a DAGRequest into multiple QueryTasks which can be dispatched to multiple Compute nodes.
// Currently we don't support window functions
QueryTasks DAGRequestBuilder::buildMPPTasks(MockDAGRequestContext & mock_context, const DAGProperties & properties)
{
    columnPrune(root);
    mock_context.context.setMPPTest();
    auto query_tasks = queryPlanToQueryTasks(properties, root, executor_index, mock_context.context);
    root.reset();
    executor_index = 0;
    return query_tasks;
}

QueryTasks DAGRequestBuilder::buildMPPTasks(MockDAGRequestContext & mock_context)
{
    columnPrune(root);
    DAGProperties properties;
    properties.is_mpp_query = true;
    properties.mpp_partition_num = 1;
    mock_context.context.setMPPTest();
    auto query_tasks = queryPlanToQueryTasks(properties, root, executor_index, mock_context.context);
    root.reset();
    executor_index = 0;
    return query_tasks;
}

DAGRequestBuilder & DAGRequestBuilder::mockTable(const String & db, const String & table, TableInfo & table_info, const MockColumnInfoVec & columns)
{
    assert(!columns.empty());
    root = mock::compileTableScan(getExecutorIndex(), table_info, db, table, false);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::mockTable(const MockTableName & name, TableInfo & table_info, const MockColumnInfoVec & columns)
{
    return mockTable(name.first, name.second, table_info, columns);
}

DAGRequestBuilder & DAGRequestBuilder::exchangeReceiver(const MockColumnInfoVec & columns, uint64_t fine_grained_shuffle_stream_count)
{
    return buildExchangeReceiver(columns, fine_grained_shuffle_stream_count);
}

DAGRequestBuilder & DAGRequestBuilder::buildExchangeReceiver(const MockColumnInfoVec & columns, uint64_t fine_grained_shuffle_stream_count)
{
    DAGSchema schema;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo info;
        info.tp = column.second;
        info.name = column.first;
        schema.push_back({column.first, info});
    }

    root = mock::compileExchangeReceiver(getExecutorIndex(), schema, fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::filter(ASTPtr filter_expr)
{
    assert(root);
    root = mock::compileSelection(root, getExecutorIndex(), filter_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::limit(int limit)
{
    assert(root);
    root = mock::compileLimit(root, getExecutorIndex(), buildLiteral(Field(static_cast<UInt64>(limit))));
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::limit(ASTPtr limit_expr)
{
    assert(root);
    root = mock::compileLimit(root, getExecutorIndex(), limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(ASTPtr order_exprs, ASTPtr limit_expr)
{
    assert(root);
    root = mock::compileTopN(root, getExecutorIndex(), order_exprs, limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(const String & col_name, bool desc, int limit)
{
    assert(root);
    root = mock::compileTopN(root, getExecutorIndex(), buildOrderByItemVec({{col_name, desc}}), buildLiteral(Field(static_cast<UInt64>(limit))));
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::topN(MockOrderByItemVec order_by_items, int limit)
{
    return topN(order_by_items, buildLiteral(Field(static_cast<UInt64>(limit))));
}

DAGRequestBuilder & DAGRequestBuilder::topN(MockOrderByItemVec order_by_items, ASTPtr limit_expr)
{
    assert(root);
    root = mock::compileTopN(root, getExecutorIndex(), buildOrderByItemVec(order_by_items), limit_expr);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::project(MockAstVec exprs)
{
    assert(root);
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & expr : exprs)
    {
        exp_list->children.push_back(expr);
    }
    root = mock::compileProject(root, getExecutorIndex(), exp_list);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::project(MockColumnNameVec col_names)
{
    assert(root);
    auto exp_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : col_names)
    {
        exp_list->children.push_back(col(name));
    }
    root = mock::compileProject(root, getExecutorIndex(), exp_list);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::exchangeSender(tipb::ExchangeType exchange_type)
{
    assert(root);
    root = mock::compileExchangeSender(root, getExecutorIndex(), exchange_type);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::join(const DAGRequestBuilder & right,
                                            tipb::JoinType tp,
                                            MockAstVec join_cols,
                                            MockAstVec left_conds,
                                            MockAstVec right_conds,
                                            MockAstVec other_conds,
                                            MockAstVec other_eq_conds_from_in)
{
    assert(root);
    assert(right.root);

    root = mock::compileJoin(getExecutorIndex(), root, right.root, tp, join_cols, left_conds, right_conds, other_conds, other_eq_conds_from_in);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::aggregation(ASTPtr agg_func, ASTPtr group_by_expr)
{
    auto agg_funcs = std::make_shared<ASTExpressionList>();
    auto group_by_exprs = std::make_shared<ASTExpressionList>();
    if (agg_func)
        agg_funcs->children.push_back(agg_func);
    if (group_by_expr)
        group_by_exprs->children.push_back(group_by_expr);
    return buildAggregation(agg_funcs, group_by_exprs);
}

DAGRequestBuilder & DAGRequestBuilder::aggregation(MockAstVec agg_funcs, MockAstVec group_by_exprs)
{
    auto agg_func_list = std::make_shared<ASTExpressionList>();
    auto group_by_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & func : agg_funcs)
        agg_func_list->children.push_back(func);
    for (const auto & group_by : group_by_exprs)
        group_by_expr_list->children.push_back(group_by);
    return buildAggregation(agg_func_list, group_by_expr_list);
}

DAGRequestBuilder & DAGRequestBuilder::buildAggregation(ASTPtr agg_funcs, ASTPtr group_by_exprs)
{
    assert(root);
    root = compileAggregation(root, getExecutorIndex(), agg_funcs, group_by_exprs);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(ASTPtr window_func, MockOrderByItem order_by, MockPartitionByItem partition_by, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    window_func_list->children.push_back(window_func);
    root = compileWindow(root, getExecutorIndex(), window_func_list, buildOrderByItemVec({partition_by}), buildOrderByItemVec({order_by}), frame, fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(ASTPtr window_func, MockOrderByItemVec order_by_vec, MockPartitionByItemVec partition_by_vec, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    window_func_list->children.push_back(window_func);
    root = compileWindow(root, getExecutorIndex(), window_func_list, buildOrderByItemVec(partition_by_vec), buildOrderByItemVec(order_by_vec), frame, fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(MockAstVec window_funcs, MockOrderByItemVec order_by_vec, MockPartitionByItemVec partition_by_vec, MockWindowFrame frame, uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    for (const auto & func : window_funcs)
        window_func_list->children.push_back(func);
    root = compileWindow(root, getExecutorIndex(), window_func_list, buildOrderByItemVec(partition_by_vec), buildOrderByItemVec(order_by_vec), frame, fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::sort(MockOrderByItem order_by, bool is_partial_sort, uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    root = compileSort(root, getExecutorIndex(), buildOrderByItemVec({order_by}), is_partial_sort, fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::sort(MockOrderByItemVec order_by_vec, bool is_partial_sort, uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    root = compileSort(root, getExecutorIndex(), buildOrderByItemVec(order_by_vec), is_partial_sort, fine_grained_shuffle_stream_count);
    return *this;
}

void MockDAGRequestContext::addMockTable(const String & db, const String & table, const MockColumnInfoVec & columnInfos)
{
    mock_storage.addTableSchema(db + "." + table, columnInfos);
}

void MockDAGRequestContext::addMockTable(const MockTableName & name, const MockColumnInfoVec & columnInfos)
{
    mock_storage.addTableSchema(name.first + "." + name.second, columnInfos);
}

void MockDAGRequestContext::addExchangeRelationSchema(String name, const MockColumnInfoVec & columnInfos)
{
    mock_storage.addExchangeSchema(name, columnInfos);
}

void MockDAGRequestContext::addMockTableColumnData(const String & db, const String & table, ColumnsWithTypeAndName columns)
{
    mock_storage.addTableData(db + "." + table, columns);
}

void MockDAGRequestContext::addMockTableColumnData(const MockTableName & name, ColumnsWithTypeAndName columns)
{
    mock_storage.addTableData(name.first + "." + name.second, columns);
}

void MockDAGRequestContext::addExchangeReceiverColumnData(const String & name, ColumnsWithTypeAndName columns)
{
    mock_storage.addExchangeData(name, columns);
}

void MockDAGRequestContext::addMockTable(const String & db, const String & table, const MockColumnInfoVec & columnInfos, ColumnsWithTypeAndName columns)
{
    addMockTable(db, table, columnInfos);
    addMockTableColumnData(db, table, columns);
}

void MockDAGRequestContext::addMockTable(const MockTableName & name, const MockColumnInfoVec & columnInfos, ColumnsWithTypeAndName columns)
{
    addMockTable(name, columnInfos);
    addMockTableColumnData(name, columns);
}

void MockDAGRequestContext::addExchangeReceiver(const String & name, MockColumnInfoVec columnInfos, ColumnsWithTypeAndName columns)
{
    addExchangeRelationSchema(name, columnInfos);
    addExchangeReceiverColumnData(name, columns);
}

DAGRequestBuilder MockDAGRequestContext::scan(const String & db_name, const String & table_name)
{
    auto table_info = mock_storage.getTableInfo(db_name + "." + table_name);
    return DAGRequestBuilder(index, collation).mockTable({db_name, table_name}, table_info, mock_storage.getTableSchema(db_name + "." + table_name));
}

DAGRequestBuilder MockDAGRequestContext::receive(const String & exchange_name, uint64_t fine_grained_shuffle_stream_count)
{
    auto builder = DAGRequestBuilder(index, collation).exchangeReceiver(mock_storage.getExchangeSchema(exchange_name), fine_grained_shuffle_stream_count);
    receiver_source_task_ids_map[builder.getRoot()->name] = {};
    mock_storage.addExchangeRelation(builder.getRoot()->name, exchange_name);
    return builder;
}
} // namespace DB::tests
