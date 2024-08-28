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
#include <Debug/MockExecutor/AggregationBinder.h>
#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/ExpandBinder.h>
#include <Debug/MockExecutor/ExpandBinder2.h>
#include <Debug/MockExecutor/JoinBinder.h>
#include <Debug/MockExecutor/LimitBinder.h>
#include <Debug/MockExecutor/ProjectBinder.h>
#include <Debug/MockExecutor/SelectionBinder.h>
#include <Debug/MockExecutor/SortBinder.h>
#include <Debug/MockExecutor/TableScanBinder.h>
#include <Debug/MockExecutor/TopNBinder.h>
#include <Debug/dbgQueryExecutor.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <TestUtils/TiFlashTestException.h>
#include <TestUtils/mockExecutor.h>
#include <TiDB/Schema/TiDB.h>
#include <tipb/executor.pb.h>

#include <memory>
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
    MockAstVec vec(order_by_items.size());
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
    dag_request.set_flags(
        dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));

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
    MPPInfo mpp_info(
        properties.start_ts,
        properties.gather_id,
        properties.query_ts,
        properties.server_id,
        properties.local_query_id,
        -1,
        -1,
        {},
        mock_context.receiver_source_task_ids_map);
    std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
    tipb::DAGRequest & dag_request = *dag_request_ptr;
    initDAGRequest(dag_request);
    root->toTiPBExecutor(dag_request.mutable_root_executor(), properties.collator, mpp_info, *mock_context.context);
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
    mock_context.context->setMPPTest();
    auto query_tasks = queryPlanToQueryTasks(properties, root, executor_index, *mock_context.context);
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
    mock_context.context->setMPPTest();
    auto query_tasks = queryPlanToQueryTasks(properties, root, executor_index, *mock_context.context);
    root.reset();
    executor_index = 0;
    return query_tasks;
}

DAGRequestBuilder & DAGRequestBuilder::mockTable(
    const String & db,
    const String & table,
    TableInfo & table_info,
    const MockColumnInfoVec & columns [[maybe_unused]],
    bool keep_order)
{
    assert(!columns.empty());
    root = mock::compileTableScan(getExecutorIndex(), table_info, db, table, false, keep_order);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::mockTable(
    const MockTableName & name,
    TableInfo & table_info,
    const MockColumnInfoVec & columns,
    bool keep_order)
{
    return mockTable(name.first, name.second, table_info, columns, keep_order);
}

DAGRequestBuilder & DAGRequestBuilder::exchangeReceiver(
    const String & exchange_name,
    const MockColumnInfoVec & columns,
    uint64_t fine_grained_shuffle_stream_count)
{
    return buildExchangeReceiver(exchange_name, columns, fine_grained_shuffle_stream_count);
}

DAGRequestBuilder & DAGRequestBuilder::buildExchangeReceiver(
    const String & exchange_name,
    const MockColumnInfoVec & columns,
    uint64_t fine_grained_shuffle_stream_count)
{
    DAGSchema schema;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo info;
        info.name = column.name;
        info.tp = column.type;
        schema.push_back({exchange_name + "." + info.name, info});
    }

    root = mock::compileExchangeReceiver(
        getExecutorIndex(),
        schema,
        fine_grained_shuffle_stream_count,
        std::static_pointer_cast<mock::ExchangeSenderBinder>(root));
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
    root = mock::compileTopN(
        root,
        getExecutorIndex(),
        buildOrderByItemVec({{col_name, desc}}),
        buildLiteral(Field(static_cast<UInt64>(limit))));
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

DAGRequestBuilder & DAGRequestBuilder::exchangeSender(
    tipb::ExchangeType exchange_type,
    MockColumnNameVec part_keys,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto partition_key_list = std::make_shared<ASTExpressionList>();
    for (const auto & part_key : part_keys)
    {
        partition_key_list->children.push_back(col(part_key));
    }
    root = mock::compileExchangeSender(
        root,
        getExecutorIndex(),
        exchange_type,
        partition_key_list,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::join(
    const DAGRequestBuilder & right,
    tipb::JoinType tp,
    MockAstVec join_col_exprs,
    MockAstVec left_conds,
    MockAstVec right_conds,
    MockAstVec other_conds,
    MockAstVec other_eq_conds_from_in,
    uint64_t fine_grained_shuffle_stream_count,
    bool is_null_aware_semi_join,
    int64_t inner_index)
{
    assert(root);
    assert(right.root);
    root = mock::compileJoin(
        getExecutorIndex(),
        root,
        right.root,
        tp,
        join_col_exprs,
        left_conds,
        right_conds,
        other_conds,
        other_eq_conds_from_in,
        fine_grained_shuffle_stream_count,
        is_null_aware_semi_join,
        inner_index);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::aggregation(
    ASTPtr agg_func,
    ASTPtr group_by_expr,
    uint64_t fine_grained_shuffle_stream_count,
    std::shared_ptr<AutoPassThroughSwitcher> switcher)
{
    auto agg_funcs = std::make_shared<ASTExpressionList>();
    auto group_by_exprs = std::make_shared<ASTExpressionList>();
    if (agg_func)
        agg_funcs->children.push_back(agg_func);
    if (group_by_expr)
        group_by_exprs->children.push_back(group_by_expr);
    return buildAggregation(agg_funcs, group_by_exprs, fine_grained_shuffle_stream_count, switcher);
}

DAGRequestBuilder & DAGRequestBuilder::aggregation(
    MockAstVec agg_funcs,
    MockAstVec group_by_exprs,
    uint64_t fine_grained_shuffle_stream_count,
    std::shared_ptr<AutoPassThroughSwitcher> switcher)
{
    auto agg_func_list = std::make_shared<ASTExpressionList>();
    auto group_by_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & func : agg_funcs)
        agg_func_list->children.push_back(func);
    for (const auto & group_by : group_by_exprs)
        group_by_expr_list->children.push_back(group_by);
    return buildAggregation(agg_func_list, group_by_expr_list, fine_grained_shuffle_stream_count, switcher);
}

DAGRequestBuilder & DAGRequestBuilder::buildAggregation(
    ASTPtr agg_funcs,
    ASTPtr group_by_exprs,
    uint64_t fine_grained_shuffle_stream_count,
    std::shared_ptr<AutoPassThroughSwitcher> switcher)
{
    assert(root);
    root = compileAggregation(
        root,
        getExecutorIndex(),
        agg_funcs,
        group_by_exprs,
        fine_grained_shuffle_stream_count,
        switcher);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(
    ASTPtr window_func,
    MockOrderByItem order_by,
    MockPartitionByItem partition_by,
    MockWindowFrame frame,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    window_func_list->children.push_back(window_func);
    root = compileWindow(
        root,
        getExecutorIndex(),
        window_func_list,
        buildOrderByItemVec({partition_by}),
        buildOrderByItemVec({order_by}),
        frame,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(
    ASTPtr window_func,
    MockOrderByItemVec order_by_vec,
    MockPartitionByItemVec partition_by_vec,
    MockWindowFrame frame,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    window_func_list->children.push_back(window_func);
    root = compileWindow(
        root,
        getExecutorIndex(),
        window_func_list,
        buildOrderByItemVec(partition_by_vec),
        buildOrderByItemVec(order_by_vec),
        frame,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::window(
    MockAstVec window_funcs,
    MockOrderByItemVec order_by_vec,
    MockPartitionByItemVec partition_by_vec,
    MockWindowFrame frame,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    auto window_func_list = std::make_shared<ASTExpressionList>();
    for (const auto & func : window_funcs)
        window_func_list->children.push_back(func);
    root = compileWindow(
        root,
        getExecutorIndex(),
        window_func_list,
        buildOrderByItemVec(partition_by_vec),
        buildOrderByItemVec(order_by_vec),
        frame,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::sort(
    MockOrderByItem order_by,
    bool is_partial_sort,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    root = compileSort(
        root,
        getExecutorIndex(),
        buildOrderByItemVec({order_by}),
        is_partial_sort,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::sort(
    MockOrderByItemVec order_by_vec,
    bool is_partial_sort,
    uint64_t fine_grained_shuffle_stream_count)
{
    assert(root);
    root = compileSort(
        root,
        getExecutorIndex(),
        buildOrderByItemVec(order_by_vec),
        is_partial_sort,
        fine_grained_shuffle_stream_count);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::expand(MockVVecColumnNameVec grouping_set_columns)
{
    assert(root);
    auto grouping_sets_ast = mock::MockVVecGroupingNameVec();
    auto grouping_col_collection = std::set<String>();
    for (const auto & grouping_set : grouping_set_columns)
    {
        auto grouping_set_ast = mock::MockVecGroupingNameVec();
        for (const auto & grouping_exprs : grouping_set)
        {
            auto grouping_exprs_ast = mock::MockGroupingNameVec();
            for (const auto & grouping_col : grouping_exprs)
            {
                auto ast_col_ptr = buildColumn(grouping_col); // string identifier change to ast column ref
                grouping_exprs_ast.emplace_back(std::move(ast_col_ptr));
                grouping_col_collection.insert(grouping_col);
            }
            grouping_set_ast.emplace_back(std::move(grouping_exprs_ast));
        }
        grouping_sets_ast.emplace_back(std::move(grouping_set_ast));
    }
    root = compileExpand(root, getExecutorIndex(), grouping_sets_ast, grouping_col_collection);
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::appendRuntimeFilter(mock::MockRuntimeFilter & rf)
{
    auto * join = dynamic_cast<mock::JoinBinder *>(root.get());
    if (join)
    {
        join->addRuntimeFilter(rf);
    }
    return *this;
}

DAGRequestBuilder & DAGRequestBuilder::expand2(
    std::vector<MockAstVec> level_projection_expressions,
    std::vector<String> output_names,
    std::vector<tipb::FieldType> fts)
{
    assert(root);
    std::vector<std::shared_ptr<DB::IAST>> expression_list_vec;
    for (const auto & one_level_proj : level_projection_expressions)
    {
        auto exp_list = std::make_shared<ASTExpressionList>();
        for (const auto & proj_expr : one_level_proj)
        {
            exp_list->children.push_back(proj_expr);
        }
        expression_list_vec.push_back(exp_list);
    }
    root = mock::compileExpand2(root, getExecutorIndex(), expression_list_vec, output_names, fts);
    return *this;
}

void MockDAGRequestContext::addMockTable(
    const String & db,
    const String & table,
    const MockColumnInfoVec & columnInfos,
    size_t concurrency_hint)
{
    auto columns
        = getColumnWithTypeAndName(genNamesAndTypes(mockColumnInfosToTiDBColumnInfos(columnInfos), "mock_table_scan"));
    addMockTable(db, table, columnInfos, columns, concurrency_hint);
}

void MockDAGRequestContext::addMockTableSchema(
    const String & db,
    const String & table,
    const MockColumnInfoVec & columnInfos)
{
    mock_storage->addTableSchema(db + "." + table, columnInfos);
}

void MockDAGRequestContext::addMockTableSchema(const MockTableName & name, const MockColumnInfoVec & columnInfos)
{
    mock_storage->addTableSchema(name.first + "." + name.second, columnInfos);
}

void MockDAGRequestContext::addMockTable(
    const MockTableName & name,
    const MockColumnInfoVec & columnInfos,
    size_t concurrency_hint)
{
    auto columns
        = getColumnWithTypeAndName(genNamesAndTypes(mockColumnInfosToTiDBColumnInfos(columnInfos), "mock_table_scan"));
    addMockTable(name, columnInfos, columns, concurrency_hint);
}

void MockDAGRequestContext::addMockTableConcurrencyHint(
    const String & db,
    const String & table,
    size_t concurrency_hint)
{
    mock_storage->addTableScanConcurrencyHint(db + "." + table, concurrency_hint);
}

void MockDAGRequestContext::addMockTableConcurrencyHint(const MockTableName & name, size_t concurrency_hint)
{
    mock_storage->addTableScanConcurrencyHint(name.first + "." + name.second, concurrency_hint);
}

void MockDAGRequestContext::addMockDeltaMergeTableConcurrencyHint(const MockTableName & name, size_t concurrency_hint)
{
    mock_storage->addDeltaMergeTableConcurrencyHint(name.first + "." + name.second, concurrency_hint);
}

void MockDAGRequestContext::addExchangeRelationSchema(String name, const MockColumnInfoVec & columnInfos)
{
    mock_storage->addExchangeSchema(name, columnInfos);
}

void MockDAGRequestContext::addMockTableColumnData(
    const String & db,
    const String & table,
    ColumnsWithTypeAndName columns)
{
    mock_storage->addTableData(db + "." + table, columns);
}

void MockDAGRequestContext::addMockTableColumnData(const MockTableName & name, ColumnsWithTypeAndName columns)
{
    mock_storage->addTableData(name.first + "." + name.second, columns);
}

void MockDAGRequestContext::addMockDeltaMergeData(
    const String & db,
    const String & table,
    ColumnsWithTypeAndName columns)
{
    for (const auto & column : columns)
        RUNTIME_ASSERT(!column.name.empty(), "mock column must have column name");

    mock_storage->addTableDataForDeltaMerge(*context, db + "." + table, columns);
}

void MockDAGRequestContext::addExchangeReceiverColumnData(const String & name, ColumnsWithTypeAndName columns)
{
    mock_storage->addExchangeData(name, columns);
}

void MockDAGRequestContext::addMockTable(
    const String & db,
    const String & table,
    const MockColumnInfoVec & columnInfos,
    ColumnsWithTypeAndName columns,
    size_t concurrency_hint)
{
    assertMockInput(columnInfos, columns);

    addMockTableSchema(db, table, columnInfos);
    addMockTableColumnData(db, table, columns);
    addMockTableConcurrencyHint(db, table, concurrency_hint);
}

void MockDAGRequestContext::addMockTable(
    const MockTableName & name,
    const MockColumnInfoVec & columnInfos,
    ColumnsWithTypeAndName columns,
    size_t concurrency_hint)
{
    assertMockInput(columnInfos, columns);

    addMockTableSchema(name, columnInfos);
    addMockTableColumnData(name, columns);
    addMockTableConcurrencyHint(name, concurrency_hint);
}

void MockDAGRequestContext::addMockDeltaMergeSchema(
    const String & db,
    const String & table,
    const MockColumnInfoVec & columnInfos)
{
    mock_storage->addTableSchemaForDeltaMerge(db + "." + table, columnInfos);
}

void MockDAGRequestContext::addMockDeltaMerge(
    const String & db,
    const String & table,
    const MockColumnInfoVec & columnInfos,
    ColumnsWithTypeAndName columns)
{
    assert(mock_storage->useDeltaMerge());
    assertMockInput(columnInfos, columns);

    addMockDeltaMergeSchema(db, table, columnInfos);
    addMockDeltaMergeData(db, table, columns);
}

void MockDAGRequestContext::addMockDeltaMerge(
    const MockTableName & name,
    const MockColumnInfoVec & columnInfos,
    ColumnsWithTypeAndName columns)
{
    assert(mock_storage->useDeltaMerge());
    assertMockInput(columnInfos, columns);

    addMockDeltaMergeSchema(name.first, name.second, columnInfos);
    addMockDeltaMergeData(name.first, name.second, columns);
}

void MockDAGRequestContext::addMockDeltaMerge(
    const MockTableName & name,
    const MockColumnInfoVec & columnInfos,
    ColumnsWithTypeAndName columns,
    size_t concurrency_hint)
{
    assert(mock_storage->useDeltaMerge());
    assertMockInput(columnInfos, columns);

    addMockDeltaMergeSchema(name.first, name.second, columnInfos);
    addMockDeltaMergeData(name.first, name.second, columns);
    addMockDeltaMergeTableConcurrencyHint(name, concurrency_hint);
}

void MockDAGRequestContext::addExchangeReceiver(
    const String & name,
    const MockColumnInfoVec & columnInfos,
    size_t fine_grained_stream_count,
    const MockColumnInfoVec & partition_column_infos)
{
    auto columns = getColumnWithTypeAndName(
        genNamesAndTypes(mockColumnInfosToTiDBColumnInfos(columnInfos), "mock_exchange_receiver"));
    addExchangeReceiver(name, columnInfos, columns, fine_grained_stream_count, partition_column_infos);
}

void MockDAGRequestContext::addExchangeReceiver(
    const String & name,
    const MockColumnInfoVec & columnInfos,
    const ColumnsWithTypeAndName & columns,
    size_t fine_grained_stream_count,
    const MockColumnInfoVec & partition_column_infos)
{
    assertMockInput(columnInfos, columns);
    addExchangeRelationSchema(name, columnInfos);
    addExchangeReceiverColumnData(name, columns);
    if (fine_grained_stream_count > 0)
    {
        Block original_block(columns);
        std::vector<Int64> partition_column_ids;
        for (const auto & mock_column_info : partition_column_infos)
        {
            for (size_t col_index = 0; col_index < columns.size(); col_index++)
            {
                if (columns[col_index].name == mock_column_info.name)
                {
                    partition_column_ids.push_back(col_index);
                    break;
                }
            }
        }
        RUNTIME_CHECK_MSG(
            partition_column_ids.size() == partition_column_infos.size(),
            "Could not find partition columns");
        TiDB::TiDBCollators collators(partition_column_infos.size(), nullptr);
        std::vector<String> partition_key_containers(partition_column_infos.size(), "");
        auto dest_tbl_cols = HashBaseWriterHelper::createDestColumns(original_block, fine_grained_stream_count);
        WeakHash32 hash(0);
        HashBaseWriterHelper::computeHash(
            original_block,
            partition_column_ids,
            collators,
            partition_key_containers,
            hash);

        IColumn::Selector selector;
        const auto & hash_data = hash.getData();
        selector.resize(original_block.rows());
        for (size_t i = 0; i < original_block.rows(); ++i)
        {
            selector[i] = hash_data[i] % fine_grained_stream_count;
        }

        for (size_t col_id = 0; col_id < original_block.columns(); ++col_id)
        {
            // Scatter columns to different partitions
            std::vector<MutableColumnPtr> part_columns
                = original_block.getByPosition(col_id).column->scatter(fine_grained_stream_count, selector);
            assert(part_columns.size() == fine_grained_stream_count);
            for (size_t bucket_idx = 0; bucket_idx < fine_grained_stream_count; ++bucket_idx)
            {
                dest_tbl_cols[bucket_idx][col_id] = std::move(part_columns[bucket_idx]);
            }
        }
        std::vector<ColumnsWithTypeAndName> fine_grained_columns_vector;
        for (size_t i = 0; i < fine_grained_stream_count; i++)
        {
            auto new_columns = columns;
            for (size_t j = 0; j < dest_tbl_cols[i].size(); j++)
                new_columns[j].column = std::move(dest_tbl_cols[i][j]);
            fine_grained_columns_vector.push_back(std::move(new_columns));
        }
        mock_storage->addFineGrainedExchangeData(name, fine_grained_columns_vector);
    }
}

DAGRequestBuilder MockDAGRequestContext::scan(const String & db_name, const String & table_name, bool keep_order)
{
    if (!mock_storage->useDeltaMerge())
    {
        auto table_info = mock_storage->getTableInfo(db_name + "." + table_name);
        return DAGRequestBuilder(index, collation)
            .mockTable(
                {db_name, table_name},
                table_info,
                mock_storage->getTableSchema(db_name + "." + table_name),
                keep_order);
    }
    else
    {
        auto table_info = mock_storage->getTableInfoForDeltaMerge(db_name + "." + table_name);
        return DAGRequestBuilder(index, collation)
            .mockTable(
                {db_name, table_name},
                table_info,
                mock_storage->getTableSchemaForDeltaMerge(db_name + "." + table_name),
                keep_order);
    }
}

DAGRequestBuilder MockDAGRequestContext::scan(
    const String & db_name,
    const String & table_name,
    const std::vector<int> & rf_ids)
{
    auto dag_request_builder = scan(db_name, table_name);
    mock::TableScanBinder * table_scan = dynamic_cast<mock::TableScanBinder *>(dag_request_builder.getRoot().get());
    if (table_scan)
    {
        table_scan->setRuntimeFilterIds(rf_ids);
    }
    return dag_request_builder;
}

DAGRequestBuilder MockDAGRequestContext::receive(
    const String & exchange_name,
    uint64_t fine_grained_shuffle_stream_count)
{
    auto builder = DAGRequestBuilder(index, collation)
                       .exchangeReceiver(
                           exchange_name,
                           mock_storage->getExchangeSchema(exchange_name),
                           fine_grained_shuffle_stream_count);
    receiver_source_task_ids_map[builder.getRoot()->name] = {};
    mock_storage->addExchangeRelation(builder.getRoot()->name, exchange_name);
    return builder;
}

void MockDAGRequestContext::initMockStorage()
{
    mock_storage = std::make_unique<MockStorage>();
}

void MockDAGRequestContext::assertMockInput(
    const MockColumnInfoVec & columnInfos [[maybe_unused]],
    ColumnsWithTypeAndName columns)
{
    assert(columnInfos.size() == columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        assert(columnInfos[i].name == columns[i].name);
}

} // namespace DB::tests
