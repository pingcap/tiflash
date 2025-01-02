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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Debug/MockExecutor/AggregationBinder.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/MockExecutor/ExchangeReceiverBinder.h>
#include <Debug/MockExecutor/ExchangeSenderBinder.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockExecutor/JoinBinder.h>
#include <Debug/MockExecutor/LimitBinder.h>
#include <Debug/MockExecutor/ProjectBinder.h>
#include <Debug/MockExecutor/SelectionBinder.h>
#include <Debug/MockExecutor/SortBinder.h>
#include <Debug/MockExecutor/TableScanBinder.h>
#include <Debug/MockExecutor/TopNBinder.h>
#include <Debug/MockExecutor/WindowBinder.h>
#include <Debug/dbgQueryCompiler.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/MockComputeClient.h>
#include <Storages/MutableSupport.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

const ASTTablesInSelectQueryElement * getJoin(ASTSelectQuery & ast_query)
{
    if (!ast_query.tables)
        return nullptr;

    const auto & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*ast_query.tables);
    if (tables_in_select_query.children.empty())
        return nullptr;

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const auto & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
        if (tables_element.table_join)
        {
            if (!joined_table)
                joined_table = &tables_element;
            else
                throw Exception(
                    "Support for more than one JOIN in query is not implemented",
                    ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return joined_table;
}

std::pair<ExecutorBinderPtr, bool> compileQueryBlock(
    Context & context,
    size_t & executor_index,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties,
    ASTSelectQuery & ast_query)
{
    const auto * joined_table = getJoin(ast_query);
    /// uniq_raw is used to test `ApproxCountDistinct`, when testing `ApproxCountDistinct` in mock coprocessor
    /// the return value of `ApproxCountDistinct` is just the raw result, we need to convert it to a readable
    /// value when decoding the result(using `UniqRawResReformatBlockOutputStream`)
    bool has_uniq_raw_res = false;
    ExecutorBinderPtr root_executor = nullptr;

    TableInfo table_info;
    String table_alias;
    {
        String database_name, table_name;
        auto query_database = ast_query.database();
        auto query_table = ast_query.table();
        if (query_database)
            database_name = typeid_cast<ASTIdentifier &>(*query_database).name;
        if (query_table)
            table_name = typeid_cast<ASTIdentifier &>(*query_table).name;
        if (!query_table)
        {
            database_name = "system";
            table_name = "one";
        }
        else if (!query_database)
        {
            database_name = context.getCurrentDatabase();
        }
        table_alias = query_table->tryGetAlias();
        if (table_alias.empty())
            table_alias = table_name;

        table_info = schema_fetcher(database_name, table_name);
    }

    if (!joined_table)
    {
        /// Table scan.
        {
            bool append_pk_column = false;
            for (const auto & expr : ast_query.select_expression_list->children)
            {
                if (auto * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
                {
                    if (identifier->getColumnName() == MutSup::extra_handle_column_name)
                    {
                        append_pk_column = true;
                    }
                }
            }
            root_executor = mock::compileTableScan(executor_index, table_info, "", table_alias, append_pk_column);
        }
    }
    else
    {
        TableInfo left_table_info = table_info;
        auto const & left_table_alias = table_alias;
        TableInfo right_table_info;
        String right_table_alias;
        {
            String database_name, table_name;
            const auto & table_to_join = static_cast<const ASTTableExpression &>(*joined_table->table_expression);
            if (table_to_join.database_and_table_name)
            {
                auto identifier = static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name);
                table_name = identifier.name;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception(
                            "Qualified table name could have only two components",
                            ErrorCodes::LOGICAL_ERROR);

                    database_name = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
                    table_name = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
                }
                if (database_name.empty())
                    database_name = context.getCurrentDatabase();

                right_table_alias = table_to_join.database_and_table_name->tryGetAlias();
                if (right_table_alias.empty())
                    right_table_alias = table_name;

                right_table_info = schema_fetcher(database_name, table_name);
            }
            else
            {
                throw Exception("subquery not supported as join source");
            }
        }
        /// Table scan.
        bool left_append_pk_column = false;
        bool right_append_pk_column = false;
        for (const auto & expr : ast_query.select_expression_list->children)
        {
            if (auto * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                auto [db_name, table_name, column_name] = splitQualifiedName(identifier->getColumnName());
                if (column_name == MutSup::extra_handle_column_name)
                {
                    if (table_name.empty())
                    {
                        throw Exception("tidb pk column must be qualified since there are more than one tables");
                    }
                    if (table_name == left_table_alias)
                        left_append_pk_column = true;
                    else if (table_name == right_table_alias)
                        right_append_pk_column = true;
                    else
                        throw Exception("Unknown table alias: " + table_name);
                }
            }
        }
        auto left_ts
            = mock::compileTableScan(executor_index, left_table_info, "", left_table_alias, left_append_pk_column);
        auto right_ts
            = mock::compileTableScan(executor_index, right_table_info, "", right_table_alias, right_append_pk_column);
        root_executor = mock::compileJoin(executor_index, left_ts, right_ts, joined_table->table_join);
    }

    /// Filter.
    if (ast_query.where_expression)
    {
        root_executor = compileSelection(root_executor, executor_index, ast_query.where_expression);
    }

    /// TopN.
    if (ast_query.order_expression_list && ast_query.limit_length)
    {
        root_executor
            = compileTopN(root_executor, executor_index, ast_query.order_expression_list, ast_query.limit_length);
    }
    else if (ast_query.limit_length)
    {
        root_executor = compileLimit(root_executor, executor_index, ast_query.limit_length);
    }

    bool has_gby = ast_query.group_expression_list != nullptr;
    bool has_agg_func = false;
    for (const auto & child : ast_query.select_expression_list->children)
    {
        const auto * func = typeid_cast<const ASTFunction *>(child.get());
        if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            has_agg_func = true;
            break;
        }
    }
    DAGSchema final_schema;
    /// Aggregation
    std::vector<size_t> output_offsets;
    if (has_gby || has_agg_func)
    {
        if (!properties.is_mpp_query
            && (dynamic_cast<mock::LimitBinder *>(root_executor.get()) != nullptr
                || dynamic_cast<mock::TopNBinder *>(root_executor.get()) != nullptr))
            throw Exception("Limit/TopN and Agg cannot co-exist in non-mpp mode.", ErrorCodes::LOGICAL_ERROR);

        root_executor = compileAggregation(
            root_executor,
            executor_index,
            ast_query.select_expression_list,
            has_gby ? ast_query.group_expression_list : nullptr);

        if (dynamic_cast<mock::AggregationBinder *>(root_executor.get())->hasUniqRawRes())
        {
            // todo support uniq_raw in mpp mode
            if (properties.is_mpp_query)
                throw Exception("uniq_raw_res not supported in mpp mode.", ErrorCodes::LOGICAL_ERROR);
            else
                has_uniq_raw_res = true;
        }

        auto * agg = dynamic_cast<mock::AggregationBinder *>(root_executor.get());
        if (agg->needAppendProject() || ast_query.select_expression_list->children.size() != agg->exprSize())
        {
            /// Project if needed
            root_executor = compileProject(root_executor, executor_index, ast_query.select_expression_list);
        }
    }
    else
    {
        /// Project
        root_executor = compileProject(root_executor, executor_index, ast_query.select_expression_list);
    }
    return std::make_pair(root_executor, has_uniq_raw_res);
}

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
    Context & context,
    const String & query,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties)
{
    MakeResOutputStream func_wrap_output_stream = [](BlockInputStreamPtr in) {
        return in;
    };

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    size_t executor_index = 0;
    auto [root_executor, has_uniq_raw_res]
        = compileQueryBlock(context, executor_index, schema_fetcher, properties, ast_query);
    if (has_uniq_raw_res)
        func_wrap_output_stream = [](BlockInputStreamPtr in) {
            return std::make_shared<UniqRawResReformatBlockOutputStream>(in);
        };

    // we will not prune column in executor test
    // since it doesn't call initOutputInfo in DAGContext
    if (!context.isExecutorTest())
    {
        /// finalize
        std::unordered_set<String> used_columns;
        for (auto & schema : root_executor->output_schema)
            used_columns.emplace(schema.first);

        root_executor->columnPrune(used_columns);
    }

    return std::make_tuple(
        queryPlanToQueryTasks(properties, root_executor, executor_index, context),
        func_wrap_output_stream);
}

TableID findTableIdForQueryFragment(ExecutorBinderPtr root_executor, bool must_have_table_id)
{
    ExecutorBinderPtr current_executor = root_executor;
    while (!current_executor->children.empty())
    {
        ExecutorBinderPtr non_exchange_child;
        for (const auto & c : current_executor->children)
        {
            if (dynamic_cast<mock::ExchangeReceiverBinder *>(c.get()))
                continue;
            non_exchange_child = c;
        }
        if (non_exchange_child == nullptr)
        {
            if (must_have_table_id)
                throw Exception("Table scan not found");
            return -1;
        }
        current_executor = non_exchange_child;
    }
    auto * ts = dynamic_cast<mock::TableScanBinder *>(current_executor.get());
    if (ts == nullptr)
    {
        if (must_have_table_id)
            throw Exception("Table scan not found");
        return -1;
    }
    return ts->getTableId();
}
QueryFragments mppQueryToQueryFragments(
    ExecutorBinderPtr root_executor,
    size_t & executor_index,
    const DAGProperties & properties,
    bool for_root_fragment,
    MPPCtxPtr mpp_ctx)
{
    QueryFragments fragments;
    std::unordered_map<
        String,
        std::pair<std::shared_ptr<mock::ExchangeReceiverBinder>, std::shared_ptr<mock::ExchangeSenderBinder>>>
        exchange_map;
    root_executor->toMPPSubPlan(executor_index, properties, exchange_map);
    TableID table_id = findTableIdForQueryFragment(root_executor, exchange_map.empty());
    std::vector<Int64> sender_target_task_ids = mpp_ctx->sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    size_t current_task_num = properties.mpp_partition_num;
    for (auto & exchange : exchange_map)
    {
        if (exchange.second.second->getType() == tipb::ExchangeType::PassThrough)
        {
            current_task_num = 1;
            break;
        }
    }
    std::vector<Int64> current_task_ids;
    for (size_t i = 0; i < current_task_num; ++i)
        current_task_ids.push_back(mpp_ctx->next_task_id++);
    for (auto & exchange : exchange_map)
    {
        mpp_ctx->sender_target_task_ids = current_task_ids;
        auto sub_fragments
            = mppQueryToQueryFragments(exchange.second.second, executor_index, properties, false, mpp_ctx);
        receiver_source_task_ids_map[exchange.first] = sub_fragments[sub_fragments.size() - 1].task_ids;
        fragments.insert(fragments.end(), sub_fragments.begin(), sub_fragments.end());
    }
    fragments.emplace_back(
        root_executor,
        table_id,
        for_root_fragment,
        std::move(sender_target_task_ids),
        std::move(receiver_source_task_ids_map),
        std::move(current_task_ids));
    return fragments;
}

QueryFragments queryPlanToQueryFragments(
    const DAGProperties & properties,
    ExecutorBinderPtr root_executor,
    size_t & executor_index)
{
    if (properties.is_mpp_query)
    {
        ExecutorBinderPtr root_exchange_sender = std::make_shared<mock::ExchangeSenderBinder>(
            executor_index,
            root_executor->output_schema,
            tipb::PassThrough);
        root_exchange_sender->children.push_back(root_executor);
        root_executor = root_exchange_sender;
        MPPCtxPtr mpp_ctx = std::make_shared<MPPCtx>(properties.start_ts);
        mpp_ctx->sender_target_task_ids.emplace_back(-1);
        return mppQueryToQueryFragments(root_executor, executor_index, properties, true, mpp_ctx);
    }
    else
    {
        QueryFragments fragments;
        fragments.emplace_back(root_executor, findTableIdForQueryFragment(root_executor, true), true);
        return fragments;
    }
}

QueryTasks queryPlanToQueryTasks(
    const DAGProperties & properties,
    ExecutorBinderPtr root_executor,
    size_t & executor_index,
    const Context & context)
{
    QueryFragments fragments = queryPlanToQueryFragments(properties, root_executor, executor_index);
    QueryTasks tasks;
    for (auto & fragment : fragments)
    {
        auto t = fragment.toQueryTasks(properties, context);
        tasks.insert(tasks.end(), t.begin(), t.end());
    }
    return tasks;
}
} // namespace DB
