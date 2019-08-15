#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/select.pb.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICA_ERROR;
} // namespace ErrorCodes

using DAGField = std::pair<String, tipb::FieldType>;
using DAGSchema = std::vector<DAGField>;
using SchemaFetcher = std::function<TiDB::TableInfo(const String &, const String &)>;
std::tuple<TableID, DAGSchema, tipb::DAGRequest> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, Timestamp start_ts);
tipb::SelectResponse executeDAGRequest(
    Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version);
BlockInputStreamPtr outputDAGResponse(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response);

BlockInputStreamPtr dbgFuncDAG(Context & context, const ASTs & args)
{
    if (args.size() < 1 || args.size() > 2)
        throw Exception("Args not matched, should be: query[, region-id]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = InvalidRegionID;
    if (args.size() == 2)
        region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [table_id, schema, dag_request] = compileQuery(context, query,
        [&](const String & database_name, const String & table_name) {
            auto storage = context.getTable(database_name, table_name);
            auto mmt = std::dynamic_pointer_cast<StorageMergeTree>(storage);
            if (!mmt || mmt->getData().merging_params.mode != MergeTreeData::MergingParams::Txn)
                throw Exception("Not TMT", ErrorCodes::BAD_ARGUMENTS);
            return mmt->getTableInfo();
        },
        start_ts);

    RegionPtr region;
    if (region_id == InvalidRegionID)
    {
        auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
        if (regions.empty())
            throw Exception("No region for table", ErrorCodes::BAD_ARGUMENTS);
        region = context.getTMTContext().getRegionTable().getRegionsByTable(table_id).front().second;
    }
    else
    {
        region = context.getTMTContext().getRegionTable().getRegionByTableAndID(table_id, region_id);
        if (!region)
            throw Exception("No such region", ErrorCodes::BAD_ARGUMENTS);
    }
    tipb::SelectResponse dag_response = executeDAGRequest(context, dag_request, region->id(), region->version(), region->confVer());

    return outputDAGResponse(context, schema, dag_response);
}

BlockInputStreamPtr dbgFuncMockDAG(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 3)
        throw Exception("Args not matched, should be: query, region-id[, start-ts]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    if (args.size() == 3)
        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    if (start_ts == 0)
        start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [table_id, schema, dag_request] = compileQuery(context, query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        start_ts);
    std::ignore = table_id;

    RegionPtr region = context.getTMTContext().getKVStore()->getRegion(region_id);
    tipb::SelectResponse dag_response = executeDAGRequest(context, dag_request, region_id, region->version(), region->confVer());

    return outputDAGResponse(context, schema, dag_response);
}

struct ExecutorCtx
{
    tipb::Executor * input;
    DAGSchema output;
    std::unordered_map<String, tipb::Expr *> col_ref_map;
};

void compileExpr(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, std::unordered_set<String> & referred_columns,
    std::unordered_map<String, tipb::Expr *> & col_ref_map)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) { return field.first == id->getColumnName(); });
        if (ft == input.end())
            throw DB::Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        expr->set_tp(tipb::ColumnRef);
        *(expr->mutable_field_type()) = (*ft).second;

        referred_columns.emplace((*ft).first);
        col_ref_map.emplace((*ft).first, expr);
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        // TODO: Support agg functions.
        for (const auto & child_ast : func->arguments->children)
        {
            tipb::Expr * child = expr->add_children();
            compileExpr(input, child_ast, child, referred_columns, col_ref_map);
        }

        String func_name_lowercase = Poco::toLower(func->name);
        // TODO: Support more functions.
        // TODO: Support type inference.
        if (func_name_lowercase == "equals")
        {
            expr->set_sig(tipb::ScalarFuncSig::EQInt);
            auto * ft = expr->mutable_field_type();
            // TODO: TiDB will infer Int64.
            ft->set_tp(TiDB::TypeTiny);
            ft->set_flag(TiDB::ColumnFlagUnsigned);
        }
        else if (func_name_lowercase == "and")
        {
            expr->set_sig(tipb::ScalarFuncSig::LogicalAnd);
            auto * ft = expr->mutable_field_type();
            // TODO: TiDB will infer Int64.
            ft->set_tp(TiDB::TypeTiny);
            ft->set_flag(TiDB::ColumnFlagUnsigned);
        }
        else if (func_name_lowercase == "or")
        {
            expr->set_sig(tipb::ScalarFuncSig::LogicalOr);
            auto * ft = expr->mutable_field_type();
            // TODO: TiDB will infer Int64.
            ft->set_tp(TiDB::TypeTiny);
            ft->set_flag(TiDB::ColumnFlagUnsigned);
        }
        else
        {
            throw DB::Exception("Unsupported function: " + func_name_lowercase, ErrorCodes::LOGICAL_ERROR);
        }
        expr->set_tp(tipb::ExprType::ScalarFunc);
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        std::stringstream ss;
        switch (lit->value.getType())
        {
            case Field::Types::Which::Null:
                expr->set_tp(tipb::Null);
                // Null literal epxr doesn't need value.
                break;
            case Field::Types::Which::UInt64:
                expr->set_tp(tipb::Uint64);
                encodeDAGUInt64(lit->value.get<UInt64>(), ss);
                break;
            case Field::Types::Which::Int64:
                expr->set_tp(tipb::Int64);
                encodeDAGInt64(lit->value.get<Int64>(), ss);
                break;
            case Field::Types::Which::Float64:
                expr->set_tp(tipb::Float64);
                encodeDAGFloat64(lit->value.get<Float64>(), ss);
                break;
            case Field::Types::Which::Decimal:
                expr->set_tp(tipb::MysqlDecimal);
                encodeDAGDecimal(lit->value.get<Decimal>(), ss);
                break;
            case Field::Types::Which::String:
                expr->set_tp(tipb::String);
                // TODO: Align with TiDB.
                encodeDAGBytes(lit->value.get<String>(), ss);
                break;
            default:
                throw DB::Exception(String("Unsupported literal type: ") + lit->value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
        }
        expr->set_val(ss.str());
    }
    else
    {
        throw DB::Exception("Unsupported expression " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
}

std::tuple<TableID, DAGSchema, tipb::DAGRequest> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, Timestamp start_ts)
{
    DAGSchema schema;
    tipb::DAGRequest dag_request;

    dag_request.set_start_ts(start_ts);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    /// Get table metadata.
    TiDB::TableInfo table_info;
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

        table_info = schema_fetcher(database_name, table_name);
    }

    std::unordered_map<tipb::Executor *, ExecutorCtx> executor_ctx_map;
    std::unordered_set<String> referred_columns;
    tipb::TableScan * ts = nullptr;
    tipb::Executor * last_executor = nullptr;

    /// Table scan.
    {
        tipb::Executor * ts_exec = dag_request.add_executors();
        ts_exec->set_tp(tipb::ExecType::TypeTableScan);
        ts = ts_exec->mutable_tbl_scan();
        ts->set_table_id(table_info.id);
        DAGSchema ts_output;
        for (const auto & column_info : table_info.columns)
        {
            tipb::FieldType field_type;
            field_type.set_tp(column_info.tp);
            field_type.set_flag(column_info.flag);
            field_type.set_flen(column_info.flen);
            field_type.set_decimal(column_info.decimal);
            ts_output.emplace_back(std::make_pair(column_info.name, std::move(field_type)));
        }
        executor_ctx_map.emplace(ts_exec, ExecutorCtx{nullptr, std::move(ts_output), std::unordered_map<String, tipb::Expr *>{}});
        last_executor = ts_exec;
    }

    /// Filter.
    if (ast_query.where_expression)
    {
        tipb::Executor * filter_exec = dag_request.add_executors();
        filter_exec->set_tp(tipb::ExecType::TypeSelection);
        tipb::Selection * filter = filter_exec->mutable_selection();
        tipb::Expr * cond = filter->add_conditions();
        std::unordered_map<String, tipb::Expr *> col_ref_map;
        compileExpr(executor_ctx_map[last_executor].output, ast_query.where_expression, cond, referred_columns, col_ref_map);
        executor_ctx_map.emplace(filter_exec, ExecutorCtx{last_executor, executor_ctx_map[last_executor].output, std::move(col_ref_map)});
        last_executor = filter_exec;
    }

    /// TopN.
    if (ast_query.order_expression_list && ast_query.limit_length)
    {
        tipb::Executor * topn_exec = dag_request.add_executors();
        topn_exec->set_tp(tipb::ExecType::TypeTopN);
        tipb::TopN * topn = topn_exec->mutable_topn();
        std::unordered_map<String, tipb::Expr *> col_ref_map;
        for (const auto & child : ast_query.order_expression_list->children)
        {
            ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
            if (!elem)
                throw DB::Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
            tipb::ByItem * by = topn->add_order_by();
            by->set_desc(elem->direction < 0);
            tipb::Expr * expr = by->mutable_expr();
            compileExpr(executor_ctx_map[last_executor].output, elem->children[0], expr, referred_columns, col_ref_map);
        }
        auto limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*ast_query.limit_length).value);
        topn->set_limit(limit);
        executor_ctx_map.emplace(topn_exec, ExecutorCtx{last_executor, executor_ctx_map[last_executor].output, std::move(col_ref_map)});
        last_executor = topn_exec;
    }
    else if (ast_query.limit_length)
    {
        tipb::Executor * limit_exec = dag_request.add_executors();
        limit_exec->set_tp(tipb::ExecType::TypeLimit);
        tipb::Limit * limit = limit_exec->mutable_limit();
        auto limit_length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*ast_query.limit_length).value);
        limit->set_limit(limit_length);
        executor_ctx_map.emplace(
            limit_exec, ExecutorCtx{last_executor, executor_ctx_map[last_executor].output, std::unordered_map<String, tipb::Expr *>{}});
        last_executor = limit_exec;
    }

    /// Column pruner.
    std::function<void(ExecutorCtx &)> column_pruner = [&](ExecutorCtx & executor_ctx) {
        if (!executor_ctx.input)
        {
            executor_ctx.output.erase(std::remove_if(executor_ctx.output.begin(), executor_ctx.output.end(),
                                          [&](const auto & field) { return referred_columns.count(field.first) == 0; }),
                executor_ctx.output.end());

            for (const auto & field : executor_ctx.output)
            {
                tipb::ColumnInfo * ci = ts->add_columns();
                ci->set_column_id(table_info.getColumnID(field.first));
                ci->set_tp(field.second.tp());
                ci->set_flag(field.second.flag());
                ci->set_columnlen(field.second.flen());
                ci->set_decimal(field.second.decimal());
            }

            return;
        }
        column_pruner(executor_ctx_map[executor_ctx.input]);
        const auto & last_output = executor_ctx_map[executor_ctx.input].output;
        for (const auto & pair : executor_ctx.col_ref_map)
        {
            auto iter = std::find_if(last_output.begin(), last_output.end(), [&](const auto & field) { return field.first == pair.first; });
            if (iter == last_output.end())
                throw DB::Exception("Column not found when pruning: " + pair.first, ErrorCodes::LOGICAL_ERROR);
            std::stringstream ss;
            encodeDAGInt64(iter - last_output.begin(), ss);
            pair.second->set_val(ss.str());
        }
        executor_ctx.output = last_output;
    };

    /// Aggregation finalize.
    {
        bool has_gby = ast_query.group_expression_list != nullptr;
        bool has_agg_func = false;
        for (const auto & child : ast_query.select_expression_list->children)
        {
            const ASTFunction * func = typeid_cast<const ASTFunction *>(child.get());
            if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                has_agg_func = true;
                break;
            }
        }

        if (has_gby || has_agg_func)
        {
            if (last_executor->has_limit() || last_executor->has_topn())
                throw DB::Exception("Limit/TopN and Agg cannot co-exist.", ErrorCodes::LOGICAL_ERROR);

            tipb::Executor * agg_exec = dag_request.add_executors();
            agg_exec->set_tp(tipb::ExecType::TypeAggregation);
            tipb::Aggregation * agg = agg_exec->mutable_aggregation();
            std::unordered_map<String, tipb::Expr *> col_ref_map;
            for (const auto & expr : ast_query.select_expression_list->children)
            {
                const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
                if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
                    throw DB::Exception("Only agg function is allowed in select for a query with aggregation", ErrorCodes::LOGICAL_ERROR);

                tipb::Expr * agg_func = agg->add_agg_func();

                for (const auto & arg : func->arguments->children)
                {
                    tipb::Expr * arg_expr = agg_func->add_children();
                    compileExpr(executor_ctx_map[last_executor].output, arg, arg_expr, referred_columns, col_ref_map);
                }

                if (func->name == "count")
                {
                    agg_func->set_tp(tipb::Count);
                    auto ft = agg_func->mutable_field_type();
                    ft->set_tp(TiDB::TypeLongLong);
                    ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
                }
                // TODO: Other agg func.
                else
                {
                    throw DB::Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
                }

                schema.emplace_back(std::make_pair(func->getColumnName(), agg_func->field_type()));
            }

            if (has_gby)
            {
                for (const auto & child : ast_query.group_expression_list->children)
                {
                    tipb::Expr * gby = agg->add_group_by();
                    compileExpr(executor_ctx_map[last_executor].output, child, gby, referred_columns, col_ref_map);
                    schema.emplace_back(std::make_pair(child->getColumnName(), gby->field_type()));
                }
            }

            executor_ctx_map.emplace(agg_exec, ExecutorCtx{last_executor, DAGSchema{}, std::move(col_ref_map)});
            last_executor = agg_exec;

            column_pruner(executor_ctx_map[last_executor]);
        }
    }

    /// Non-aggregation finalize.
    if (!last_executor->has_aggregation())
    {
        std::vector<String> final_output;
        for (const auto & expr : ast_query.select_expression_list->children)
        {
            if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                referred_columns.emplace(id->getColumnName());
                final_output.emplace_back(id->getColumnName());
            }
            else if (typeid_cast<ASTAsterisk *>(expr.get()))
            {
                const auto & last_output = executor_ctx_map[last_executor].output;
                for (const auto & field : last_output)
                {
                    referred_columns.emplace(field.first);
                    final_output.push_back(field.first);
                }
            }
            else
            {
                throw DB::Exception("Unsupported expression type in select", ErrorCodes::LOGICAL_ERROR);
            }
        }

        column_pruner(executor_ctx_map[last_executor]);

        const auto & last_output = executor_ctx_map[last_executor].output;
        for (const auto & field : final_output)
        {
            auto iter
                = std::find_if(last_output.begin(), last_output.end(), [&](const auto & last_field) { return last_field.first == field; });
            if (iter == last_output.end())
                throw DB::Exception("Column not found after pruning: " + field, ErrorCodes::LOGICAL_ERROR);
            dag_request.add_output_offsets(iter - last_output.begin());
            schema.push_back(*iter);
        }
    }

    return std::make_tuple(table_info.id, std::move(schema), std::move(dag_request));
}

tipb::SelectResponse executeDAGRequest(
    Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version)
{
    static Logger * log = &Logger::get("MockDAG");
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
    context.setSetting("dag_planner", "optree");
    tipb::SelectResponse dag_response;
    DAGDriver driver(context, dag_request, region_id, region_version, region_conf_version, dag_response, true);
    driver.execute();
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle DAG request done");
    return dag_response;
}

BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    if (dag_response.has_error())
        throw DB::Exception(dag_response.error().msg(), dag_response.error().code());

    BlocksList blocks;
    for (const auto & chunk : dag_response.chunks())
    {
        std::vector<std::vector<DB::Field>> rows;
        std::vector<DB::Field> curr_row;
        const std::string & data = chunk.rows_data();
        size_t cursor = 0;
        while (cursor < data.size())
        {
            curr_row.push_back(DB::DecodeDatum(cursor, data));
            if (curr_row.size() == schema.size())
            {
                rows.emplace_back(std::move(curr_row));
                curr_row.clear();
            }
        }

        ColumnsWithTypeAndName columns;
        for (auto & field : schema)
        {
            const auto & name = field.first;
            auto data_type = getDataTypeByFieldType(field.second);
            ColumnWithTypeAndName col(data_type, name);
            col.column->assumeMutable()->reserve(rows.size());
            columns.emplace_back(std::move(col));
        }
        for (const auto & row : rows)
        {
            for (size_t i = 0; i < row.size(); i++)
            {
                columns[i].column->assumeMutable()->insert(row[i]);
            }
        }

        blocks.emplace_back(Block(columns));
    }

    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}

} // namespace DB
