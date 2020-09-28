#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/StringTokenizer.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Datum.h>
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
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using TiDB::DatumFlat;
using TiDB::TableInfo;

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;
using SchemaFetcher = std::function<TableInfo(const String &, const String &)>;
static const String ENCODE_TYPE_NAME = "encode_type";
static const String TZ_OFFSET_NAME = "tz_offset";
static const String TZ_NAME_NAME = "tz_name";
static const String COLLATOR_NAME = "collator";

struct DAGProperties
{
    String encode_type = "";
    Int64 tz_offset = 0;
    String tz_name = "";
    Int32 collator = 0;
};

using MakeResOutputStream = std::function<BlockInputStreamPtr(BlockInputStreamPtr)>;

std::tuple<TableID, DAGSchema, tipb::DAGRequest, MakeResOutputStream> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, const DAGProperties & properties);

class UniqRawResReformatBlockOutputStream : public IProfilingBlockInputStream
{
public:
    UniqRawResReformatBlockOutputStream(const BlockInputStreamPtr & in_) : in(in_) {}

    String getName() const override { return "UniqRawResReformat"; }

    Block getHeader() const override { return in->getHeader(); }

protected:
    Block readImpl() override
    {
        while (true)
        {
            Block block = in->read();
            if (!block)
                return block;

            size_t num_columns = block.columns();
            MutableColumns columns(num_columns);
            for (size_t i = 0; i < num_columns; ++i)
            {
                ColumnWithTypeAndName & ori_column = block.getByPosition(i);

                if (std::string::npos != ori_column.name.find_first_of(UniqRawResName))
                {
                    MutableColumnPtr mutable_holder = ori_column.column->cloneEmpty();

                    for (size_t j = 0; j < ori_column.column->size(); ++j)
                    {
                        Field field;
                        ori_column.column->get(j, field);

                        auto & str_ref = field.safeGet<String>();

                        ReadBufferFromString in(str_ref);
                        AggregateFunctionUniqUniquesHashSetDataForVariadicRawRes set;
                        set.set.read(in);

                        mutable_holder->insert(std::to_string(set.set.size()));
                    }
                    ori_column.column = std::move(mutable_holder);
                }
            }
            return block;
        }
    }

private:
    BlockInputStreamPtr in;
};

tipb::SelectResponse executeDAGRequest(Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version,
    UInt64 region_conf_version, Timestamp start_ts, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges);
BlockInputStreamPtr outputDAGResponse(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response);


DAGProperties getDAGProperties(String prop_string)
{
    DAGProperties ret;
    if (prop_string.empty())
        return ret;
    std::unordered_map<String, String> properties;
    Poco::StringTokenizer string_tokens(prop_string, ",");
    for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
    {
        Poco::StringTokenizer tokens(*it, ":");
        if (tokens.count() != 2)
            continue;
        properties[Poco::toLower(tokens[0])] = tokens[1];
    }

    if (properties.find(ENCODE_TYPE_NAME) != properties.end())
        ret.encode_type = properties[ENCODE_TYPE_NAME];
    if (properties.find(TZ_OFFSET_NAME) != properties.end())
        ret.tz_offset = std::stol(properties[TZ_OFFSET_NAME]);
    if (properties.find(TZ_NAME_NAME) != properties.end())
        ret.tz_name = properties[TZ_NAME_NAME];
    if (properties.find(COLLATOR_NAME) != properties.end())
        ret.collator = std::stoi(properties[COLLATOR_NAME]);

    return ret;
}

BlockInputStreamPtr dbgFuncDAG(Context & context, const ASTs & args)
{
    if (args.size() < 1 || args.size() > 3)
        throw Exception("Args not matched, should be: query[, region-id, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = InvalidRegionID;
    if (args.size() >= 2)
        region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);

    String prop_string = "";
    if (args.size() == 3)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    DAGProperties properties = getDAGProperties(prop_string);
    Timestamp start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [table_id, schema, dag_request, func_wrap_output_stream] = compileQuery(
        context, query,
        [&](const String & database_name, const String & table_name) {
            auto storage = context.getTable(database_name, table_name);
            auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
            if (!managed_storage //
                || !(managed_storage->engineType() == ::TiDB::StorageEngine::DT
                    || managed_storage->engineType() == ::TiDB::StorageEngine::TMT))
                throw Exception(database_name + "." + table_name + " is not ManageableStorage", ErrorCodes::BAD_ARGUMENTS);
            return managed_storage->getTableInfo();
        },
        properties);

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
        region = context.getTMTContext().getKVStore()->getRegion(region_id);
        if (!region)
            throw Exception("No such region", ErrorCodes::BAD_ARGUMENTS);
    }

    auto handle_range = getHandleRangeByTable(region->getRange()->rawKeys(), table_id);
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges;
    DecodedTiKVKeyPtr start_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
    DecodedTiKVKeyPtr end_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
    key_ranges.emplace_back(std::make_pair(std::move(start_key), std::move(end_key)));
    tipb::SelectResponse dag_response
        = executeDAGRequest(context, dag_request, region->id(), region->version(), region->confVer(), start_ts, key_ranges);

    return func_wrap_output_stream(outputDAGResponse(context, schema, dag_response));
}

BlockInputStreamPtr dbgFuncMockDAG(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 4)
        throw Exception("Args not matched, should be: query, region-id[, start-ts, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    if (args.size() >= 3)
        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    if (start_ts == 0)
        start_ts = context.getTMTContext().getPDClient()->getTS();

    String prop_string = "";
    if (args.size() == 4)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    DAGProperties properties = getDAGProperties(prop_string);

    auto [table_id, schema, dag_request, func_wrap_output_stream] = compileQuery(
        context, query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        properties);
    std::ignore = table_id;

    RegionPtr region = context.getTMTContext().getKVStore()->getRegion(region_id);
    auto handle_range = getHandleRangeByTable(region->getRange()->rawKeys(), table_id);
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges;
    DecodedTiKVKeyPtr start_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
    DecodedTiKVKeyPtr end_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
    key_ranges.emplace_back(std::make_pair(std::move(start_key), std::move(end_key)));
    tipb::SelectResponse dag_response
        = executeDAGRequest(context, dag_request, region_id, region->version(), region->confVer(), start_ts, key_ranges);

    return func_wrap_output_stream(outputDAGResponse(context, schema, dag_response));
}

struct ExecutorCtx
{
    tipb::Executor * input;
    DAGSchema output;
    std::unordered_map<String, std::vector<tipb::Expr *>> col_ref_map;
};

std::unordered_map<String, tipb::ScalarFuncSig> func_name_to_sig({
    {"equals", tipb::ScalarFuncSig::EQInt},
    {"and", tipb::ScalarFuncSig::LogicalAnd},
    {"or", tipb::ScalarFuncSig::LogicalOr},
    {"greater", tipb::ScalarFuncSig::GTInt},
    {"greaterorequals", tipb::ScalarFuncSig::GEInt},
    {"less", tipb::ScalarFuncSig::LTInt},
    {"lessorequals", tipb::ScalarFuncSig::LEInt},
    {"in", tipb::ScalarFuncSig::InInt},
    {"notin", tipb::ScalarFuncSig::InInt},
    {"date_format", tipb::ScalarFuncSig::DateFormatSig},
    {"if", tipb::ScalarFuncSig::IfInt},
    {"from_unixtime", tipb::ScalarFuncSig::FromUnixTime2Arg},
    {"bit_and", tipb::ScalarFuncSig::BitAndSig},
    {"bit_or", tipb::ScalarFuncSig::BitOrSig},
    {"bit_xor", tipb::ScalarFuncSig::BitXorSig},
    {"bit_not", tipb::ScalarFuncSig::BitNegSig},
    {"notequals", tipb::ScalarFuncSig::NEInt},
    {"like", tipb::ScalarFuncSig::LikeSig},
    {"cast_int_int", tipb::ScalarFuncSig::CastIntAsInt},
    {"cast_real_int", tipb::ScalarFuncSig::CastRealAsInt},
    {"cast_decimal_int", tipb::ScalarFuncSig::CastDecimalAsInt},
    {"cast_time_int", tipb::ScalarFuncSig::CastTimeAsInt},
    {"cast_string_int", tipb::ScalarFuncSig::CastStringAsInt},
    {"cast_int_decimal", tipb::ScalarFuncSig::CastIntAsDecimal},
    {"cast_real_decimal", tipb::ScalarFuncSig::CastRealAsDecimal},
    {"cast_decimal_decimal", tipb::ScalarFuncSig::CastDecimalAsDecimal},
    {"cast_time_decimal", tipb::ScalarFuncSig::CastTimeAsDecimal},
    {"cast_string_decimal", tipb::ScalarFuncSig::CastStringAsDecimal},
});

void compileExpr(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, std::unordered_set<String> & referred_columns,
    std::unordered_map<String, std::vector<tipb::Expr *>> & col_ref_map, Int32 collator_id)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) { return field.first == id->getColumnName(); });
        if (ft == input.end())
            throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        expr->set_tp(tipb::ColumnRef);
        *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);

        referred_columns.emplace((*ft).first);
        if (col_ref_map.find((*ft).first) == col_ref_map.end())
            col_ref_map[(*ft).first] = {};
        col_ref_map[(*ft).first].push_back(expr);
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        String func_name_lowercase = Poco::toLower(func->name);
        // TODO: Support more functions.
        // TODO: Support type inference.

        const auto it_sig = func_name_to_sig.find(func_name_lowercase);
        if (it_sig == func_name_to_sig.end())
        {
            throw Exception("Unsupported function: " + func_name_lowercase, ErrorCodes::LOGICAL_ERROR);
        }
        switch (it_sig->second)
        {
            case tipb::ScalarFuncSig::InInt:
            {
                tipb::Expr * in_expr = expr;
                if (func_name_lowercase == "notin")
                {
                    // notin is transformed into not(in()) by tidb
                    expr->set_sig(tipb::ScalarFuncSig::UnaryNotInt);
                    auto * ft = expr->mutable_field_type();
                    ft->set_tp(TiDB::TypeLongLong);
                    ft->set_flag(TiDB::ColumnFlagUnsigned);
                    expr->set_tp(tipb::ExprType::ScalarFunc);
                    in_expr = expr->add_children();
                }
                in_expr->set_sig(tipb::ScalarFuncSig::InInt);
                auto * ft = in_expr->mutable_field_type();
                ft->set_tp(TiDB::TypeLongLong);
                ft->set_flag(TiDB::ColumnFlagUnsigned);
                ft->set_collate(collator_id);
                in_expr->set_tp(tipb::ExprType::ScalarFunc);
                for (const auto & child_ast : func->arguments->children)
                {
                    auto * tuple_func = typeid_cast<ASTFunction *>(child_ast.get());
                    if (tuple_func != nullptr && tuple_func->name == "tuple")
                    {
                        // flatten tuple elements
                        for (const auto & c : tuple_func->arguments->children)
                        {
                            tipb::Expr * child = in_expr->add_children();
                            compileExpr(input, c, child, referred_columns, col_ref_map, collator_id);
                        }
                    }
                    else
                    {
                        tipb::Expr * child = in_expr->add_children();
                        compileExpr(input, child_ast, child, referred_columns, col_ref_map, collator_id);
                    }
                }
                return;
            }
            case tipb::ScalarFuncSig::IfInt:
            case tipb::ScalarFuncSig::BitAndSig:
            case tipb::ScalarFuncSig::BitOrSig:
            case tipb::ScalarFuncSig::BitXorSig:
            case tipb::ScalarFuncSig::BitNegSig:
                expr->set_sig(it_sig->second);
                expr->set_tp(tipb::ExprType::ScalarFunc);
                for (size_t i = 0; i < func->arguments->children.size(); i++)
                {
                    const auto & child_ast = func->arguments->children[i];
                    tipb::Expr * child = expr->add_children();
                    compileExpr(input, child_ast, child, referred_columns, col_ref_map, collator_id);
                    // todo should infer the return type based on all input types
                    if ((it_sig->second == tipb::ScalarFuncSig::IfInt && i == 1)
                        || (it_sig->second != tipb::ScalarFuncSig::IfInt && i == 0))
                        *(expr->mutable_field_type()) = child->field_type();
                }
                return;
            case tipb::ScalarFuncSig::LikeSig:
            {
                expr->set_sig(tipb::ScalarFuncSig::LikeSig);
                auto * ft = expr->mutable_field_type();
                ft->set_tp(TiDB::TypeLongLong);
                ft->set_flag(TiDB::ColumnFlagUnsigned);
                ft->set_collate(collator_id);
                expr->set_tp(tipb::ExprType::ScalarFunc);
                for (const auto & child_ast : func->arguments->children)
                {
                    tipb::Expr * child = expr->add_children();
                    compileExpr(input, child_ast, child, referred_columns, col_ref_map, collator_id);
                }
                // for like need to add the third argument
                tipb::Expr * constant_expr = expr->add_children();
                constructInt64LiteralTiExpr(*constant_expr, 92);
                return;
            }
            case tipb::ScalarFuncSig::FromUnixTime2Arg:
                if (func->arguments->children.size() == 1)
                {
                    expr->set_sig(tipb::ScalarFuncSig::FromUnixTime1Arg);
                    auto * ft = expr->mutable_field_type();
                    ft->set_tp(TiDB::TypeDatetime);
                    ft->set_decimal(6);
                }
                else
                {
                    expr->set_sig(tipb::ScalarFuncSig::FromUnixTime2Arg);
                    auto * ft = expr->mutable_field_type();
                    ft->set_tp(TiDB::TypeString);
                }
                break;
            case tipb::ScalarFuncSig::DateFormatSig:
                expr->set_sig(tipb::ScalarFuncSig::DateFormatSig);
                expr->mutable_field_type()->set_tp(TiDB::TypeString);
                break;
            default:
            {
                expr->set_sig(it_sig->second);
                auto * ft = expr->mutable_field_type();
                ft->set_tp(TiDB::TypeLongLong);
                ft->set_flag(TiDB::ColumnFlagUnsigned);
                ft->set_collate(collator_id);
                break;
            }
        }
        expr->set_tp(tipb::ExprType::ScalarFunc);
        for (const auto & child_ast : func->arguments->children)
        {
            tipb::Expr * child = expr->add_children();
            compileExpr(input, child_ast, child, referred_columns, col_ref_map, collator_id);
        }
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        std::stringstream ss;
        switch (lit->value.getType())
        {
            case Field::Types::Which::Null:
                expr->set_tp(tipb::Null);
                // Null literal expr doesn't need value.
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
            case Field::Types::Which::Decimal32:
            case Field::Types::Which::Decimal64:
            case Field::Types::Which::Decimal128:
            case Field::Types::Which::Decimal256:
                expr->set_tp(tipb::MysqlDecimal);
                encodeDAGDecimal(lit->value, ss);
                break;
            case Field::Types::Which::String:
                expr->set_tp(tipb::String);
                // TODO: Align with TiDB.
                encodeDAGBytes(lit->value.get<String>(), ss);
                break;
            default:
                throw Exception(String("Unsupported literal type: ") + lit->value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
        }
        expr->set_val(ss.str());
    }
    else
    {
        throw Exception("Unsupported expression " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
}

void compileFilter(const DAGSchema & input, ASTPtr ast, tipb::Selection * filter, std::unordered_set<String> & referred_columns,
    std::unordered_map<String, std::vector<tipb::Expr *>> & col_ref_map, Int32 collator_id)
{
    if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (func->name == "and")
        {
            for (auto & child : func->arguments->children)
            {
                compileFilter(input, child, filter, referred_columns, col_ref_map, collator_id);
            }
            return;
        }
    }
    tipb::Expr * cond = filter->add_conditions();
    compileExpr(input, ast, cond, referred_columns, col_ref_map, collator_id);
}

std::tuple<TableID, DAGSchema, tipb::DAGRequest, MakeResOutputStream> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, const DAGProperties & properties)
{
    MakeResOutputStream func_wrap_output_stream = [](BlockInputStreamPtr in) { return in; };
    DAGSchema schema;
    tipb::DAGRequest dag_request;
    dag_request.set_time_zone_name(properties.tz_name);
    dag_request.set_time_zone_offset(properties.tz_offset);
    dag_request.set_flags(dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));

    if (properties.encode_type == "chunk")
        dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
    else if (properties.encode_type == "chblock")
        dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
    else
        dag_request.set_encode_type(tipb::EncodeType::TypeDefault);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    /// Get table metadata.
    TableInfo table_info;
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
            ColumnInfo ci;
            ci.tp = column_info.tp;
            ci.flag = column_info.flag;
            ci.flen = column_info.flen;
            ci.decimal = column_info.decimal;
            ci.elems = column_info.elems;
            ci.default_value = column_info.default_value;
            ci.origin_default_value = column_info.origin_default_value;
            ts_output.emplace_back(std::make_pair(column_info.name, std::move(ci)));
        }
        for (const auto & expr : ast_query.select_expression_list->children)
        {
            if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                if (identifier->getColumnName() == MutableSupport::tidb_pk_column_name)
                {
                    ColumnInfo ci;
                    ci.tp = TiDB::TypeLongLong;
                    ci.setPriKeyFlag();
                    ci.setNotNullFlag();
                    ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
                }
            }
        }
        executor_ctx_map.emplace(
            ts_exec, ExecutorCtx{nullptr, std::move(ts_output), std::unordered_map<String, std::vector<tipb::Expr *>>{}});
        last_executor = ts_exec;
    }

    /// Filter.
    if (ast_query.where_expression)
    {
        tipb::Executor * filter_exec = dag_request.add_executors();
        filter_exec->set_tp(tipb::ExecType::TypeSelection);
        tipb::Selection * filter = filter_exec->mutable_selection();
        std::unordered_map<String, std::vector<tipb::Expr *>> col_ref_map;
        compileFilter(
            executor_ctx_map[last_executor].output, ast_query.where_expression, filter, referred_columns, col_ref_map, properties.collator);
        executor_ctx_map.emplace(filter_exec, ExecutorCtx{last_executor, executor_ctx_map[last_executor].output, std::move(col_ref_map)});
        last_executor = filter_exec;
    }

    /// TopN.
    if (ast_query.order_expression_list && ast_query.limit_length)
    {
        tipb::Executor * topn_exec = dag_request.add_executors();
        topn_exec->set_tp(tipb::ExecType::TypeTopN);
        tipb::TopN * topn = topn_exec->mutable_topn();
        std::unordered_map<String, std::vector<tipb::Expr *>> col_ref_map;
        for (const auto & child : ast_query.order_expression_list->children)
        {
            ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
            if (!elem)
                throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
            tipb::ByItem * by = topn->add_order_by();
            by->set_desc(elem->direction < 0);
            tipb::Expr * expr = by->mutable_expr();
            compileExpr(
                executor_ctx_map[last_executor].output, elem->children[0], expr, referred_columns, col_ref_map, properties.collator);
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
        executor_ctx_map.emplace(limit_exec,
            ExecutorCtx{last_executor, executor_ctx_map[last_executor].output, std::unordered_map<String, std::vector<tipb::Expr *>>{}});
        last_executor = limit_exec;
    }

    /// Column pruner.
    std::function<void(ExecutorCtx &)> column_pruner = [&](ExecutorCtx & executor_ctx) {
        if (!executor_ctx.input)
        {
            executor_ctx.output.erase(std::remove_if(executor_ctx.output.begin(), executor_ctx.output.end(),
                                          [&](const auto & field) { return referred_columns.count(field.first) == 0; }),
                executor_ctx.output.end());

            for (const auto & info : executor_ctx.output)
            {
                tipb::ColumnInfo * ci = ts->add_columns();
                if (info.first == MutableSupport::tidb_pk_column_name)
                    ci->set_column_id(-1);
                else
                    ci->set_column_id(table_info.getColumnID(info.first));
                ci->set_tp(info.second.tp);
                ci->set_flag(info.second.flag);
                ci->set_columnlen(info.second.flen);
                ci->set_decimal(info.second.decimal);
                if (!info.second.elems.empty())
                {
                    for (auto & pair : info.second.elems)
                    {
                        ci->add_elems(pair.first);
                    }
                }
            }

            return;
        }
        column_pruner(executor_ctx_map[executor_ctx.input]);
        const auto & last_output = executor_ctx_map[executor_ctx.input].output;
        for (const auto & pair : executor_ctx.col_ref_map)
        {
            auto iter = std::find_if(last_output.begin(), last_output.end(), [&](const auto & field) { return field.first == pair.first; });
            if (iter == last_output.end())
                throw Exception("Column not found when pruning: " + pair.first, ErrorCodes::LOGICAL_ERROR);
            std::stringstream ss;
            encodeDAGInt64(iter - last_output.begin(), ss);
            auto s_val = ss.str();
            for (auto * expr : pair.second)
                expr->set_val(s_val);
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
                throw Exception("Limit/TopN and Agg cannot co-exist.", ErrorCodes::LOGICAL_ERROR);

            tipb::Executor * agg_exec = dag_request.add_executors();
            agg_exec->set_tp(tipb::ExecType::TypeAggregation);
            tipb::Aggregation * agg = agg_exec->mutable_aggregation();
            std::unordered_map<String, std::vector<tipb::Expr *>> col_ref_map;
            for (const auto & expr : ast_query.select_expression_list->children)
            {
                const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
                if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
                    throw Exception("Only agg function is allowed in select for a query with aggregation", ErrorCodes::LOGICAL_ERROR);

                tipb::Expr * agg_func = agg->add_agg_func();

                for (const auto & arg : func->arguments->children)
                {
                    tipb::Expr * arg_expr = agg_func->add_children();
                    compileExpr(executor_ctx_map[last_executor].output, arg, arg_expr, referred_columns, col_ref_map, properties.collator);
                }

                if (func->name == "count")
                {
                    agg_func->set_tp(tipb::Count);
                    auto ft = agg_func->mutable_field_type();
                    ft->set_tp(TiDB::TypeLongLong);
                    ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
                }
                else if (func->name == "max")
                {
                    agg_func->set_tp(tipb::Max);
                    if (agg_func->children_size() != 1)
                        throw Exception("udaf max only accept 1 argument");
                    auto ft = agg_func->mutable_field_type();
                    ft->set_tp(agg_func->children(0).field_type().tp());
                    ft->set_collate(properties.collator);
                }
                else if (func->name == "min")
                {
                    agg_func->set_tp(tipb::Min);
                    if (agg_func->children_size() != 1)
                        throw Exception("udaf min only accept 1 argument");
                    auto ft = agg_func->mutable_field_type();
                    ft->set_tp(agg_func->children(0).field_type().tp());
                    ft->set_collate(properties.collator);
                }
                else if (func->name == UniqRawResName)
                {
                    agg_func->set_tp(tipb::ApproxCountDistinct);
                    auto ft = agg_func->mutable_field_type();
                    ft->set_tp(TiDB::TypeString);
                    ft->set_flag(1);
                    func_wrap_output_stream
                        = [](BlockInputStreamPtr in) { return std::make_shared<UniqRawResReformatBlockOutputStream>(in); };
                }
                // TODO: Other agg func.
                else
                {
                    throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
                }

                schema.emplace_back(std::make_pair(func->getColumnName(), fieldTypeToColumnInfo(agg_func->field_type())));
            }

            if (has_gby)
            {
                for (const auto & child : ast_query.group_expression_list->children)
                {
                    tipb::Expr * gby = agg->add_group_by();
                    compileExpr(executor_ctx_map[last_executor].output, child, gby, referred_columns, col_ref_map, properties.collator);
                    schema.emplace_back(std::make_pair(child->getColumnName(), fieldTypeToColumnInfo(gby->field_type())));
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
                throw Exception("Unsupported expression type in select", ErrorCodes::LOGICAL_ERROR);
            }
        }

        column_pruner(executor_ctx_map[last_executor]);

        const auto & last_output = executor_ctx_map[last_executor].output;

        for (const auto & field : final_output)
        {
            auto iter
                = std::find_if(last_output.begin(), last_output.end(), [&](const auto & last_field) { return last_field.first == field; });
            if (iter == last_output.end())
                throw Exception("Column not found after pruning: " + field, ErrorCodes::LOGICAL_ERROR);
            dag_request.add_output_offsets(iter - last_output.begin());
            schema.push_back(*iter);
        }
    }

    return std::make_tuple(table_info.id, std::move(schema), std::move(dag_request), func_wrap_output_stream);
}

tipb::SelectResponse executeDAGRequest(Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version,
    UInt64 region_conf_version, Timestamp start_ts, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges)
{
    static Logger * log = &Logger::get("MockDAG");
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
    tipb::SelectResponse dag_response;
    std::unordered_map<RegionID, RegionInfo> regions;
    regions.emplace(region_id, RegionInfo(region_id, region_version, region_conf_version, std::move(key_ranges), nullptr));
    DAGDriver driver(context, dag_request, regions, start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
    driver.execute();
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle DAG request done");
    return dag_response;
}

std::unique_ptr<ChunkCodec> getCodec(tipb::EncodeType encode_type)
{
    switch (encode_type)
    {
        case tipb::EncodeType::TypeDefault:
            return std::make_unique<DefaultChunkCodec>();
        case tipb::EncodeType::TypeChunk:
            return std::make_unique<ArrowChunkCodec>();
        case tipb::EncodeType::TypeCHBlock:
            return std::make_unique<CHBlockChunkCodec>();
        default:
            throw Exception("Unsupported encode type", ErrorCodes::BAD_ARGUMENTS);
    }
}

void chunksToBlocks(const DAGSchema & schema, const tipb::SelectResponse & dag_response, BlocksList & blocks)
{
    auto codec = getCodec(dag_response.encode_type());
    for (const auto & chunk : dag_response.chunks())
        blocks.emplace_back(codec->decode(chunk, schema));
}

BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    if (dag_response.has_error())
        throw Exception(dag_response.error().msg(), dag_response.error().code());

    BlocksList blocks;
    chunksToBlocks(schema, dag_response, blocks);
    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}

} // namespace DB
