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

#include <utility>

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
static const String MPP_QUERY = "mpp_query";
static const String USE_BROADCAST_JOIN = "use_broadcast_join";
static const String MPP_PARTITION_NUM = "mpp_partition_num";

struct DAGProperties
{
    String encode_type = "";
    Int64 tz_offset = 0;
    String tz_name = "";
    Int32 collator = 0;
    bool is_mpp_query = false;
    bool use_broadcast_join = false;
    Int32 mpp_partition_num = 1;
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
    {"cast_int_string", tipb::ScalarFuncSig::CastIntAsString},
    {"cast_real_string", tipb::ScalarFuncSig::CastRealAsString},
    {"cast_decimal_string", tipb::ScalarFuncSig::CastDecimalAsString},
    {"cast_time_string", tipb::ScalarFuncSig::CastTimeAsString},
    {"cast_string_string", tipb::ScalarFuncSig::CastStringAsString},
    {"cast_int_date", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_date", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_date", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_date", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_date", tipb::ScalarFuncSig::CastStringAsTime},
    {"cast_int_datetime", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_datetime", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_datetime", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_datetime", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_datetime", tipb::ScalarFuncSig::CastStringAsTime},

});

enum QueryTaskType
{
    DAG,
    MPP_ESTABLISH_CONNECTION,
    MPP_DISPATCH
};

struct QueryTask
{
    std::shared_ptr<tipb::DAGRequest> dag_request;
    TableID table_id;
    DAGSchema result_schema;
    QueryTaskType type;
    QueryTask(std::shared_ptr<tipb::DAGRequest> request, TableID table_id_, const DAGSchema & result_schema_, QueryTaskType type_)
        : dag_request(std::move(request)), table_id(table_id_), result_schema(result_schema_), type(type_)
    {}
};

using QueryTasks = std::vector<QueryTask>;

using MakeResOutputStream = std::function<BlockInputStreamPtr(BlockInputStreamPtr)>;

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
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
    if (properties.find(MPP_QUERY) != properties.end())
        ret.is_mpp_query = properties[MPP_QUERY] == "true";
    if (properties.find(USE_BROADCAST_JOIN) != properties.end())
        ret.use_broadcast_join = properties[USE_BROADCAST_JOIN] == "true";
    if (properties.find(MPP_PARTITION_NUM) != properties.end())
        ret.mpp_partition_num = std::stoi(properties[USE_BROADCAST_JOIN]);

    return ret;
}

BlockInputStreamPtr executeQuery(Context & context, RegionID region_id, Timestamp start_ts, const DAGProperties & properties,
    QueryTasks & query_fragments, MakeResOutputStream & func_wrap_output_stream)
{
    if (properties.is_mpp_query)
    {
        throw Exception("mpp query not support yet");
    }
    else
    {
        auto & query_fragment = query_fragments[0];
        auto table_id = query_fragment.table_id;
        RegionPtr region;
        if (region_id == InvalidRegionID)
        {
            auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
            if (regions.empty())
                throw Exception("No region for table", ErrorCodes::BAD_ARGUMENTS);
            region = regions[0].second;
            region_id = regions[0].first;
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
        tipb::SelectResponse dag_response = executeDAGRequest(
            context, *query_fragment.dag_request, region_id, region->version(), region->confVer(), start_ts, key_ranges);

        return func_wrap_output_stream(outputDAGResponse(context, query_fragment.result_schema, dag_response));
    }
}

BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args)
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

    auto [query_fragments, func_wrap_output_stream] = compileQuery(
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

    return executeQuery(context, region_id, start_ts, properties, query_fragments, func_wrap_output_stream);
}

BlockInputStreamPtr dbgFuncMockTiDBQuery(Context & context, const ASTs & args)
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

    auto [query_fragments, func_wrap_output_stream] = compileQuery(
        context, query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        properties);

    return executeQuery(context, region_id, start_ts, properties, query_fragments, func_wrap_output_stream);
}

void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, uint32_t collator_id)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) { return field.first == id->getColumnName(); });
        if (ft == input.end())
            throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        expr->set_tp(tipb::ColumnRef);
        *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);
        std::stringstream ss;
        encodeDAGInt64(ft - input.begin(), ss);
        auto s_val = ss.str();
        expr->set_val(s_val);
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            /// aggregation function is handled in Aggregation, so just treated as a column
            auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) { return field.first == id->getColumnName(); });
            if (ft == input.end())
                throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
            expr->set_tp(tipb::ColumnRef);
            *(expr->mutable_field_type()) = columnInfoToFieldType((*ft).second);
            std::stringstream ss;
            encodeDAGInt64(ft - input.begin(), ss);
            auto s_val = ss.str();
            expr->set_val(s_val);
            return;
        }
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
                            astToPB(input, c, child, collator_id);
                        }
                    }
                    else
                    {
                        tipb::Expr * child = in_expr->add_children();
                        astToPB(input, child_ast, child, collator_id);
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
                    astToPB(input, child_ast, child, collator_id);
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
                    astToPB(input, child_ast, child, collator_id);
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
            case tipb::ScalarFuncSig::CastIntAsTime:
            case tipb::ScalarFuncSig::CastRealAsTime:
            case tipb::ScalarFuncSig::CastTimeAsTime:
            case tipb::ScalarFuncSig::CastDecimalAsTime:
            case tipb::ScalarFuncSig::CastStringAsTime:
            {
                expr->set_sig(it_sig->second);
                auto * ft = expr->mutable_field_type();
                if (it_sig->first.find("datetime"))
                {
                    ft->set_tp(TiDB::TypeDatetime);
                }
                else
                {
                    ft->set_tp(TiDB::TypeDate);
                }
                break;
            }
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
            astToPB(input, child_ast, child, collator_id);
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

void collectUsedColumnsFromExpr(const DAGSchema & input, ASTPtr ast, std::unordered_set<String> & used_columns)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        used_columns.emplace(id->getColumnName());
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            used_columns.emplace(func->getColumnName());
        }
        else
        {
            /// check function
            for (const auto & child_ast : func->arguments->children)
            {
                collectUsedColumnsFromExpr(input, child_ast, used_columns);
            }
        }
    }
}

struct Executor
{
    size_t index;
    DAGSchema output_schema;
    std::vector<std::shared_ptr<Executor>> children;
    virtual void columnPrune(std::unordered_set<String> & used_columns) = 0;
    virtual std::unordered_set<String> * getReferredColumns() { throw Exception("Should not reach here"); }
    Executor(size_t & index_, const DAGSchema & output_schema_) : index(index_), output_schema(output_schema_) { index_++; }
    virtual bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) = 0;
    virtual ~Executor() {}
};

struct TableScan : public Executor
{
    TableInfo table_info;
    /// used by column pruner
    TableScan(size_t & index_, const DAGSchema & output_schema_, TableInfo & table_info_)
        : Executor(index_, output_schema_), table_info(table_info_)
    {}
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(),
                                [&](const auto & field) { return used_columns.count(field.first) == 0; }),
            output_schema.end());
    }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTableScan);
        tipb_executor->set_executor_id("table_scan_" + std::to_string(index));
        auto * ts = tipb_executor->mutable_tbl_scan();
        ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
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
        return true;
    }
};

struct Selection : public Executor
{
    std::vector<ASTPtr> conditions;
    Selection(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && conditions_)
        : Executor(index_, output_schema_), conditions(std::move(conditions_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeSelection);
        tipb_executor->set_executor_id("selection_" + std::to_string(index));
        auto * sel = tipb_executor->mutable_selection();
        for (auto & expr : conditions)
        {
            tipb::Expr * cond = sel->add_conditions();
            astToPB(children[0]->output_schema, expr, cond, collator_id);
        }
        auto * child_executor = sel->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        for (auto & expr : conditions)
            collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
        children[0]->columnPrune(used_columns);
        /// update output schema after column prune
        output_schema = children[0]->output_schema;
    }
};

struct TopN : public Executor
{
    std::vector<ASTPtr> order_columns;
    size_t limit;
    TopN(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && order_columns_, size_t limit_)
        : Executor(index_, output_schema_), order_columns(std::move(order_columns_)), limit(limit_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTopN);
        tipb_executor->set_executor_id("topn_" + std::to_string(index));
        tipb::TopN * topn = tipb_executor->mutable_topn();
        for (const auto & child : order_columns)
        {
            ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
            if (!elem)
                throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
            tipb::ByItem * by = topn->add_order_by();
            by->set_desc(elem->direction < 0);
            tipb::Expr * expr = by->mutable_expr();
            astToPB(children[0]->output_schema, elem->children[0], expr, collator_id);
        }
        topn->set_limit(limit);
        auto * child_executor = topn->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        for (auto & expr : order_columns)
            collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_columns);
        children[0]->columnPrune(used_columns);
        /// update output schema after column prune
        output_schema = children[0]->output_schema;
    }
};

struct Limit : public Executor
{
    size_t limit;
    Limit(size_t & index_, const DAGSchema & output_schema_, size_t limit_) : Executor(index_, output_schema_), limit(limit_) {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeLimit);
        tipb_executor->set_executor_id("limit_" + std::to_string(index));
        tipb::Limit * lt = tipb_executor->mutable_limit();
        lt->set_limit(limit);
        auto * child_executor = lt->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        children[0]->columnPrune(used_columns);
        /// update output schema after column prune
        output_schema = children[0]->output_schema;
    }
};

struct Aggregation : public Executor
{
    bool has_uniq_raw_res;
    bool need_append_project;
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    Aggregation(size_t & index_, const DAGSchema & output_schema_, bool has_uniq_raw_res_, bool need_append_project_,
        std::vector<ASTPtr> && agg_exprs_, std::vector<ASTPtr> && gby_exprs_)
        : Executor(index_, output_schema_),
          has_uniq_raw_res(has_uniq_raw_res_),
          need_append_project(need_append_project_),
          agg_exprs(std::move(agg_exprs_)),
          gby_exprs(std::move(gby_exprs_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeAggregation);
        tipb_executor->set_executor_id("aggregation_" + std::to_string(index));
        auto * agg = tipb_executor->mutable_aggregation();
        auto & input_schema = children[0]->output_schema;
        for (const auto & expr : agg_exprs)
        {
            const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
            if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
                throw Exception("Only agg function is allowed in select for a query with aggregation", ErrorCodes::LOGICAL_ERROR);

            tipb::Expr * agg_func = agg->add_agg_func();

            for (const auto & arg : func->arguments->children)
            {
                tipb::Expr * arg_expr = agg_func->add_children();
                astToPB(input_schema, arg, arg_expr, collator_id);
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
                ft->set_collate(collator_id);
            }
            else if (func->name == "min")
            {
                agg_func->set_tp(tipb::Min);
                if (agg_func->children_size() != 1)
                    throw Exception("udaf min only accept 1 argument");
                auto ft = agg_func->mutable_field_type();
                ft->set_tp(agg_func->children(0).field_type().tp());
                ft->set_collate(collator_id);
            }
            else if (func->name == UniqRawResName)
            {
                agg_func->set_tp(tipb::ApproxCountDistinct);
                auto ft = agg_func->mutable_field_type();
                ft->set_tp(TiDB::TypeString);
                ft->set_flag(1);
            }
            // TODO: Other agg func.
            else
            {
                throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
            }
        }

        for (const auto & child : gby_exprs)
        {
            tipb::Expr * gby = agg->add_group_by();
            astToPB(input_schema, child, gby, collator_id);
        }

        auto * child_executor = agg->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(),
                                [&](const auto & field) { return used_columns.count(field.first) == 0; }),
            output_schema.end());
        std::unordered_set<String> used_input_columns;
        for (auto & func : agg_exprs)
        {
            if (used_columns.find(func->getColumnName()) != used_columns.end())
            {
                const ASTFunction * agg_func = typeid_cast<const ASTFunction *>(func.get());
                if (agg_func != nullptr)
                {
                    /// agg_func should not be nullptr, just double check
                    for (auto & child : agg_func->arguments->children)
                        collectUsedColumnsFromExpr(children[0]->output_schema, child, used_input_columns);
                }
            }
        }
        for (auto & gby_expr : gby_exprs)
        {
            collectUsedColumnsFromExpr(children[0]->output_schema, gby_expr, used_input_columns);
        }
        children[0]->columnPrune(used_input_columns);
    }
};

struct Project : public Executor
{
    std::vector<ASTPtr> exprs;
    Project(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && exprs_)
        : Executor(index_, output_schema_), exprs(std::move(exprs_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeProjection);
        tipb_executor->set_executor_id("project_" + std::to_string(index));
        auto * proj = tipb_executor->mutable_projection();
        auto & input_schema = children[0]->output_schema;
        for (const auto & child : exprs)
        {
            if (typeid_cast<ASTAsterisk *>(child.get()))
            {
                /// special case, select *
                for (size_t i = 0; i < input_schema.size(); i++)
                {
                    tipb::Expr * expr = proj->add_exprs();
                    expr->set_tp(tipb::ColumnRef);
                    *(expr->mutable_field_type()) = columnInfoToFieldType(input_schema[i].second);
                    std::stringstream ss;
                    encodeDAGInt64(i, ss);
                    auto s_val = ss.str();
                    expr->set_val(s_val);
                }
                continue;
            }
            tipb::Expr * expr = proj->add_exprs();
            astToPB(input_schema, child, expr, collator_id);
        }
        auto * children_executor = proj->mutable_child();
        return children[0]->toTiPBExecutor(children_executor, collator_id);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(),
                                [&](const auto & field) { return used_columns.count(field.first) == 0; }),
            output_schema.end());
        std::unordered_set<String> used_input_columns;
        for (auto & expr : exprs)
        {
            if (typeid_cast<ASTAsterisk *>(expr.get()))
            {
                /// for select *, just add all its input columns, maybe
                /// can do some optimization, but it is not worth for mock
                /// tests
                for (auto & field : children[0]->output_schema)
                {
                    used_input_columns.emplace(field.first);
                }
                break;
            }
            if (used_columns.find(expr->getColumnName()) != used_columns.end())
            {
                collectUsedColumnsFromExpr(children[0]->output_schema, expr, used_input_columns);
            }
        }
        children[0]->columnPrune(used_input_columns);
    }
};

struct ExchangeSender : Executor
{
    tipb::ExchangeType type;
    std::vector<size_t> partition_keys;
    ExchangeSender(size_t & index, const DAGSchema & output, tipb::ExchangeType type_, std::vector<size_t> && partition_keys_)
        : Executor(index, output), type(type_), partition_keys(std::move(partition_keys_))
    {}
    void columnPrune(std::unordered_set<String> & used_columns) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeExchangeSender);
        tipb_executor->set_executor_id("exchange_sender_" + std::to_string(index));
        tipb::ExchangeSender * exchange_sender = tipb_executor->mutable_exchange_sender();
        exchange_sender->set_tp(type);
        for (auto i : partition_keys)
        {
            auto * expr = exchange_sender->add_partition_keys();
            expr->set_tp(tipb::Int64);
            std::stringstream ss;
            encodeDAGInt64(i, ss);
            expr->set_val(ss.str());
        }
        // todo add task_meta
        auto * child_executor = exchange_sender->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id);
    }
};

struct ExchangeReceiver : Executor
{
    void columnPrune(std::unordered_set<String> & used_columns) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeExchangeReceiver);
        tipb_executor->set_executor_id("exchange_receiver_" + std::to_string(index));
        tipb::ExchangeReceiver * exchange_receiver = tipb_executor->mutable_exchange_receiver();
        for (auto & field : output_schema)
        {
            auto * field_type = exchange_receiver->add_field_types();
            field_type->set_tp(field.second.tp);
            field_type->set_flag(field.second.flag);
            field_type->set_flen(field.second.flen);
            field_type->set_decimal(field.second.decimal);
        }
        // todo add task_meta
        return true;
    }
};

using ExecutorPtr = std::shared_ptr<Executor>;

TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast)
{
    TiDB::ColumnInfo ci;
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        /// check column
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) { return field.first == id->getColumnName(); });
        if (ft == input.end())
            throw Exception("No such column " + id->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        ci = ft->second;
    }
    else if (ASTFunction * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        /// check function
        String func_name_lowercase = Poco::toLower(func->name);
        const auto it_sig = func_name_to_sig.find(func_name_lowercase);
        if (it_sig == func_name_to_sig.end())
        {
            throw Exception("Unsupported function: " + func_name_lowercase, ErrorCodes::LOGICAL_ERROR);
        }
        switch (it_sig->second)
        {
            case tipb::ScalarFuncSig::InInt:
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned;
                for (const auto & child_ast : func->arguments->children)
                {
                    auto * tuple_func = typeid_cast<ASTFunction *>(child_ast.get());
                    if (tuple_func != nullptr && tuple_func->name == "tuple")
                    {
                        // flatten tuple elements
                        for (const auto & c : tuple_func->arguments->children)
                        {
                            compileExpr(input, c);
                        }
                    }
                    else
                    {
                        compileExpr(input, child_ast);
                    }
                }
                return ci;
            case tipb::ScalarFuncSig::IfInt:
            case tipb::ScalarFuncSig::BitAndSig:
            case tipb::ScalarFuncSig::BitOrSig:
            case tipb::ScalarFuncSig::BitXorSig:
            case tipb::ScalarFuncSig::BitNegSig:
                for (size_t i = 0; i < func->arguments->children.size(); i++)
                {
                    const auto & child_ast = func->arguments->children[i];
                    auto child_ci = compileExpr(input, child_ast);
                    // todo should infer the return type based on all input types
                    if ((it_sig->second == tipb::ScalarFuncSig::IfInt && i == 1)
                        || (it_sig->second != tipb::ScalarFuncSig::IfInt && i == 0))
                        ci = child_ci;
                }
                return ci;
            case tipb::ScalarFuncSig::LikeSig:
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned;
                for (const auto & child_ast : func->arguments->children)
                {
                    compileExpr(input, child_ast);
                }
                return ci;
            case tipb::ScalarFuncSig::FromUnixTime2Arg:
                if (func->arguments->children.size() == 1)
                {
                    ci.tp = TiDB::TypeDatetime;
                    ci.decimal = 6;
                }
                else
                {
                    ci.tp = TiDB::TypeString;
                }
                break;
            case tipb::ScalarFuncSig::DateFormatSig:
                ci.tp = TiDB::TypeString;
                break;
            case tipb::ScalarFuncSig::CastIntAsTime:
            case tipb::ScalarFuncSig::CastRealAsTime:
            case tipb::ScalarFuncSig::CastTimeAsTime:
            case tipb::ScalarFuncSig::CastDecimalAsTime:
            case tipb::ScalarFuncSig::CastStringAsTime:
                if (it_sig->first.find("datetime"))
                {
                    ci.tp = TiDB::TypeDatetime;
                }
                else
                {
                    ci.tp = TiDB::TypeDate;
                }
                break;
            default:
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned;
                break;
        }
        for (const auto & child_ast : func->arguments->children)
        {
            compileExpr(input, child_ast);
        }
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        switch (lit->value.getType())
        {
            case Field::Types::Which::Null:
                ci.tp = TiDB::TypeNull;
                // Null literal expr doesn't need value.
                break;
            case Field::Types::Which::UInt64:
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned;
                break;
            case Field::Types::Which::Int64:
                ci.tp = TiDB::TypeLongLong;
                break;
            case Field::Types::Which::Float64:
                ci.tp = TiDB::TypeDouble;
                break;
            case Field::Types::Which::Decimal32:
            case Field::Types::Which::Decimal64:
            case Field::Types::Which::Decimal128:
            case Field::Types::Which::Decimal256:
                ci.tp = TiDB::TypeNewDecimal;
                break;
            case Field::Types::Which::String:
                ci.tp = TiDB::TypeString;
                break;
            default:
                throw Exception(String("Unsupported literal type: ") + lit->value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
        }
    }
    else
    {
        /// not supported unless this is a literal
        throw Exception("Unsupported expression " + ast->getColumnName(), ErrorCodes::LOGICAL_ERROR);
    }
    return ci;
}

void compileFilter(const DAGSchema & input, ASTPtr ast, std::vector<ASTPtr> & conditions)
{
    if (auto * func = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (func->name == "and")
        {
            for (auto & child : func->arguments->children)
            {
                compileFilter(input, child, conditions);
            }
            return;
        }
    }
    conditions.push_back(ast);
    compileExpr(input, ast);
}

ExecutorPtr compileTableScan(size_t & executor_index, TableInfo & table_info, bool append_pk_column)
{
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
    if (append_pk_column)
    {
        ColumnInfo ci;
        ci.tp = TiDB::TypeLongLong;
        ci.setPriKeyFlag();
        ci.setNotNullFlag();
        ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
    }
    return std::make_shared<TableScan>(executor_index, ts_output, table_info);
}

ExecutorPtr compileSelection(ExecutorPtr input, size_t & executor_index, ASTPtr filter)
{
    std::vector<ASTPtr> conditions;
    compileFilter(input->output_schema, filter, conditions);
    auto selection = std::make_shared<Selection>(executor_index, input->output_schema, std::move(conditions));
    selection->children.push_back(input);
    return selection;
}

ExecutorPtr compileTopN(ExecutorPtr input, size_t & executor_index, ASTPtr order_exprs, ASTPtr limit_expr)
{
    std::vector<ASTPtr> order_columns;
    for (const auto & child : order_exprs->children)
    {
        ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
        if (!elem)
            throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
        order_columns.push_back(child);
        compileExpr(input->output_schema, elem->children[0]);
    }
    auto limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto topN = std::make_shared<TopN>(executor_index, input->output_schema, std::move(order_columns), limit);
    topN->children.push_back(input);
    return topN;
}

ExecutorPtr compileLimit(ExecutorPtr input, size_t & executor_index, ASTPtr limit_expr)
{
    auto limit_length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto limit = std::make_shared<Limit>(executor_index, input->output_schema, limit_length);
    limit->children.push_back(input);
    return limit;
}

ExecutorPtr compileAggregation(ExecutorPtr input, size_t & executor_index, ASTPtr agg_funcs, ASTPtr group_by_exprs)
{
    std::vector<ASTPtr> agg_exprs;
    std::vector<ASTPtr> gby_exprs;
    DAGSchema output_schema;
    bool has_uniq_raw_res = false;
    bool need_append_project = false;
    if (agg_funcs != nullptr)
    {
        for (const auto & expr : agg_funcs->children)
        {
            const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
            if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                need_append_project = true;
                continue;
            }

            agg_exprs.push_back(expr);
            std::vector<TiDB::ColumnInfo> children_ci;

            for (const auto & arg : func->arguments->children)
            {
                children_ci.push_back(compileExpr(input->output_schema, arg));
            }

            TiDB::ColumnInfo ci;
            if (func->name == "count")
            {
                ci.tp = TiDB::TypeLongLong;
                ci.flag = TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull;
            }
            else if (func->name == "max" || func->name == "min")
            {
                ci = children_ci[0];
            }
            else if (func->name == UniqRawResName)
            {
                has_uniq_raw_res = true;
                ci.tp = TiDB::TypeString;
                ci.flag = 1;
            }
            // TODO: Other agg func.
            else
            {
                throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
            }

            output_schema.emplace_back(std::make_pair(func->getColumnName(), ci));
        }
    }

    if (group_by_exprs != nullptr)
    {
        for (const auto & child : group_by_exprs->children)
        {
            gby_exprs.push_back(child);
            auto ci = compileExpr(input->output_schema, child);
            output_schema.emplace_back(std::make_pair(child->getColumnName(), ci));
        }
    }

    auto aggregation = std::make_shared<Aggregation>(
        executor_index, output_schema, has_uniq_raw_res, need_append_project, std::move(agg_exprs), std::move(gby_exprs));
    aggregation->children.push_back(input);
    return aggregation;
}

ExecutorPtr compileProject(ExecutorPtr input, size_t & executor_index, ASTPtr select_list)
{
    std::vector<ASTPtr> exprs;
    DAGSchema output_schema;
    for (const auto & expr : select_list->children)
    {
        if (typeid_cast<ASTAsterisk *>(expr.get()))
        {
            /// special case, select *
            exprs.push_back(expr);
            const auto & last_output = input->output_schema;
            for (const auto & field : last_output)
            {
                output_schema.emplace_back(field.first, field.second);
            }
        }
        else
        {
            exprs.push_back(expr);
            const ASTFunction * func = typeid_cast<const ASTFunction *>(expr.get());
            if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                auto ft = std::find_if(input->output_schema.begin(), input->output_schema.end(),
                    [&](const auto & field) { return field.first == func->getColumnName(); });
                if (ft == input->output_schema.end())
                    throw Exception("No such agg " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
                output_schema.emplace_back(ft->first, ft->second);
            }
            else
            {
                auto ci = compileExpr(input->output_schema, expr);
                output_schema.emplace_back(std::make_pair(expr->getColumnName(), ci));
            }
        }
    }

    auto project = std::make_shared<Project>(executor_index, output_schema, std::move(exprs));
    project->children.push_back(input);
    return project;
}

struct QueryFragment
{
    ExecutorPtr root_executor;
    TableID table_id;
    bool is_top_fragment;
    QueryFragment(ExecutorPtr root_executor_, TableID table_id_, bool is_top_fragment_)
        : root_executor(std::move(root_executor_)), table_id(table_id_), is_top_fragment(is_top_fragment_)
    {}
    QueryTask toQueryTask(const DAGProperties & properties)
    {
        std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
        tipb::DAGRequest & dag_request = *dag_request_ptr;
        dag_request.set_time_zone_name(properties.tz_name);
        dag_request.set_time_zone_offset(properties.tz_offset);
        dag_request.set_flags(dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));

        if (is_top_fragment)
        {
            if (properties.encode_type == "chunk")
                dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
            else if (properties.encode_type == "chblock")
                dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
            else
                dag_request.set_encode_type(tipb::EncodeType::TypeDefault);
        }
        else
        {
            dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
        }

        for (size_t i = 0; i < root_executor->output_schema.size(); i++)
            dag_request.add_output_offsets(i);
        auto * root_tipb_executor = dag_request.mutable_root_executor();
        root_executor->toTiPBExecutor(root_tipb_executor, properties.collator);

        return QueryTask(dag_request_ptr, table_id, root_executor->output_schema, properties.is_mpp_query ? MPP_DISPATCH : DAG);
        //QueryTask ret(dag_request_ptr, table_id, root_executor->output_schema, properties.is_mpp_query ? MPP_DISPATCH : DAG);
        //return ret;
    }
};

using QueryFragments = std::vector<QueryFragment>;

QueryFragments queryPlanToQueryFragments(const DAGProperties & properties, ExecutorPtr root_executor)
{
    QueryFragments fragments;
    if (properties.is_mpp_query)
    {
        throw Exception("MPP not supported yet");
    }
    else
    {
        ExecutorPtr current_executor = root_executor;
        while (!current_executor->children.empty())
        {
            if (current_executor->children.size() > 1)
                throw Exception("Join is not supported in non mpp mode");
            current_executor = current_executor->children[0];
        }
        auto * ts = dynamic_cast<TableScan *>(current_executor.get());
        if (ts == nullptr)
            throw Exception("Only table scan is supported as the source executor in non mpp mode");
        fragments.emplace_back(root_executor, ts->table_info.id, true);
    }
    return fragments;
}


QueryTasks queryPlanToQueryTasks(const DAGProperties & properties, ExecutorPtr root_executor)
{
    QueryFragments fragments = queryPlanToQueryFragments(properties, root_executor);
    QueryTasks tasks;
    for (auto & fragment : fragments)
        tasks.emplace_back(fragment.toQueryTask(properties));
    return tasks;
}

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
    Context & context, const String & query, SchemaFetcher schema_fetcher, const DAGProperties & properties)
{
    if (properties.is_mpp_query)
        throw Exception("mpp query is not supported yet");
    MakeResOutputStream func_wrap_output_stream = [](BlockInputStreamPtr in) { return in; };

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);
    size_t executor_index = 0;

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

    ExecutorPtr root_executor = nullptr;

    /// Table scan.
    {
        bool append_pk_column = false;
        for (const auto & expr : ast_query.select_expression_list->children)
        {
            if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                if (identifier->getColumnName() == MutableSupport::tidb_pk_column_name)
                {
                    append_pk_column = true;
                }
            }
        }
        root_executor = compileTableScan(executor_index, table_info, append_pk_column);
    }

    /// Filter.
    if (ast_query.where_expression)
    {
        root_executor = compileSelection(root_executor, executor_index, ast_query.where_expression);
    }

    /// TopN.
    if (ast_query.order_expression_list && ast_query.limit_length)
    {
        root_executor = compileTopN(root_executor, executor_index, ast_query.order_expression_list, ast_query.limit_length);
    }
    else if (ast_query.limit_length)
    {
        root_executor = compileLimit(root_executor, executor_index, ast_query.limit_length);
    }

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
    DAGSchema final_schema;
    /// Aggregation
    std::vector<size_t> output_offsets;
    if (has_gby || has_agg_func)
    {
        if (!properties.is_mpp_query
            && (dynamic_cast<Limit *>(root_executor.get()) != nullptr || dynamic_cast<TopN *>(root_executor.get()) != nullptr))
            throw Exception("Limit/TopN and Agg cannot co-exist in non-mpp mode.", ErrorCodes::LOGICAL_ERROR);

        root_executor = compileAggregation(
            root_executor, executor_index, ast_query.select_expression_list, has_gby ? ast_query.group_expression_list : nullptr);

        if (dynamic_cast<Aggregation *>(root_executor.get())->has_uniq_raw_res)
            func_wrap_output_stream = [](BlockInputStreamPtr in) { return std::make_shared<UniqRawResReformatBlockOutputStream>(in); };


        if (dynamic_cast<Aggregation *>(root_executor.get())->need_append_project)
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

    /// finalize
    std::unordered_set<String> used_columns;
    for (auto & schema : root_executor->output_schema)
        used_columns.emplace(schema.first);
    root_executor->columnPrune(used_columns);
    /// todo for mpp, add top level exchange sender

    return std::make_tuple(queryPlanToQueryTasks(properties, root_executor), func_wrap_output_stream);
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
