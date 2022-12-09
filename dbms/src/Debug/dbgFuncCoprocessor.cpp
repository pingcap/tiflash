#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/FieldToDataType.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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
static const String MPP_TIMEOUT = "mpp_timeout";
static String LOCAL_HOST = "127.0.0.1:3930";

namespace Debug
{
void setServiceAddr(const std::string & addr) { LOCAL_HOST = addr; }
} // namespace Debug

struct DAGProperties
{
    String encode_type = "";
    Int64 tz_offset = 0;
    String tz_name = "";
    Int32 collator = 0;
    bool is_mpp_query = false;
    bool use_broadcast_join = false;
    Int32 mpp_partition_num = 1;
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    Int32 mpp_timeout = 10;
};

std::unordered_map<String, tipb::ScalarFuncSig> func_name_to_sig({
    {"equals", tipb::ScalarFuncSig::EQInt},
    {"and", tipb::ScalarFuncSig::LogicalAnd},
    {"or", tipb::ScalarFuncSig::LogicalOr},
    {"xor", tipb::ScalarFuncSig::LogicalXor},
    {"not", tipb::ScalarFuncSig::UnaryNotInt},
    {"greater", tipb::ScalarFuncSig::GTInt},
    {"greaterorequals", tipb::ScalarFuncSig::GEInt},
    {"less", tipb::ScalarFuncSig::LTInt},
    {"lessorequals", tipb::ScalarFuncSig::LEInt},
    {"in", tipb::ScalarFuncSig::InInt},
    {"notin", tipb::ScalarFuncSig::InInt},
    {"date_format", tipb::ScalarFuncSig::DateFormatSig},
    {"if", tipb::ScalarFuncSig::IfInt},
    {"from_unixtime", tipb::ScalarFuncSig::FromUnixTime2Arg},
    /// bit_and/bit_or/bit_xor is aggregated function in clickhouse/mysql
    {"bitand", tipb::ScalarFuncSig::BitAndSig},
    {"bitor", tipb::ScalarFuncSig::BitOrSig},
    {"bitxor", tipb::ScalarFuncSig::BitXorSig},
    {"bitnot", tipb::ScalarFuncSig::BitNegSig},
    {"notequals", tipb::ScalarFuncSig::NEInt},
    {"like", tipb::ScalarFuncSig::LikeSig},
    {"cast_int_int", tipb::ScalarFuncSig::CastIntAsInt},
    {"cast_int_real", tipb::ScalarFuncSig::CastIntAsReal},
    {"cast_real_int", tipb::ScalarFuncSig::CastRealAsInt},
    {"cast_real_real", tipb::ScalarFuncSig::CastRealAsReal},
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

std::unordered_map<String, tipb::ExprType> agg_func_name_to_sig({
    {"min", tipb::ExprType::Min},
    {"max", tipb::ExprType::Max},
    {"count", tipb::ExprType::Count},
    {"sum", tipb::ExprType::Sum},
    {"first_row", tipb::ExprType::First},
    {"uniqRawRes", tipb::ExprType::ApproxCountDistinct},
    {"group_concat", tipb::ExprType::GroupConcat},
});

std::pair<String, String> splitQualifiedName(String s)
{
    std::pair<String, String> ret;
    Poco::StringTokenizer string_tokens(s, ".");
    if (string_tokens.count() == 1)
    {
        ret.second = s;
    }
    else if (string_tokens.count() == 2)
    {
        ret.first = string_tokens[0];
        ret.second = string_tokens[1];
    }
    else
    {
        throw Exception("Invalid identifier name");
    }
    return ret;
}

DAGColumnInfo toNullableDAGColumnInfo(DAGColumnInfo & input)
{
    DAGColumnInfo output = input;
    output.second.clearNotNullFlag();
    return output;
}


enum QueryTaskType
{
    DAG,
    MPP_DISPATCH
};

struct QueryTask
{
    std::shared_ptr<tipb::DAGRequest> dag_request;
    TableID table_id;
    DAGSchema result_schema;
    QueryTaskType type;
    Int64 task_id;
    Int64 partition_id;
    bool is_root_task;
    QueryTask(std::shared_ptr<tipb::DAGRequest> request, TableID table_id_, const DAGSchema & result_schema_, QueryTaskType type_,
        Int64 task_id_, Int64 partition_id_, bool is_root_task_)
        : dag_request(std::move(request)),
          table_id(table_id_),
          result_schema(result_schema_),
          type(type_),
          task_id(task_id_),
          partition_id(partition_id_),
          is_root_task(is_root_task_)
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
        ret.mpp_partition_num = std::stoi(properties[MPP_PARTITION_NUM]);
    if (properties.find(MPP_TIMEOUT) != properties.end())
        ret.mpp_timeout = std::stoi(properties[MPP_TIMEOUT]);

    return ret;
}

BlockInputStreamPtr executeQuery(Context & context, RegionID region_id, const DAGProperties & properties, QueryTasks & query_tasks,
    MakeResOutputStream & func_wrap_output_stream)
{
    if (properties.is_mpp_query)
    {
        DAGSchema root_task_schema;
        std::vector<Int64> root_task_ids;
        for (auto & task : query_tasks)
        {
            if (task.is_root_task)
            {
                root_task_ids.push_back(task.task_id);
                root_task_schema = task.result_schema;
            }
            auto req = std::make_shared<mpp::DispatchTaskRequest>();
            auto * tm = req->mutable_meta();
            tm->set_start_ts(properties.start_ts);
            tm->set_partition_id(task.partition_id);
            tm->set_address(LOCAL_HOST);
            tm->set_task_id(task.task_id);
            auto * encoded_plan = req->mutable_encoded_plan();
            task.dag_request->AppendToString(encoded_plan);
            req->set_timeout(properties.mpp_timeout);
            req->set_schema_ver(DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
            auto table_id = task.table_id;
            if (table_id != -1)
            {
                /// contains a table scan
                auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
                if (regions.size() < (size_t)properties.mpp_partition_num)
                    throw Exception("Not supported: table region num less than mpp partition num");
                for (size_t i = 0; i < regions.size(); i++)
                {
                    if (i % properties.mpp_partition_num != (size_t)task.partition_id)
                        continue;
                    auto * region = req->add_regions();
                    region->set_region_id(regions[i].first);
                    auto * meta = region->mutable_region_epoch();
                    meta->set_conf_ver(regions[i].second->confVer());
                    meta->set_version(regions[i].second->version());
                    auto * range = region->add_ranges();
                    auto handle_range = getHandleRangeByTable(regions[i].second->getRange()->rawKeys(), table_id);
                    range->set_start(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
                    range->set_end(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
                }
            }
            pingcap::kv::RpcCall<mpp::DispatchTaskRequest> call(req);
            context.getTMTContext().getCluster()->rpc_client->sendRequest(LOCAL_HOST, call, 1000);
            if (call.getResp()->has_error())
                throw Exception("Meet error while dispatch mpp task: " + call.getResp()->error().msg());
        }
        tipb::ExchangeReceiver tipb_exchange_receiver;
        for (const auto root_task_id : root_task_ids)
        {
            mpp::TaskMeta tm;
            tm.set_start_ts(properties.start_ts);
            tm.set_address(LOCAL_HOST);
            tm.set_task_id(root_task_id);
            tm.set_partition_id(-1);
            auto * tm_string = tipb_exchange_receiver.add_encoded_task_meta();
            tm.AppendToString(tm_string);
        }
        for (auto & field : root_task_schema)
        {
            auto tipb_type = TiDB::columnInfoToFieldType(field.second);
            tipb_type.set_collate(properties.collator);
            auto * field_type = tipb_exchange_receiver.add_field_types();
            *field_type = tipb_type;
        }
        mpp::TaskMeta root_tm;
        root_tm.set_start_ts(properties.start_ts);
        root_tm.set_address(LOCAL_HOST);
        root_tm.set_task_id(-1);
        root_tm.set_partition_id(-1);
        std::shared_ptr<ExchangeReceiver> exchange_receiver
            = std::make_shared<ExchangeReceiver>(context, tipb_exchange_receiver, root_tm, 10);
        BlockInputStreamPtr ret = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver);
        return ret;
    }
    else
    {
        auto & task = query_tasks[0];
        auto table_id = task.table_id;
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
            context, *task.dag_request, region_id, region->version(), region->confVer(), properties.start_ts, key_ranges);

        return func_wrap_output_stream(outputDAGResponse(context, task.result_schema, dag_response));
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
    properties.start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
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

    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
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
    properties.start_ts = start_ts;

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        context, query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        properties);

    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
}

void literalToPB(tipb::Expr * expr, const Field & value, uint32_t collator_id)
{
    std::stringstream ss;
    switch (value.getType())
    {
        case Field::Types::Which::Null:
        {
            expr->set_tp(tipb::Null);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeNull);
            ft->set_collate(collator_id);
            // Null literal expr doesn't need value.
            break;
        }
        case Field::Types::Which::UInt64:
        {
            expr->set_tp(tipb::Uint64);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeLongLong);
            ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
            ft->set_collate(collator_id);
            encodeDAGUInt64(value.get<UInt64>(), ss);
            break;
        }
        case Field::Types::Which::Int64:
        {
            expr->set_tp(tipb::Int64);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeLongLong);
            ft->set_flag(TiDB::ColumnFlagNotNull);
            ft->set_collate(collator_id);
            encodeDAGInt64(value.get<Int64>(), ss);
            break;
        }
        case Field::Types::Which::Float64:
        {
            expr->set_tp(tipb::Float64);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeFloat);
            ft->set_flag(TiDB::ColumnFlagNotNull);
            ft->set_collate(collator_id);
            encodeDAGFloat64(value.get<Float64>(), ss);
            break;
        }
        case Field::Types::Which::Decimal32:
        case Field::Types::Which::Decimal64:
        case Field::Types::Which::Decimal128:
        case Field::Types::Which::Decimal256:
        {
            expr->set_tp(tipb::MysqlDecimal);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeNewDecimal);
            ft->set_flag(TiDB::ColumnFlagNotNull);
            ft->set_collate(collator_id);
            encodeDAGDecimal(value, ss);
            break;
        }
        case Field::Types::Which::String:
        {
            expr->set_tp(tipb::String);
            auto * ft = expr->mutable_field_type();
            ft->set_tp(TiDB::TypeString);
            ft->set_flag(TiDB::ColumnFlagNotNull);
            ft->set_collate(collator_id);
            // TODO: Align with TiDB.
            encodeDAGBytes(value.get<String>(), ss);
            break;
        }
        default:
            throw Exception(String("Unsupported literal type: ") + value.getTypeName(), ErrorCodes::LOGICAL_ERROR);
    }
    expr->set_val(ss.str());
}

String getFunctionNameForConstantFolding(tipb::Expr * expr)
{
    // todo support more function for constant folding
    switch (expr->sig())
    {
        case tipb::ScalarFuncSig::CastStringAsTime:
            return "toMyDateTimeOrNull";
        default:
            return "";
    }
}

void foldConstant(tipb::Expr * expr, uint32_t collator_id, const Context & context)
{
    if (expr->tp() == tipb::ScalarFunc)
    {
        bool all_const = true;
        for (auto c : expr->children())
        {
            if (!isLiteralExpr(c))
            {
                all_const = false;
                break;
            }
        }
        if (!all_const)
            return;
        DataTypes arguments_types;
        ColumnsWithTypeAndName argument_columns;
        for (auto & c : expr->children())
        {
            Field value = decodeLiteral(c);
            DataTypePtr flash_type = applyVisitor(FieldToDataType(), value);
            DataTypePtr target_type = inferDataType4Literal(c);
            ColumnWithTypeAndName column;
            column.column = target_type->createColumnConst(1, convertFieldToType(value, *target_type, flash_type.get()));
            column.name = exprToString(c, {}) + "_" + target_type->getName();
            column.type = target_type;
            arguments_types.emplace_back(target_type);
            argument_columns.emplace_back(column);
        }
        auto func_name = getFunctionNameForConstantFolding(expr);
        if (func_name.empty())
            return;
        const auto & function_builder_ptr = FunctionFactory::instance().get(func_name, context);
        auto function_ptr = function_builder_ptr->build(argument_columns);
        if (function_ptr->isSuitableForConstantFolding())
        {
            Block block_with_constants(argument_columns);
            ColumnNumbers argument_numbers(arguments_types.size());
            for (size_t i = 0, size = arguments_types.size(); i < size; i++)
                argument_numbers[i] = i;
            size_t result_pos = argument_numbers.size();
            block_with_constants.insert({nullptr, function_ptr->getReturnType(), "result"});
            function_ptr->execute(block_with_constants, argument_numbers, result_pos);
            const auto & result_column = block_with_constants.getByPosition(result_pos).column;
            if (result_column->isColumnConst())
            {
                auto updated_value = (*result_column)[0];
                tipb::FieldType orig_field_type = expr->field_type();
                expr->Clear();
                literalToPB(expr, updated_value, collator_id);
                expr->clear_field_type();
                auto * field_type = expr->mutable_field_type();
                (*field_type) = orig_field_type;
            }
        }
    }
}

void astToPB(const DAGSchema & input, ASTPtr ast, tipb::Expr * expr, uint32_t collator_id, const Context & context)
{
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
            auto column_name = splitQualifiedName(id->getColumnName());
            auto field_name = splitQualifiedName(field.first);
            if (column_name.first.empty())
                return field_name.second == column_name.second;
            else
                return field_name.first == column_name.first && field_name.second == column_name.second;
        });
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
            auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
                auto column_name = splitQualifiedName(func->getColumnName());
                auto field_name = splitQualifiedName(field.first);
                if (column_name.first.empty())
                    return field_name.second == column_name.second;
                else
                    return field_name.first == column_name.first && field_name.second == column_name.second;
            });
            if (ft == input.end())
                throw Exception("No such column " + func->getColumnName(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
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
                            astToPB(input, c, child, collator_id, context);
                        }
                    }
                    else
                    {
                        tipb::Expr * child = in_expr->add_children();
                        astToPB(input, child_ast, child, collator_id, context);
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
                    astToPB(input, child_ast, child, collator_id, context);
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
                    astToPB(input, child_ast, child, collator_id, context);
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
            case tipb::ScalarFuncSig::CastIntAsReal:
            case tipb::ScalarFuncSig::CastRealAsReal:
            {
                expr->set_sig(it_sig->second);
                auto * ft = expr->mutable_field_type();
                ft->set_tp(TiDB::TypeDouble);
                ft->set_collate(collator_id);
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
            astToPB(input, child_ast, child, collator_id, context);
        }
        foldConstant(expr, collator_id, context);
    }
    else if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(ast.get()))
    {
        literalToPB(expr, lit->value, collator_id);
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
        auto column_name = splitQualifiedName(id->getColumnName());
        if (!column_name.first.empty())
            used_columns.emplace(id->getColumnName());
        else
        {
            bool found = false;
            for (auto & field : input)
            {
                auto field_name = splitQualifiedName(field.first);
                if (field_name.second == column_name.second)
                {
                    if (found)
                        throw Exception("ambiguous column for " + column_name.second);
                    found = true;
                    used_columns.emplace(field.first);
                }
            }
        }
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

struct MPPCtx
{
    Timestamp start_ts;
    Int64 next_task_id;
    std::vector<Int64> sender_target_task_ids;
    explicit MPPCtx(Timestamp start_ts_) : start_ts(start_ts_), next_task_id(1) {}
};

using MPPCtxPtr = std::shared_ptr<MPPCtx>;

struct MPPInfo
{
    Timestamp start_ts;
    Int64 partition_id;
    Int64 task_id;
    const std::vector<Int64> & sender_target_task_ids;
    const std::unordered_map<String, std::vector<Int64>> & receiver_source_task_ids_map;
    MPPInfo(Timestamp start_ts_, Int64 partition_id_, Int64 task_id_, const std::vector<Int64> & sender_target_task_ids_,
        const std::unordered_map<String, std::vector<Int64>> & receiver_source_task_ids_map_)
        : start_ts(start_ts_),
          partition_id(partition_id_),
          task_id(task_id_),
          sender_target_task_ids(sender_target_task_ids_),
          receiver_source_task_ids_map(receiver_source_task_ids_map_)
    {}
};

struct TaskMeta
{
    UInt64 start_ts = 0;
    Int64 task_id = 0;
    Int64 partition_id = 0;
};

using TaskMetas = std::vector<TaskMeta>;

namespace mock
{
struct ExchangeSender;
struct ExchangeReceiver;
struct Executor
{
    size_t index;
    String name;
    DAGSchema output_schema;
    std::vector<std::shared_ptr<Executor>> children;
    virtual void columnPrune(std::unordered_set<String> & used_columns) = 0;
    Executor(size_t & index_, String && name_, const DAGSchema & output_schema_)
        : index(index_), name(std::move(name_)), output_schema(output_schema_)
    {
        index_++;
    }
    virtual bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context)
        = 0;
    virtual void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties,
        std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map)
    {
        children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
    }
    virtual ~Executor() {}
};

struct ExchangeSender : Executor
{
    tipb::ExchangeType type;
    TaskMetas task_metas;
    std::vector<size_t> partition_keys;
    ExchangeSender(size_t & index, const DAGSchema & output, tipb::ExchangeType type_, const std::vector<size_t> & partition_keys_ = {})
        : Executor(index, "exchange_sender_" + std::to_string(index), output), type(type_), partition_keys(partition_keys_)
    {}
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeExchangeSender);
        tipb_executor->set_executor_id(name);
        tipb::ExchangeSender * exchange_sender = tipb_executor->mutable_exchange_sender();
        exchange_sender->set_tp(type);
        for (auto i : partition_keys)
        {
            auto * expr = exchange_sender->add_partition_keys();
            expr->set_tp(tipb::ColumnRef);
            std::stringstream ss;
            encodeDAGInt64(i, ss);
            expr->set_val(ss.str());
        }
        for (auto task_id : mpp_info.sender_target_task_ids)
        {
            mpp::TaskMeta meta;
            meta.set_start_ts(mpp_info.start_ts);
            meta.set_task_id(task_id);
            meta.set_partition_id(mpp_info.partition_id);
            meta.set_address(LOCAL_HOST);
            auto * meta_string = exchange_sender->add_encoded_task_meta();
            meta.AppendToString(meta_string);
        }
        auto * child_executor = exchange_sender->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
    }
};

struct ExchangeReceiver : Executor
{
    TaskMetas task_metas;
    ExchangeReceiver(size_t & index, const DAGSchema & output) : Executor(index, "exchange_receiver_" + std::to_string(index), output) {}
    void columnPrune(std::unordered_set<String> &) override { throw Exception("Should not reach here"); }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context &) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeExchangeReceiver);
        tipb_executor->set_executor_id(name);
        tipb::ExchangeReceiver * exchange_receiver = tipb_executor->mutable_exchange_receiver();
        for (auto & field : output_schema)
        {
            auto tipb_type = TiDB::columnInfoToFieldType(field.second);
            tipb_type.set_collate(collator_id);

            auto * field_type = exchange_receiver->add_field_types();
            *field_type = tipb_type;
        }
        auto it = mpp_info.receiver_source_task_ids_map.find(name);
        if (it == mpp_info.receiver_source_task_ids_map.end())
            throw Exception("Can not found mpp receiver info");
        for (size_t i = 0; i < it->second.size(); i++)
        {
            mpp::TaskMeta meta;
            meta.set_start_ts(mpp_info.start_ts);
            meta.set_task_id(it->second[i]);
            meta.set_partition_id(i);
            meta.set_address(LOCAL_HOST);
            auto * meta_string = exchange_receiver->add_encoded_task_meta();
            meta.AppendToString(meta_string);
        }
        return true;
    }
};

struct TableScan : public Executor
{
    TableInfo table_info;
    /// used by column pruner
    TableScan(size_t & index_, const DAGSchema & output_schema_, TableInfo & table_info_)
        : Executor(index_, "table_scan_" + std::to_string(index_), output_schema_), table_info(table_info_)
    {}
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        output_schema.erase(std::remove_if(output_schema.begin(), output_schema.end(),
                                [&](const auto & field) { return used_columns.count(field.first) == 0; }),
            output_schema.end());
    }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t, const MPPInfo &, const Context &) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTableScan);
        tipb_executor->set_executor_id(name);
        auto * ts = tipb_executor->mutable_tbl_scan();
        ts->set_table_id(table_info.id);
        for (const auto & info : output_schema)
        {
            tipb::ColumnInfo * ci = ts->add_columns();
            auto column_name = splitQualifiedName(info.first).second;
            if (column_name == MutableSupport::tidb_pk_column_name)
                ci->set_column_id(-1);
            else
                ci->set_column_id(table_info.getColumnID(column_name));
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
    void toMPPSubPlan(size_t &, const DAGProperties &,
        std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> &) override
    {}
};

struct Selection : public Executor
{
    std::vector<ASTPtr> conditions;
    Selection(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && conditions_)
        : Executor(index_, "selection_" + std::to_string(index_), output_schema_), conditions(std::move(conditions_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeSelection);
        tipb_executor->set_executor_id(name);
        auto * sel = tipb_executor->mutable_selection();
        for (auto & expr : conditions)
        {
            tipb::Expr * cond = sel->add_conditions();
            astToPB(children[0]->output_schema, expr, cond, collator_id, context);
        }
        auto * child_executor = sel->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
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
        : Executor(index_, "topn_" + std::to_string(index_), output_schema_), order_columns(std::move(order_columns_)), limit(limit_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeTopN);
        tipb_executor->set_executor_id(name);
        tipb::TopN * topn = tipb_executor->mutable_topn();
        for (const auto & child : order_columns)
        {
            ASTOrderByElement * elem = typeid_cast<ASTOrderByElement *>(child.get());
            if (!elem)
                throw Exception("Invalid order by element", ErrorCodes::LOGICAL_ERROR);
            tipb::ByItem * by = topn->add_order_by();
            by->set_desc(elem->direction < 0);
            tipb::Expr * expr = by->mutable_expr();
            astToPB(children[0]->output_schema, elem->children[0], expr, collator_id, context);
        }
        topn->set_limit(limit);
        auto * child_executor = topn->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
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
    Limit(size_t & index_, const DAGSchema & output_schema_, size_t limit_)
        : Executor(index_, "limit_" + std::to_string(index_), output_schema_), limit(limit_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeLimit);
        tipb_executor->set_executor_id(name);
        tipb::Limit * lt = tipb_executor->mutable_limit();
        lt->set_limit(limit);
        auto * child_executor = lt->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
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
    bool is_final_mode;
    DAGSchema output_schema_for_partial_agg;
    Aggregation(size_t & index_, const DAGSchema & output_schema_, bool has_uniq_raw_res_, bool need_append_project_,
        std::vector<ASTPtr> && agg_exprs_, std::vector<ASTPtr> && gby_exprs_, bool is_final_mode_)
        : Executor(index_, "aggregation_" + std::to_string(index_), output_schema_),
          has_uniq_raw_res(has_uniq_raw_res_),
          need_append_project(need_append_project_),
          agg_exprs(std::move(agg_exprs_)),
          gby_exprs(std::move(gby_exprs_)),
          is_final_mode(is_final_mode_)
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeAggregation);
        tipb_executor->set_executor_id(name);
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
                astToPB(input_schema, arg, arg_expr, collator_id, context);
            }
            auto agg_sig_it = agg_func_name_to_sig.find(func->name);
            if (agg_sig_it == agg_func_name_to_sig.end())
                throw Exception("Unsupported agg function " + func->name, ErrorCodes::LOGICAL_ERROR);
            auto agg_sig = agg_sig_it->second;
            agg_func->set_tp(agg_sig);

            if (agg_sig == tipb::ExprType::Count || agg_sig == tipb::ExprType::Sum)
            {
                auto * ft = agg_func->mutable_field_type();
                ft->set_tp(TiDB::TypeLongLong);
                ft->set_flag(TiDB::ColumnFlagUnsigned | TiDB::ColumnFlagNotNull);
            }
            else if (agg_sig == tipb::ExprType::Min || agg_sig == tipb::ExprType::Max || agg_sig == tipb::ExprType::First)
            {
                if (agg_func->children_size() != 1)
                    throw Exception("udaf " + func->name + " only accept 1 argument");
                auto * ft = agg_func->mutable_field_type();
                ft->set_tp(agg_func->children(0).field_type().tp());
                ft->set_decimal(agg_func->children(0).field_type().decimal());
                ft->set_flag(agg_func->children(0).field_type().flag() & (~TiDB::ColumnFlagNotNull));
                ft->set_collate(collator_id);
            }
            else if (agg_sig == tipb::ExprType::ApproxCountDistinct)
            {
                auto * ft = agg_func->mutable_field_type();
                ft->set_tp(TiDB::TypeString);
                ft->set_flag(1);
            }
            else if (agg_sig == tipb::ExprType::GroupConcat)
            {
                auto * ft = agg_func->mutable_field_type();
                ft->set_tp(TiDB::TypeString);
            }
            if (is_final_mode)
                agg_func->set_aggfuncmode(tipb::AggFunctionMode::FinalMode);
            else
                agg_func->set_aggfuncmode(tipb::AggFunctionMode::Partial1Mode);
        }

        for (const auto & child : gby_exprs)
        {
            tipb::Expr * gby = agg->add_group_by();
            astToPB(input_schema, child, gby, collator_id, context);
        }

        auto * child_executor = agg->mutable_child();
        return children[0]->toTiPBExecutor(child_executor, collator_id, mpp_info, context);
    }
    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        /// output schema for partial agg is the original agg's output schema
        output_schema_for_partial_agg = output_schema;
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
    void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties,
        std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map) override
    {
        if (!is_final_mode)
        {
            children[0]->toMPPSubPlan(executor_index, properties, exchange_map);
            return;
        }
        /// for aggregation, change aggregation to partial_aggregation => exchange_sender => exchange_receiver => final_aggregation
        // todo support avg
        if (has_uniq_raw_res)
            throw Exception("uniq raw res not supported in mpp query");
        std::shared_ptr<Aggregation> partial_agg = std::make_shared<Aggregation>(
            executor_index, output_schema_for_partial_agg, has_uniq_raw_res, false, std::move(agg_exprs), std::move(gby_exprs), false);
        partial_agg->children.push_back(children[0]);
        std::vector<size_t> partition_keys;
        size_t agg_func_num = partial_agg->agg_exprs.size();
        for (size_t i = 0; i < partial_agg->gby_exprs.size(); i++)
        {
            partition_keys.push_back(i + agg_func_num);
        }
        std::shared_ptr<ExchangeSender> exchange_sender = std::make_shared<ExchangeSender>(
            executor_index, output_schema_for_partial_agg, partition_keys.empty() ? tipb::PassThrough : tipb::Hash, partition_keys);
        exchange_sender->children.push_back(partial_agg);

        std::shared_ptr<ExchangeReceiver> exchange_receiver
            = std::make_shared<ExchangeReceiver>(executor_index, output_schema_for_partial_agg);
        exchange_map[exchange_receiver->name] = std::make_pair(exchange_receiver, exchange_sender);
        /// re-construct agg_exprs and gby_exprs in final_agg
        for (size_t i = 0; i < partial_agg->agg_exprs.size(); i++)
        {
            const ASTFunction * agg_func = typeid_cast<const ASTFunction *>(partial_agg->agg_exprs[i].get());
            ASTPtr update_agg_expr = agg_func->clone();
            auto * update_agg_func = typeid_cast<ASTFunction *>(update_agg_expr.get());
            if (agg_func->name == "count")
                update_agg_func->name = "sum";
            update_agg_func->arguments->children.clear();
            update_agg_func->arguments->children.push_back(std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[i].first));
            agg_exprs.push_back(update_agg_expr);
        }
        for (size_t i = 0; i < partial_agg->gby_exprs.size(); i++)
        {
            gby_exprs.push_back(std::make_shared<ASTIdentifier>(output_schema_for_partial_agg[agg_func_num + i].first));
        }
        children[0] = exchange_receiver;
    }
};

struct Project : public Executor
{
    std::vector<ASTPtr> exprs;
    Project(size_t & index_, const DAGSchema & output_schema_, std::vector<ASTPtr> && exprs_)
        : Executor(index_, "project_" + std::to_string(index_), output_schema_), exprs(std::move(exprs_))
    {}
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeProjection);
        tipb_executor->set_executor_id(name);
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
            astToPB(input_schema, child, expr, collator_id, context);
        }
        auto * children_executor = proj->mutable_child();
        return children[0]->toTiPBExecutor(children_executor, collator_id, mpp_info, context);
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

struct Join : Executor
{
    ASTPtr params;
    const ASTTableJoin & join_params;
    Join(size_t & index_, const DAGSchema & output_schema_, ASTPtr params_)
        : Executor(index_, "Join_" + std::to_string(index_), output_schema_),
          params(params_),
          join_params(static_cast<const ASTTableJoin &>(*params))
    {
        if (join_params.using_expression_list == nullptr)
            throw Exception("No join condition found.");
        if (join_params.strictness != ASTTableJoin::Strictness::All)
            throw Exception("Only support join with strictness ALL");
    }

    void columnPrune(std::unordered_set<String> & used_columns) override
    {
        std::unordered_set<String> left_columns;
        std::unordered_set<String> right_columns;
        for (auto & field : children[0]->output_schema)
            left_columns.emplace(field.first);
        for (auto & field : children[1]->output_schema)
            right_columns.emplace(field.first);

        std::unordered_set<String> left_used_columns;
        std::unordered_set<String> right_used_columns;
        for (auto & s : used_columns)
        {
            if (left_columns.find(s) != left_columns.end())
                left_used_columns.emplace(s);
            else
                right_used_columns.emplace(s);
        }
        for (auto child : join_params.using_expression_list->children)
        {
            if (auto * identifier = typeid_cast<ASTIdentifier *>(child.get()))
            {
                auto col_name = identifier->getColumnName();
                for (auto & field : children[0]->output_schema)
                {
                    if (col_name == splitQualifiedName(field.first).second)
                    {
                        left_used_columns.emplace(field.first);
                        break;
                    }
                }
                for (auto & field : children[1]->output_schema)
                {
                    if (col_name == splitQualifiedName(field.first).second)
                    {
                        right_used_columns.emplace(field.first);
                        break;
                    }
                }
            }
            else
            {
                throw Exception("Only support Join on columns");
            }
        }
        children[0]->columnPrune(left_used_columns);
        children[1]->columnPrune(right_used_columns);
        output_schema.clear();
        /// update output schema
        for (auto & field : children[0]->output_schema)
        {
            if (join_params.kind == ASTTableJoin::Kind::Right && field.second.hasNotNullFlag())
                output_schema.push_back(toNullableDAGColumnInfo(field));
            else
                output_schema.push_back(field);
        }
        for (auto & field : children[1]->output_schema)
        {
            if (join_params.kind == ASTTableJoin::Kind::Left && field.second.hasNotNullFlag())
                output_schema.push_back(toNullableDAGColumnInfo(field));
            else
                output_schema.push_back(field);
        }
    }

    void fillJoinKeyAndFieldType(
        ASTPtr key, const DAGSchema & schema, tipb::Expr * tipb_key, tipb::FieldType * tipb_field_type, uint32_t collator_id)
    {
        auto * identifier = typeid_cast<ASTIdentifier *>(key.get());
        for (size_t index = 0; index < schema.size(); index++)
        {
            auto & field = schema[index];
            if (splitQualifiedName(field.first).second == identifier->getColumnName())
            {
                auto tipb_type = TiDB::columnInfoToFieldType(field.second);
                tipb_type.set_collate(collator_id);

                tipb_key->set_tp(tipb::ColumnRef);
                std::stringstream ss;
                encodeDAGInt64(index, ss);
                tipb_key->set_val(ss.str());
                *tipb_key->mutable_field_type() = tipb_type;

                *tipb_field_type = tipb_type;
                break;
            }
        }
    }
    bool toTiPBExecutor(tipb::Executor * tipb_executor, uint32_t collator_id, const MPPInfo & mpp_info, const Context & context) override
    {
        tipb_executor->set_tp(tipb::ExecType::TypeJoin);
        tipb_executor->set_executor_id(name);
        tipb::Join * join = tipb_executor->mutable_join();
        switch (join_params.kind)
        {
            case ASTTableJoin::Kind::Inner:
                join->set_join_type(tipb::JoinType::TypeInnerJoin);
                break;
            case ASTTableJoin::Kind::Left:
                join->set_join_type(tipb::JoinType::TypeLeftOuterJoin);
                break;
            case ASTTableJoin::Kind::Right:
                join->set_join_type(tipb::JoinType::TypeRightOuterJoin);
                break;
            default:
                throw Exception("Unsupported join type");
        }
        join->set_join_exec_type(tipb::JoinExecType::TypeHashJoin);
        join->set_inner_idx(1);
        for (auto & key : join_params.using_expression_list->children)
        {
            fillJoinKeyAndFieldType(key, children[0]->output_schema, join->add_left_join_keys(), join->add_probe_types(), collator_id);
            fillJoinKeyAndFieldType(key, children[1]->output_schema, join->add_right_join_keys(), join->add_build_types(), collator_id);
        }
        auto * left_child_executor = join->add_children();
        children[0]->toTiPBExecutor(left_child_executor, collator_id, mpp_info, context);
        auto * right_child_executor = join->add_children();
        return children[1]->toTiPBExecutor(right_child_executor, collator_id, mpp_info, context);
    }
    void toMPPSubPlan(size_t & executor_index, const DAGProperties & properties,
        std::unordered_map<String, std::pair<std::shared_ptr<ExchangeReceiver>, std::shared_ptr<ExchangeSender>>> & exchange_map) override
    {
        if (properties.use_broadcast_join)
        {
            /// for broadcast join, always use right side as the broadcast side
            std::shared_ptr<ExchangeSender> right_exchange_sender
                = std::make_shared<ExchangeSender>(executor_index, children[1]->output_schema, tipb::Broadcast);
            right_exchange_sender->children.push_back(children[1]);

            std::shared_ptr<ExchangeReceiver> right_exchange_receiver
                = std::make_shared<ExchangeReceiver>(executor_index, children[1]->output_schema);
            children[1] = right_exchange_receiver;
            exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
            return;
        }
        std::vector<size_t> left_partition_keys;
        std::vector<size_t> right_partition_keys;
        for (auto & key : join_params.using_expression_list->children)
        {
            size_t index = 0;
            for (; index < children[0]->output_schema.size(); index++)
            {
                if (splitQualifiedName(children[0]->output_schema[index].first).second == key->getColumnName())
                {
                    left_partition_keys.push_back(index);
                    break;
                }
            }
            index = 0;
            for (; index < children[1]->output_schema.size(); index++)
            {
                if (splitQualifiedName(children[1]->output_schema[index].first).second == key->getColumnName())
                {
                    right_partition_keys.push_back(index);
                    break;
                }
            }
        }
        std::shared_ptr<ExchangeSender> left_exchange_sender
            = std::make_shared<ExchangeSender>(executor_index, children[0]->output_schema, tipb::Hash, left_partition_keys);
        left_exchange_sender->children.push_back(children[0]);
        std::shared_ptr<ExchangeSender> right_exchange_sender
            = std::make_shared<ExchangeSender>(executor_index, children[1]->output_schema, tipb::Hash, right_partition_keys);
        right_exchange_sender->children.push_back(children[1]);

        std::shared_ptr<ExchangeReceiver> left_exchange_receiver
            = std::make_shared<ExchangeReceiver>(executor_index, children[0]->output_schema);
        std::shared_ptr<ExchangeReceiver> right_exchange_receiver
            = std::make_shared<ExchangeReceiver>(executor_index, children[1]->output_schema);
        children[0] = left_exchange_receiver;
        children[1] = right_exchange_receiver;

        exchange_map[left_exchange_receiver->name] = std::make_pair(left_exchange_receiver, left_exchange_sender);
        exchange_map[right_exchange_receiver->name] = std::make_pair(right_exchange_receiver, right_exchange_sender);
    }
};
} // namespace mock

using ExecutorPtr = std::shared_ptr<mock::Executor>;

TiDB::ColumnInfo compileExpr(const DAGSchema & input, ASTPtr ast)
{
    TiDB::ColumnInfo ci;
    if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(ast.get()))
    {
        /// check column
        auto ft = std::find_if(input.begin(), input.end(), [&](const auto & field) {
            auto column_name = splitQualifiedName(id->getColumnName());
            auto field_name = splitQualifiedName(field.first);
            if (column_name.first.empty())
                return field_name.second == column_name.second;
            else
                return field_name.first == column_name.first && field_name.second == column_name.second;
        });
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
            case tipb::ScalarFuncSig::CastIntAsReal:
            case tipb::ScalarFuncSig::CastRealAsReal:
            {
                ci.tp = TiDB::TypeDouble;
                break;
            }
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

ExecutorPtr compileTableScan(size_t & executor_index, TableInfo & table_info, String & table_alias, bool append_pk_column)
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
        /// use qualified name as the column name to handle multiple table queries, not very
        /// efficient but functionally enough for mock test
        ts_output.emplace_back(std::make_pair(table_alias + "." + column_info.name, std::move(ci)));
    }
    if (append_pk_column)
    {
        ColumnInfo ci;
        ci.tp = TiDB::TypeLongLong;
        ci.setPriKeyFlag();
        ci.setNotNullFlag();
        ts_output.emplace_back(std::make_pair(MutableSupport::tidb_pk_column_name, std::move(ci)));
    }
    return std::make_shared<mock::TableScan>(executor_index, ts_output, table_info);
}

ExecutorPtr compileSelection(ExecutorPtr input, size_t & executor_index, ASTPtr filter)
{
    std::vector<ASTPtr> conditions;
    compileFilter(input->output_schema, filter, conditions);
    auto selection = std::make_shared<mock::Selection>(executor_index, input->output_schema, std::move(conditions));
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
    auto topN = std::make_shared<mock::TopN>(executor_index, input->output_schema, std::move(order_columns), limit);
    topN->children.push_back(input);
    return topN;
}

ExecutorPtr compileLimit(ExecutorPtr input, size_t & executor_index, ASTPtr limit_expr)
{
    auto limit_length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*limit_expr).value);
    auto limit = std::make_shared<mock::Limit>(executor_index, input->output_schema, limit_length);
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
            else if (func->name == "max" || func->name == "min" || func->name == "first_row")
            {
                ci = children_ci[0];
                ci.flag &= ~TiDB::ColumnFlagNotNull;
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

    auto aggregation = std::make_shared<mock::Aggregation>(
        executor_index, output_schema, has_uniq_raw_res, need_append_project, std::move(agg_exprs), std::move(gby_exprs), true);
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
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
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
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(ft->first, ft->second);
            }
            else
            {
                auto ci = compileExpr(input->output_schema, expr);
                // todo need to use the subquery alias to reconstruct the field
                //  name if subquery is supported
                output_schema.emplace_back(std::make_pair(expr->getColumnName(), ci));
            }
        }
    }

    auto project = std::make_shared<mock::Project>(executor_index, output_schema, std::move(exprs));
    project->children.push_back(input);
    return project;
}

ExecutorPtr compileJoin(size_t & executor_index, ExecutorPtr left, ExecutorPtr right, ASTPtr params)
{
    DAGSchema output_schema;
    auto & join_params = (static_cast<const ASTTableJoin &>(*params));
    for (auto & field : left->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Right && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    for (auto & field : right->output_schema)
    {
        if (join_params.kind == ASTTableJoin::Kind::Left && field.second.hasNotNullFlag())
            output_schema.push_back(toNullableDAGColumnInfo(field));
        else
            output_schema.push_back(field);
    }
    auto join = std::make_shared<mock::Join>(executor_index, output_schema, params);
    join->children.push_back(left);
    join->children.push_back(right);
    return join;
}

struct QueryFragment
{
    ExecutorPtr root_executor;
    TableID table_id;
    bool is_top_fragment;
    std::vector<Int64> sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    std::vector<Int64> task_ids;
    QueryFragment(ExecutorPtr root_executor_, TableID table_id_, bool is_top_fragment_, std::vector<Int64> && sender_target_task_ids_ = {},
        std::unordered_map<String, std::vector<Int64>> && receiver_source_task_ids_map_ = {}, std::vector<Int64> && task_ids_ = {})
        : root_executor(std::move(root_executor_)),
          table_id(table_id_),
          is_top_fragment(is_top_fragment_),
          sender_target_task_ids(std::move(sender_target_task_ids_)),
          receiver_source_task_ids_map(std::move(receiver_source_task_ids_map_)),
          task_ids(std::move(task_ids_))
    {}

    QueryTask toQueryTask(const DAGProperties & properties, MPPInfo & mpp_info, const Context & context)
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
        root_executor->toTiPBExecutor(root_tipb_executor, properties.collator, mpp_info, context);
        return QueryTask(dag_request_ptr, table_id, root_executor->output_schema,
            mpp_info.sender_target_task_ids.size() == 0 ? DAG : MPP_DISPATCH, mpp_info.task_id, mpp_info.partition_id, is_top_fragment);
    }

    QueryTasks toQueryTasks(const DAGProperties & properties, const Context & context)
    {
        QueryTasks ret;
        if (properties.is_mpp_query)
        {
            for (size_t partition_id = 0; partition_id < task_ids.size(); partition_id++)
            {
                MPPInfo mpp_info(
                    properties.start_ts, partition_id, task_ids[partition_id], sender_target_task_ids, receiver_source_task_ids_map);
                ret.push_back(toQueryTask(properties, mpp_info, context));
            }
        }
        else
        {
            MPPInfo mpp_info(properties.start_ts, -1, -1, {}, {});
            ret.push_back(toQueryTask(properties, mpp_info, context));
        }
        return ret;
    }
};

using QueryFragments = std::vector<QueryFragment>;

TableID findTableIdForQueryFragment(ExecutorPtr root_executor, bool must_have_table_id)
{
    ExecutorPtr current_executor = root_executor;
    while (!current_executor->children.empty())
    {
        ExecutorPtr non_exchange_child;
        for (auto c : current_executor->children)
        {
            if (dynamic_cast<mock::ExchangeReceiver *>(c.get()))
                continue;
            if (non_exchange_child != nullptr)
                throw Exception("More than one non-exchange child, should not happen");
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
    auto * ts = dynamic_cast<mock::TableScan *>(current_executor.get());
    if (ts == nullptr)
    {
        if (must_have_table_id)
            throw Exception("Table scan not found");
        return -1;
    }
    return ts->table_info.id;
}

QueryFragments mppQueryToQueryFragments(
    ExecutorPtr root_executor, size_t & executor_index, const DAGProperties & properties, bool for_root_fragment, MPPCtxPtr mpp_ctx)
{
    QueryFragments fragments;
    std::unordered_map<String, std::pair<std::shared_ptr<mock::ExchangeReceiver>, std::shared_ptr<mock::ExchangeSender>>> exchange_map;
    root_executor->toMPPSubPlan(executor_index, properties, exchange_map);
    TableID table_id = findTableIdForQueryFragment(root_executor, exchange_map.empty());
    std::vector<Int64> sender_target_task_ids = mpp_ctx->sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    size_t current_task_num = properties.mpp_partition_num;
    for (auto & exchange : exchange_map)
    {
        if (exchange.second.second->type == tipb::ExchangeType::PassThrough)
        {
            current_task_num = 1;
            break;
        }
    }
    std::vector<Int64> current_task_ids;
    for (size_t i = 0; i < current_task_num; i++)
        current_task_ids.push_back(mpp_ctx->next_task_id++);
    for (auto & exchange : exchange_map)
    {
        mpp_ctx->sender_target_task_ids = current_task_ids;
        auto sub_fragments = mppQueryToQueryFragments(exchange.second.second, executor_index, properties, false, mpp_ctx);
        receiver_source_task_ids_map[exchange.first] = sub_fragments.cbegin()->task_ids;
        fragments.insert(fragments.end(), sub_fragments.begin(), sub_fragments.end());
    }
    fragments.emplace_back(root_executor, table_id, for_root_fragment, std::move(sender_target_task_ids),
        std::move(receiver_source_task_ids_map), std::move(current_task_ids));
    return fragments;
}

QueryFragments queryPlanToQueryFragments(const DAGProperties & properties, ExecutorPtr root_executor, size_t & executor_index)
{
    if (properties.is_mpp_query)
    {
        ExecutorPtr root_exchange_sender
            = std::make_shared<mock::ExchangeSender>(executor_index, root_executor->output_schema, tipb::PassThrough);
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
    const DAGProperties & properties, ExecutorPtr root_executor, size_t & executor_index, const Context & context)
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

const ASTTablesInSelectQueryElement * getJoin(ASTSelectQuery & ast_query)
{
    if (!ast_query.tables)
        return nullptr;

    const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*ast_query.tables);
    if (tables_in_select_query.children.empty())
        return nullptr;

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
        if (tables_element.table_join)
        {
            if (!joined_table)
                joined_table = &tables_element;
            else
                throw Exception("Support for more than one JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return joined_table;
}

std::pair<ExecutorPtr, bool> compileQueryBlock(
    Context & context, size_t & executor_index, SchemaFetcher schema_fetcher, const DAGProperties & properties, ASTSelectQuery & ast_query)
{
    auto joined_table = getJoin(ast_query);
    /// uniq_raw is used to test `ApproxCountDistinct`, when testing `ApproxCountDistinct` in mock coprocessor
    /// the return value of `ApproxCountDistinct` is just the raw result, we need to convert it to a readable
    /// value when decoding the result(using `UniqRawResReformatBlockOutputStream`)
    bool has_uniq_raw_res = false;
    ExecutorPtr root_executor = nullptr;

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
                if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
                {
                    if (identifier->getColumnName() == MutableSupport::tidb_pk_column_name)
                    {
                        append_pk_column = true;
                    }
                }
            }
            root_executor = compileTableScan(executor_index, table_info, table_alias, append_pk_column);
        }
    }
    else
    {
        TableInfo left_table_info = table_info;
        String left_table_alias = table_alias;
        TableInfo right_table_info;
        String right_table_alias;
        {
            String database_name, table_name;
            const ASTTableExpression & table_to_join = static_cast<const ASTTableExpression &>(*joined_table->table_expression);
            if (table_to_join.database_and_table_name)
            {
                auto identifier = static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name);
                table_name = identifier.name;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

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
            if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                auto names = splitQualifiedName(identifier->getColumnName());
                if (names.second == MutableSupport::tidb_pk_column_name)
                {
                    if (names.first.empty())
                    {
                        throw Exception("tidb pk column must be qualified since there are more than one tables");
                    }
                    if (names.first == left_table_alias)
                        left_append_pk_column = true;
                    else if (names.first == right_table_alias)
                        right_append_pk_column = true;
                    else
                        throw Exception("Unknown table alias: " + names.first);
                }
            }
        }
        auto left_ts = compileTableScan(executor_index, left_table_info, left_table_alias, left_append_pk_column);
        auto right_ts = compileTableScan(executor_index, right_table_info, right_table_alias, right_append_pk_column);
        root_executor = compileJoin(executor_index, left_ts, right_ts, joined_table->table_join);
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
            && (dynamic_cast<mock::Limit *>(root_executor.get()) != nullptr || dynamic_cast<mock::TopN *>(root_executor.get()) != nullptr))
            throw Exception("Limit/TopN and Agg cannot co-exist in non-mpp mode.", ErrorCodes::LOGICAL_ERROR);

        root_executor = compileAggregation(
            root_executor, executor_index, ast_query.select_expression_list, has_gby ? ast_query.group_expression_list : nullptr);

        if (dynamic_cast<mock::Aggregation *>(root_executor.get())->has_uniq_raw_res)
        {
            // todo support uniq_raw in mpp mode
            if (properties.is_mpp_query)
                throw Exception("uniq_raw_res not supported in mpp mode.", ErrorCodes::LOGICAL_ERROR);
            else
                has_uniq_raw_res = true;
        }

        if (dynamic_cast<mock::Aggregation *>(root_executor.get())->need_append_project)
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
    Context & context, const String & query, SchemaFetcher schema_fetcher, const DAGProperties & properties)
{
    MakeResOutputStream func_wrap_output_stream = [](BlockInputStreamPtr in) { return in; };

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    size_t executor_index = 0;
    auto [root_executor, has_uniq_raw_res] = compileQueryBlock(context, executor_index, schema_fetcher, properties, ast_query);
    if (has_uniq_raw_res)
        func_wrap_output_stream = [](BlockInputStreamPtr in) { return std::make_shared<UniqRawResReformatBlockOutputStream>(in); };

    /// finalize
    std::unordered_set<String> used_columns;
    for (auto & schema : root_executor->output_schema)
        used_columns.emplace(schema.first);
    root_executor->columnPrune(used_columns);

    return std::make_tuple(queryPlanToQueryTasks(properties, root_executor, executor_index, context), func_wrap_output_stream);
}

tipb::SelectResponse executeDAGRequest(Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version,
    UInt64 region_conf_version, Timestamp start_ts, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges)
{
    static Logger * log = &Logger::get("MockDAG");
    LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
    tipb::SelectResponse dag_response;
    RegionInfoMap regions;
    RegionInfoList retry_regions;

    regions.emplace(region_id, RegionInfo(region_id, region_version, region_conf_version, std::move(key_ranges), nullptr));
    DAGDriver driver(context, dag_request, regions, retry_regions, start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
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
